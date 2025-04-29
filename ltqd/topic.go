package ltqd

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nsqio/go-diskqueue"
)

type Topic struct {
	name           string
	ltqd           *LTQD
	memoryMsgChan  chan *Message
	backendMsgChan BackendQueue
	channels       map[string]*Channel

	startChan         chan int //开始messagePump的标志
	exitChan          chan int //结束messagePump的标志
	channelUpdateChan chan int
	sync.RWMutex

	exitFlag int32 //退出标志位

	//根据当前topic下所有消息大小决定是否要放到后端
	messageCount uint64 //消息数量
	messageBytes uint64 //消息体大小

	waitGroup WaitGroupWrapper

	idFactory *guidFactory
}

// constructor of Topic
// - 初始化topic中的各个成员变量
// - 启动messagePump协程 利用sync.WaitGroup做同步
// - 通知ltqd 触发新topic建立时 ltqd需要做的工作(metadata持久化到磁盘、通知lookup注册了新的topic)
func NewTopic(name string, ltqd *LTQD) *Topic {
	t := &Topic{
		name:              name,
		ltqd:              ltqd,
		memoryMsgChan:     make(chan *Message, ltqd.getOpts().MemQueueSize),
		startChan:         make(chan int),
		exitChan:          make(chan int),
		channelUpdateChan: make(chan int),
		channels:          make(map[string]*Channel),
	}

	dqLogf := func(level diskqueue.LogLevel, f string, args ...interface{}) {
		fmtLogf(Debug, f, args...)
	}

	t.backendMsgChan = diskqueue.New(
		name,
		ltqd.getOpts().DataPath,
		ltqd.getOpts().MaxBytesPerFile,
		int32(minValidMsgLength),
		int32(ltqd.getOpts().MaxMsgSize)+minValidMsgLength,
		ltqd.getOpts().SyncEvery,
		ltqd.getOpts().SyncTimeout,
		dqLogf,
	)

	t.waitGroup.Wrap(t.messagePump)

	return t
}

// Start
// - 控制topic的开启 开始接收消息(控制messagePump的开启)
// - 通过一个startChan来做goroutine之间的消息传递
func (t *Topic) Start() {
	select {
	case t.startChan <- 1:
	default:
	}
}

// Exiting
// - 判断当前topic是否退出
// - 原子操作避免数据竞争 防止数据不一致和错误
// 用于客户端与ltqd连接的时候，如果需要有客户端订阅正在退出的topic，需要把这个客户端移除
func (t *Topic) Exiting() bool {
	return atomic.LoadInt32(&t.exitFlag) == 1
}

// GetChannel
// - 从当前topic中取某个channel 如果channel不存在需要创建
// - 通过channelUpdateChan，在新创建channel时与messagePump做消息传递 messagePump需要重新获取所有的channel
// - 加锁保证GetChannel操作的线程安全 防止并发产生错误
// 用于客户端与ltqd连接的时候，如果需要有客户端订阅对应topic下的channel，拿到对应的channel，把客户端加进去
func (t *Topic) GetChannel(name string) *Channel {
	t.Lock()
	channel, isNew := t.getOrCreateChannel(name)
	t.Unlock()

	if isNew {
		//新创建的channel, 需要通知topic的messagepumb更新所有list的channel
		select {
		case t.channelUpdateChan <- 1:
		case <-t.exitChan:
		}
	}

	return channel
}

func (t *Topic) getOrCreateChannel(name string) (*Channel, bool) {
	channel, ok := t.channels[name]
	if !ok {
		//找不到channel就创建一个
		channel = NewChannel(t.name, name, t.ltqd)
		t.channels[name] = channel
		fmtLogf(Debug, "TOPIC(%v): new channel(%v)", t.name, channel.name)
		return channel, true
	}
	return channel, false
}

// // GetExistingChannel
// // - 保证channel存在的前提下 获取channel的方法
// // - 相比于GetChannel不存在create channel的行为 通过sync.RWMutex提高并发性能和效率
// func GetExistingChannel() {

// }

// // DeleteExistingChannel
// // - 保证channel存在的前提下 从当前topic下删除channel
// // - 通过channelUpdateChan，在删除channel时与messagePump做消息传递 messagePump需要重新获取所有的channel
// func DeleteExistingChannel() {

// }

// messagePump
// - 当前topic的消息处理函数
// - 不断select接收来自in-memory queue和backend queue的消息 并复制到当前topic的每个channel
func (t *Topic) messagePump() {
	var msg *Message
	var buf []byte
	var err error
	var chans []*Channel
	var memoryMsgChan chan *Message
	var backendMsgChan <-chan []byte

	for {
		select {
		case <-t.channelUpdateChan: //这时候更新channel 不用进入主循环
			continue
		case <-t.exitChan:
			fmtLogf(Debug, "TOPIC(%v): closing ... messagePump", t.name)
			return
		case <-t.startChan:
		}
		break
	}

	//开始后获取所有channels
	t.RLock()
	for _, c := range t.channels {
		chans = append(chans, c)
	}
	t.RUnlock()

	if len(chans) > 0 {
		memoryMsgChan = t.memoryMsgChan
		backendMsgChan = t.backendMsgChan.ReadChan()
	}

	//主循环
	for {
		select {
		case msg = <-memoryMsgChan:
		case buf = <-backendMsgChan:
			msg, err = decodeMessage(buf)
			if err != nil {
				fmtLogf(Debug, "TOPIC(%v): failed to decode message - %v", t.name, err)
				continue
			}
		case <-t.channelUpdateChan:
			//移除之前所有的chans，更新到最新
			chans = chans[:0]
			t.RLock()
			for _, c := range t.channels {
				chans = append(chans, c)
			}
			t.RUnlock()
			if len(chans) == 0 {
				memoryMsgChan = nil
				backendMsgChan = nil
			} else {
				memoryMsgChan = t.memoryMsgChan
				backendMsgChan = t.backendMsgChan.ReadChan()
			}
			continue
		case <-t.exitChan:
			fmtLogf(Debug, "TOPIC(%v): closing ... messagePump", t.name)
			return
		}

		for i, channel := range chans {
			chanMsg := msg
			// 复制消息，因为每个 channel 都需要一个独立的实例，但如果是第一个 channel，可以避免复制（fastpath）
			if i > 0 {
				chanMsg = NewMessage(msg.ID, msg.Body)
				chanMsg.Timestamp = msg.Timestamp
			}
			err := channel.PutMessage(chanMsg)
			if err != nil {
				fmtLogf(Debug, "TOPIC(%v): failed to put msg(%v) to channel(%v) - %v", t.name, msg.ID, channel.name, err)
			}
		}
	}
}

func (t *Topic) PutMessage(m *Message) error {
	//放一个消息
	t.RLock()
	defer t.RUnlock()
	if atomic.LoadInt32(&t.exitFlag) == 1 {
		return errors.New("exiting")
	}
	err := t.put(m)
	if err != nil {
		return err
	}
	atomic.AddUint64(&t.messageCount, 1)
	atomic.AddUint64(&t.messageBytes, uint64(len(m.Body)))
	return nil
}

func (t *Topic) PutMessages(msgs []*Message) error {
	//放多个消息
	t.RLock()
	defer t.RUnlock()
	if atomic.LoadInt32(&t.exitFlag) == 1 {
		return errors.New("exiting")
	}

	messageTotalBytes := 0

	for i, m := range msgs {
		err := t.put(m)
		if err != nil {
			atomic.AddUint64(&t.messageCount, uint64(i))
			atomic.AddUint64(&t.messageBytes, uint64(messageTotalBytes))
			return err
		}
		messageTotalBytes += len(m.Body)
	}

	atomic.AddUint64(&t.messageBytes, uint64(messageTotalBytes))
	atomic.AddUint64(&t.messageCount, uint64(len(msgs)))
	return nil
}

func (t *Topic) put(m *Message) error {
	//把消息放到内存
	if cap(t.memoryMsgChan) > 0 {
		select {
		case t.memoryMsgChan <- m:
			return nil
		default:
			break
		}
	}

	//如果内存满了，放到后端
	err := writeMessageToBackend(m, t.backendMsgChan)
	t.ltqd.SetHealth(err)
	if err != nil {
		fmtLogf(Debug, "TOPIC(%v) ERROR: failed to write message to backend - %v", t.name, err)
		return err
	}
	return nil
}

func (t *Topic) GenerateID() MessageID {
	//雪花算法生成一个唯一的ID
	var i int64 = 0
	for {
		id, err := t.idFactory.NewGUID()
		if err == nil {
			return id.Hex()
		}
		if i%10000 == 0 {
			fmtLogf(Debug, "TOPIC(%v): failed to create guid - %v", t.name, err)
		}
		time.Sleep(time.Millisecond)
		i++
	}
}

func (t *Topic) Close() error {
	return t.exit()
}

func (t *Topic) exit() error {
	if !atomic.CompareAndSwapInt32(&t.exitFlag, 0, 1) {
		return errors.New("exiting")
	}

	close(t.exitChan)

	//等待messagepump完成
	t.waitGroup.Wait()

	// close all the channels
	t.RLock()
	for _, channel := range t.channels {
		err := channel.Close()
		if err != nil {
			// we need to continue regardless of error to close all the channels
			fmtLogf(Debug, "channel(%s) close - %s", channel.name, err)
		}
	}
	t.RUnlock()

	//把内存里的topic的消息持久化到磁盘
	t.flush()
	return t.backendMsgChan.Close()
}

func (t *Topic) flush() error {
	if len(t.memoryMsgChan) > 0 {
		fmtLogf(Debug, "TOPIC(%s): flushing %d memory messages to backend", t.name, len(t.memoryMsgChan))
	}

	for {
		select {
		case msg := <-t.memoryMsgChan:
			err := writeMessageToBackend(msg, t.backendMsgChan)
			if err != nil {
				fmtLogf(Debug, "ERROR: failed to write message to backend - %s", err)
			}
		default:
			return nil
		}
	}

}
