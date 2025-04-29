package ltqd

import (
	"errors"
	"sync"
	"sync/atomic"

	"github.com/nsqio/go-diskqueue"
)

type Topic struct {
	name           string
	ltqd           *LTQD
	memoryMsgChan  chan *Message
	backendMsgChan BackendQueue
	channels       map[string]*Channel

	startChan chan int //开始messagePump的标志
	exitChan  chan int //结束messagePump的标志
	sync.RWMutex

	exitFlag int32 //退出标志位

	//根据当前topic下所有消息大小决定是否要放到后端
	messageCount uint64 //消息数量
	messageBytes uint64 //消息体大小

	waitGroup WaitGroupWrapper
}

// constructor of Topic
// - 初始化topic中的各个成员变量
// - 启动messagePump协程 利用sync.WaitGroup做同步
// - 通知ltqd 触发新topic建立时 ltqd需要做的工作(metadata持久化到磁盘、通知lookup注册了新的topic)
func NewTopic(name string, ltqd *LTQD) *Topic {
	t := &Topic{
		name:          name,
		ltqd:          ltqd,
		memoryMsgChan: make(chan *Message, ltqd.getOpts().MemQueueSize),
		channels:      make(map[string]*Channel),
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
func Exiting() {

}

// GetChannel
// - 从当前topic中取某个channel 如果channel不存在需要创建
// - 通过channelUpdateChan，在新创建channel时与messagePump做消息传递 messagePump需要重新获取所有的channel
// - 加锁保证GetChannel操作的线程安全 防止并发产生错误
func GetChannel() {

}

// GetExistingChannel
// - 保证channel存在的前提下 获取channel的方法
// - 相比于GetChannel不存在create channel的行为 通过sync.RWMutex提高并发性能和效率
func GetExistingChannel() {

}

// DeleteExistingChannel
// - 保证channel存在的前提下 从当前topic下删除channel
// - 通过channelUpdateChan，在删除channel时与messagePump做消息传递 messagePump需要重新获取所有的channel
func DeleteExistingChannel() {

}

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
		case <-t.exitChan:
			fmtLogf(Debug, "TOPIC(%s): closing ... messagePump", t.name)
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
				fmtLogf(Debug, "TOPIC(%s): failed to decode message - %s", t.name, err)
				continue
			}
		case <-t.exitChan:
			fmtLogf(Debug, "TOPIC(%s): closing ... messagePump", t.name)
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
				fmtLogf(Debug, "TOPIC(%s): failed to put msg(%s) to channel(%s) - %s", t.name, msg.ID, channel.name, err)
			}
		}
	}
}

func (t *Topic) PutMessages(msgs []*Message) error {
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
		fmtLogf(Debug, "TOPIC(%s) ERROR: failed to write message to backend - %s", t.name, err)
		return err
	}
	return nil
}
