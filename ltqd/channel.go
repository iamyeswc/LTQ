package ltqd

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nsqio/go-diskqueue"
)

type Consumer interface {
	Close() error
}
type Channel struct {
	name           string
	topicName      string
	ltqd           *LTQD
	memoryMsgChan  chan *Message
	backendMsgChan BackendQueue

	exitMutex sync.RWMutex
	exitFlag  int32

	messageCount uint64 //消息数量

	//记录正在处理消息id和message的映射，可以找到message后去最小堆里删除
	inFlightMessages map[MessageID]*Message
	//保存message的最小堆
	inFlightPQ    inFlightPqueue
	inFlightMutex sync.Mutex

	sync.RWMutex
	//连接channel的客户端
	clients map[int64]Consumer
}

// constructor of Channel
// - 初始化channel中的各个成员变量
// - 通知ltqd 触发新channel建立时 ltqd需要做的工作(metadata持久化到磁盘、通知lookup注册了新的channel)
func NewChannel(name, topicName string, ltqd *LTQD) *Channel {
	c := &Channel{
		name:          name,
		topicName:     topicName,
		ltqd:          ltqd,
		memoryMsgChan: make(chan *Message, ltqd.getOpts().MemQueueSize),
	}

	dqLogf := func(level diskqueue.LogLevel, f string, args ...interface{}) {
		fmtLogf(Debug, f, args...)
	}

	c.backendMsgChan = diskqueue.New(
		name,
		ltqd.getOpts().DataPath,
		ltqd.getOpts().MaxBytesPerFile,
		int32(minValidMsgLength),
		int32(ltqd.getOpts().MaxMsgSize)+minValidMsgLength,
		ltqd.getOpts().SyncEvery,
		ltqd.getOpts().SyncTimeout,
		dqLogf,
	)

	return c
}

// Exiting
// - 判断当前channel是否退出
// - 原子操作避免数据竞争 防止数据不一致和错误
func (c *Channel) Exiting() bool {
	return atomic.LoadInt32(&c.exitFlag) == 1
}

func (c *Channel) PutMessage(m *Message) error {
	c.exitMutex.RLock()
	defer c.exitMutex.RUnlock()
	if c.Exiting() {
		return errors.New("exiting")
	}
	err := c.put(m)
	if err != nil {
		return err
	}
	atomic.AddUint64(&c.messageCount, 1)
	return nil
}

func (c *Channel) put(m *Message) error {
	//把消息放到内存
	if cap(c.memoryMsgChan) > 0 {
		select {
		case c.memoryMsgChan <- m:
			return nil
		default:
			break
		}
	}

	//如果内存满了，放到后端
	err := writeMessageToBackend(m, c.backendMsgChan)
	c.ltqd.SetHealth(err)
	if err != nil {
		fmtLogf(Debug, "CHANNEL(%s): put message to backend - %s", c.name, err)
		return err
	}
	return nil
}

func (c *Channel) StartInFlightTimeout(msg *Message, clientID int64, timeout time.Duration) error {
	now := time.Now()
	msg.clientID = clientID
	msg.deliveryTS = now
	msg.pri = now.Add(timeout).UnixNano()
	//放到map里
	err := c.pushInFlightMessage(msg)
	if err != nil {
		return err
	}
	//再放到最小堆里
	c.addToInFlightPQ(msg)
	return nil
}

func (c *Channel) pushInFlightMessage(msg *Message) error {
	c.inFlightMutex.Lock()
	_, ok := c.inFlightMessages[msg.ID]
	if ok {
		c.inFlightMutex.Unlock()
		return errors.New("ID already in flight")
	}
	c.inFlightMessages[msg.ID] = msg
	c.inFlightMutex.Unlock()
	return nil
}

func (c *Channel) addToInFlightPQ(msg *Message) {
	c.inFlightMutex.Lock()
	c.inFlightPQ.Push(msg)
	c.inFlightMutex.Unlock()
}

func (c *Channel) AddClient(clientID int64, client Consumer) error {
	c.exitMutex.RLock()
	defer c.exitMutex.RUnlock()

	if c.Exiting() {
		return errors.New("exiting")
	}

	c.RLock()
	_, ok := c.clients[clientID]
	c.RUnlock()
	if ok {
		return nil
	}

	c.Lock()
	c.clients[clientID] = client
	c.Unlock()
	return nil
}

func (c *Channel) RemoveClient(clientID int64) {
	c.exitMutex.RLock()
	defer c.exitMutex.RUnlock()

	if c.Exiting() {
		return
	}

	c.RLock()
	_, ok := c.clients[clientID]
	c.RUnlock()
	if !ok {
		return
	}

	c.Lock()
	delete(c.clients, clientID)
	numClients := len(c.clients)
	c.Unlock()

	if numClients == 0 {
		//如果没有客户端了，关闭channel
	}
}

// 客户端消费完，把消息从inFlightMessages里删除
func (c *Channel) FinishMessage(clientID int64, id MessageID) error {
	//从map里删除
	msg, err := c.popInFlightMessage(clientID, id)
	if err != nil {
		return err
	}
	//从最小堆里删除
	c.removeFromInFlightPQ(msg)
	return nil
}

func (c *Channel) popInFlightMessage(clientID int64, id MessageID) (*Message, error) {
	c.inFlightMutex.Lock()
	msg, ok := c.inFlightMessages[id]
	if !ok {
		c.inFlightMutex.Unlock()
		return nil, errors.New("ID not in flight")
	}
	if msg.clientID != clientID {
		c.inFlightMutex.Unlock()
		return nil, errors.New("client does not own message")
	}
	delete(c.inFlightMessages, id)
	c.inFlightMutex.Unlock()
	return msg, nil
}

func (c *Channel) removeFromInFlightPQ(msg *Message) {
	c.inFlightMutex.Lock()
	if msg.index == -1 {
		c.inFlightMutex.Unlock()
		return
	}
	c.inFlightPQ.Remove(msg.index)
	c.inFlightMutex.Unlock()
}

func (c *Channel) processInFlightQueue(t int64) bool {
	c.exitMutex.RLock()
	defer c.exitMutex.RUnlock()

	if c.Exiting() {
		return false
	}

	dirty := false
	for {
		c.inFlightMutex.Lock()
		msg, _ := c.inFlightPQ.PeekAndShift(t)
		c.inFlightMutex.Unlock()

		if msg == nil {
			goto exit
		}
		dirty = true

		_, err := c.popInFlightMessage(msg.clientID, msg.ID)
		if err != nil {
			goto exit
		}
		c.put(msg)
	}

exit:
	return dirty
}
