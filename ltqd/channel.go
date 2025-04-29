package ltqd

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

type Channel struct {
	name           string
	ltqd           *LTQD
	memoryMsgChan  chan *Message
	backendMsgChan BackendQueue

	exitMutex sync.RWMutex
	exitFlag  int32
}

// constructor of Channel
// - 初始化channel中的各个成员变量
// - 通知ltqd 触发新channel建立时 ltqd需要做的工作(metadata持久化到磁盘、通知lookup注册了新的channel)
func NewChannel(name string, ltqd *LTQD) *Channel {
	c := &Channel{
		name:          name,
		ltqd:          ltqd,
		memoryMsgChan: make(chan *Message, ltqd.getOpts().MemQueueSize),
	}

	dqLogf := func(level diskqueue.LogLevel, f string, args ...interface{}) {
		fmtLogf("DEBUG", f, args...)
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
func (c *Channel) Exiting() {

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

func (c *Channel) Exiting() bool {
	return atomic.LoadInt32(&c.exitFlag) == 1
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
	err := writeMessageToBackend(m, c.backend)
	c.ltqd.SetHealth(err)
	if err != nil {
		fmt.Println("CHANNEL(%s): put message to backend - %s", c.name, err)
		return err
	}
	return nil
}
