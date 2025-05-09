package ltqd

import (
	"bufio"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

const defaultBufferSize = 16 * 1024

const (
	stateInit = iota
	stateDisconnected
	stateConnected
	stateSubscribed
	stateClosing
)

type client struct {
	ID        int64
	ltqd      *LTQD
	UserAgent string

	Reader *bufio.Reader
	Writer *bufio.Writer

	net.Conn

	//有订阅事件
	SubEventChan chan *Channel
	ExitChan     chan int
	MsgTimeout   time.Duration

	writeLock sync.RWMutex

	//订阅的channel
	Channel *Channel

	State int32

	lenBuf   [4]byte
	lenSlice []byte

	IdentifyEventChan chan identifyEvent

	ClientID string
	Hostname string
	metaLock sync.RWMutex

	ReadyStateChan chan int
	ReadyCount     int64
}

type identifyData struct {
	ClientID   string `json:"client_id"`
	Hostname   string `json:"hostname"`
	MsgTimeout int    `json:"msg_timeout"`
}

type identifyEvent struct {
	MsgTimeout time.Duration
}

func newClient(id int64, conn net.Conn, ltqd *LTQD) *client {
	c := &client{
		ID:                id,
		ltqd:              ltqd,
		Conn:              conn,
		Reader:            bufio.NewReaderSize(conn, defaultBufferSize),
		Writer:            bufio.NewWriterSize(conn, defaultBufferSize),
		SubEventChan:      make(chan *Channel, 1),
		ExitChan:          make(chan int, 1),
		MsgTimeout:        ltqd.getOpts().MsgTimeout,
		IdentifyEventChan: make(chan identifyEvent, 1),
		ReadyStateChan:    make(chan int, 1),
	}
	c.lenSlice = c.lenBuf[:]
	return c
}

// 把数据写入底层连接
func (c *client) Flush() error {
	err := c.Writer.Flush()
	if err != nil {
		return err
	}

	return nil
}

func (c *client) Identify(data identifyData) error {
	var err error
	fmtLogf(Debug, "[%v] IDENTIFY: %+v", c, data)

	c.metaLock.Lock()
	c.ClientID = data.ClientID
	c.Hostname = data.Hostname
	c.metaLock.Unlock()

	err = c.SetMsgTimeout(data.MsgTimeout)
	if err != nil {
		return err
	}

	ie := identifyEvent{
		MsgTimeout: c.MsgTimeout,
	}

	select {
	case c.IdentifyEventChan <- ie:
	default:
	}

	return nil
}

func (c *client) SetMsgTimeout(msgTimeout int) error {
	c.writeLock.Lock()
	defer c.writeLock.Unlock()

	switch {
	case msgTimeout == 0:
	case msgTimeout >= 1000 &&
		msgTimeout <= int(c.ltqd.getOpts().MaxMsgTimeout/time.Millisecond):
		c.MsgTimeout = time.Duration(msgTimeout) * time.Millisecond
	default:
		return fmt.Errorf("msg timeout (%d) is invalid", msgTimeout)
	}

	return nil
}

func (c *client) SetReadyCount(count int64) {
	oldCount := atomic.SwapInt64(&c.ReadyCount, count)

	if oldCount != count {
		c.tryUpdateReadyState()
	}
}

func (c *client) tryUpdateReadyState() {
	select {
	case c.ReadyStateChan <- 1:
	default:
	}
}

func (c *client) IsReadyForMessages() bool {

	readyCount := atomic.LoadInt64(&c.ReadyCount)

	// fmtLogf(Debug, "state rdy: %d", readyCount)

	return readyCount > 0
}
