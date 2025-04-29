package ltqd

import (
	"bufio"
	"net"
	"sync"
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
}

func newClient(id int64, conn net.Conn, ltqd *LTQD) *client {
	c := &client{
		ID:           id,
		ltqd:         ltqd,
		Conn:         conn,
		Reader:       bufio.NewReaderSize(conn, defaultBufferSize),
		Writer:       bufio.NewWriterSize(conn, defaultBufferSize),
		SubEventChan: make(chan *Channel, 1),
		ExitChan:     make(chan int, 1),
		MsgTimeout:   ltqd.getOpts().MsgTimeout,
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
