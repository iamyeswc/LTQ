package ltqlookupd

import (
	"fmt"
	"net"
	"sync"
)

type LTQLOOKUPD struct {
	//和ltqd的连接
	tcpListener net.Listener
	tcpServer   *tcpServer
	//和producer consumer的连接
	httpListener net.Listener
	waitGroup    WaitGroupWrapper
	DB           *RegistrationDB

	opts *Options

	sync.RWMutex
}

func New(opts *Options) (*LTQLOOKUPD, error) {
	var err error

	l := &LTQLOOKUPD{
		opts: opts,
		DB:   NewRegistrationDB(),
	}

	l.tcpServer = &tcpServer{ltqlookupd: l}
	l.tcpListener, err = net.Listen("tcp", opts.TCPAddress)
	if err != nil {
		return nil, fmt.Errorf("listen (%v) failed - %v", opts.TCPAddress, err)
	}
	l.httpListener, err = net.Listen("tcp", opts.HTTPAddress)
	if err != nil {
		return nil, fmt.Errorf("listen (%v) failed - %v", opts.HTTPAddress, err)
	}

	return l, nil
}

func (l *LTQLOOKUPD) RealTCPAddr() *net.TCPAddr {
	return l.tcpListener.Addr().(*net.TCPAddr)
}

func (l *LTQLOOKUPD) RealHTTPAddr() *net.TCPAddr {
	return l.httpListener.Addr().(*net.TCPAddr)
}
