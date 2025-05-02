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

// 监听客户端的HTTP请求
// 监听ltqd的TCP请求
func (l *LTQLOOKUPD) Main() error {
	fmtLogf(Debug, "start main")
	exitCh := make(chan error)
	var once sync.Once
	exitFunc := func(err error) {
		once.Do(func() {
			if err != nil {
				fmtLogf(Debug, "%v", err)
			}
			exitCh <- err
		})
	}

	l.waitGroup.Wrap(func() {
		exitFunc(TCPServer(l.tcpListener, l.tcpServer))
	})
	httpServer := newHTTPServer(l)
	l.waitGroup.Wrap(func() {
		exitFunc(Serve(l.httpListener, httpServer, "HTTP"))
	})

	err := <-exitCh
	return err

}

func (l *LTQLOOKUPD) Exit() {
	if l.tcpListener != nil {
		l.tcpListener.Close()
	}

	if l.tcpServer != nil {
		l.tcpServer.Close()
	}

	if l.httpListener != nil {
		l.httpListener.Close()
	}
	l.waitGroup.Wait()
}
