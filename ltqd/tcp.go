package ltqd

import (
	"errors"
	"fmt"
	"net"
	"runtime"
	"sync"
)

type TCPHandler interface {
	Handle(net.Conn)
}

func TCPServer(listener net.Listener, handler TCPHandler) error {
	fmtLogf(Debug, "TCP: listening on %s", listener.Addr())

	var wg sync.WaitGroup

	for {
		clientConn, err := listener.Accept()
		if err != nil {
			if te, ok := err.(interface{ Temporary() bool }); ok && te.Temporary() {
				fmtLogf(Debug, "temporary Accept() failure - %s", err)
				runtime.Gosched()
				continue
			}
			if !errors.Is(err, net.ErrClosed) {
				return fmt.Errorf("listener.Accept() error - %s", err)
			}
			break
		}

		wg.Add(1)
		go func() {
			handler.Handle(clientConn)
			wg.Done()
		}()
	}

	wg.Wait()

	fmtLogf(Debug, "TCP: closing %s", listener.Addr())

	return nil
}
