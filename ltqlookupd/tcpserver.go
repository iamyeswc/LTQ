package ltqlookupd

import (
	"net"
	"sync"
)

type tcpServer struct {
	ltqlookupd *LTQLOOKUPD
	conns      sync.Map
}

func (p *tcpServer) Handle(conn net.Conn) {
	fmtLogf(Debug, "TCP: new client(%v)", conn.RemoteAddr())

	prot := &Protocol{ltqlookupd: p.ltqlookupd}

	client := prot.NewClient(conn)
	p.conns.Store(conn.RemoteAddr(), client)

	err := prot.IOLoop(client)
	if err != nil {
		fmtLogf(Debug, "client(%v) - %v", conn.RemoteAddr(), err)
	}

	p.conns.Delete(conn.RemoteAddr())
	client.Close()
}

func (p *tcpServer) Close() {
	p.conns.Range(func(k, v interface{}) bool {
		v.(Client).Close()
		return true
	})
}
