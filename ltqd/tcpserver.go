package ltqd

import (
	"net"
	"sync"
)

const (
	typeConsumer = iota
	typeProducer
)

// type Client interface {
// 	Type() int
// }

type tcpServer struct {
	ltqd  *LTQD
	conns sync.Map
}

func (p *tcpServer) Handle(conn net.Conn) {
	fmtLogf(Debug, "TCP: new client(%s)", conn.RemoteAddr())

	prot := &Protocol{ltqd: p.ltqd}

	client := prot.NewClient(conn)
	p.conns.Store(conn.RemoteAddr(), client)

	err := prot.IOLoop(client)
	if err != nil {
		fmtLogf(Debug, "client(%s) - %s", conn.RemoteAddr(), err)
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
