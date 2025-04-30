package ltqlookupd

import (
	"net"
)

type client struct {
	net.Conn
	peerInfo *PeerInfo
}

func newClient(conn net.Conn) *client {
	return &client{
		Conn: conn,
	}
}
