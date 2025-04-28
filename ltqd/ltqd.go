package ltqd

import "net"

type LTQD struct {
	Topics       map[string]*Topic
	tcpListener  net.Listener
	httpListener net.Listener
}

func GetTopic(name string) *Topic {
	return nil
}

func Main() {

}
