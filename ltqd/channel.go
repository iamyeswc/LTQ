package ltqd

type Channel struct {
	name          string
	ltqd          *LTQD
	memoryMsgChan chan *Message
}

func (c *Channel) PutMessage() {

}
