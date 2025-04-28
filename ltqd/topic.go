package ltqd

type Topic struct {
	name           string
	ltqd           *LTQD
	memoryMsgChan  chan *Message
	backendMsgChan chan *Message
	channels       map[string]*Channel
}

// construtor of Topic
func NewTopic() *Topic {

	go messagePump()

	return &Topic{}
}

func messagePump() {

}

func (t *Topic) PutMessage() {

}
