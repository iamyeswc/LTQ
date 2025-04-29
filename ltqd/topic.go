package ltqd

type Topic struct {
	name           string
	ltqd           *LTQD
	memoryMsgChan  chan *Message
	backendMsgChan chan *Message
	channels       map[string]*Channel
}

// constructor of Topic
// - 初始化topic中的各个成员变量
// - 启动messagePump协程 利用sync.WaitGroup做同步
// - 通知ltqd 触发新topic建立时 ltqd需要做的工作(metadata持久化到磁盘、通知lookup注册了新的topic)
func NewTopic() *Topic {

	go messagePump()

	return &Topic{}
}

// Start
// - 控制topic的开启 开始接收消息(控制messagePump的开启)
// - 通过一个startChan来做goroutine之间的消息传递
func Start() {
	
}

// messagePump
// - 当前topic的消息处理函数
// - 不断select接收来自in-memory queue和backend queue的消息 并复制到当前topic的每个channel
func messagePump() {

}

func (t *Topic) PutMessage() {

}
