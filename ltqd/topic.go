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

// Exiting
// - 判断当前topic是否退出
// - 原子操作避免数据竞争 防止数据不一致和错误
func Exiting() {

}

// GetChannel
// - 从当前topic中取某个channel 如果channel不存在需要创建
// - 通过channelUpdateChan，在新创建channel时与messagePump做消息传递 messagePump需要重新获取所有的channel
// - 加锁保证GetChannel操作的线程安全 防止并发产生错误
func GetChannel() {

}

// GetExistingChannel
// - 保证channel存在的前提下 获取channel的方法
// - 相比于GetChannel不存在create channel的行为 通过sync.RWMutex提高并发性能和效率
func GetExistingChannel() {

}

// DeleteExistingChannel
// - 保证channel存在的前提下 从当前topic下删除channel
// - 通过channelUpdateChan，在删除channel时与messagePump做消息传递 messagePump需要重新获取所有的channel
func DeleteExistingChannel() {

}

// messagePump
// - 当前topic的消息处理函数
// - 不断select接收来自in-memory queue和backend queue的消息 并复制到当前topic的每个channel
func messagePump() {

}

func (t *Topic) PutMessage() {

}
