package ltqd

type Channel struct {
	name          string
	ltqd          *LTQD
	memoryMsgChan chan *Message
}

// constructor of Channel
// - 初始化channel中的各个成员变量
// - 通知ltqd 触发新channel建立时 ltqd需要做的工作(metadata持久化到磁盘、通知lookup注册了新的channel)
func NewChannel() *Channel {
	return &Channel{}
}

// Exiting
// - 判断当前channel是否退出
// - 原子操作避免数据竞争 防止数据不一致和错误
func (c *Channel) Exiting() {

}

func (c *Channel) PutMessage() {

}
