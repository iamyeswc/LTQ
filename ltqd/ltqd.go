package ltqd

import (
	"net"
	"sync"
	"sync/atomic"
)

type LTQD struct {
	topics       map[string]*Topic
	tcpListener  net.Listener
	httpListener net.Listener
	isLoading    int32 //加载的标志位

	sync.RWMutex

	errValue atomic.Value

	//加载配置
	opts atomic.Value
}
type errStore struct {
	err error
}

// constructor of LTQD
// - 初始化channel中的各个成员变量
func New(opts *Options) (*LTQD, error) {
	//设置选项
	// dataPath := opts.DataPath
	// if opts.DataPath == "" {
	// 	cwd, _ := os.Getwd()
	// 	dataPath = cwd
	// }

	//创建ltqd
	l := &LTQD{
		topics: make(map[string]*Topic),
	}
	l.setOpts(opts)

	return l, nil
}

func (l *LTQD) GetTopic(name string) *Topic {
	//对topics加锁，避免并发写
	l.RLock()
	t, ok := l.topics[name]
	l.RUnlock()
	if ok {
		return t
	}

	//如果不存在topic，则创建一个新的topic
	l.Lock()

	t, ok = l.topics[name]
	if ok {
		l.Unlock()
		return t
	}
	//使用Topic构造函数
	t = NewTopic(name, l)
	l.topics[name] = t
	l.Unlock()
	fmtLogf(Debug, "TOPIC(%s): created", t.name)
	//当创建了topic后需要给topic startChan发送标志，开始messagePump

	//如果正在loading中，则不需要启动messagePump
	if atomic.LoadInt32(&l.isLoading) == 1 {
		//loading结束后，会启动所有channel的messagePump
		return t
	}

	//开始messagePump
	t.Start()
	return t
}

func (l *LTQD) SetHealth(err error) {
	l.errValue.Store(errStore{err: err})
}

func (l *LTQD) IsHealthy() bool {
	return l.GetError() == nil
}

func (l *LTQD) GetError() error {
	errValue := l.errValue.Load()
	return errValue.(errStore).err
}

// 配置选项参数
func (l *LTQD) getOpts() *Options {
	return l.opts.Load().(*Options)
}

func (l *LTQD) setOpts(opts *Options) {
	l.opts.Store(opts)
}

// Main
// - ltqd节点启动的主要逻辑
// - 启动TCP server协程 开始接受并处理来自client的TCP连接
// - 启动HTTP server协程 开始接受并处理来自client的HTTP连接
// - 启动queueScanLoop协程 处理in-flight queue和defered queue中的message
// - 启动lookupLoop协程 与lookup节点交互
func Main() {

}

// queueScanLoop
// - 通过多个queueScanWorker 并发处理扫描处理各个channel
// - 通过概率过期算法 优化scan的策略
func queueScanLoop() {

}

// resizePool
// 根据channel数量动态调整queueScanWorker的数量 对worker数量进行增减
func resizePool() {

}

// lookupLoop
// 每15s向lookup节点发送heartbeat
// 通过notifyChan接收来自topic/channel的创建/删除消息 向lookup节点register/unregister
// 通过optsNotificationChan接收opts变更的消息 更新lookup节点的地址
func lookupLoop() {

}
