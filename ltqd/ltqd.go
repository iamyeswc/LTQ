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

func NewLTQD(opts *Options) (*LTQD, error) {
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

func Main() {

}
