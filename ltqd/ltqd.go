package ltqd

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"math/big"
	"net"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"time"
)

type LTQD struct {
	topics       map[string]*Topic
	tcpServer    *tcpServer
	tcpListener  net.Listener
	httpListener net.Listener
	isLoading    int32 //加载的标志位
	isExiting    int32 //退出的标志位

	sync.RWMutex

	errValue atomic.Value

	//加载配置
	opts atomic.Value

	//当前ltqd的机器序列号
	clientIDSequence int64

	waitGroup WaitGroupWrapper

	exitChan chan int
	poolSize int

	//用系统锁给文件目录加锁
	dl *DirLock

	//通知lookup的channel
	notifyChan chan interface{}
}
type errStore struct {
	err error
}

// constructor of LTQD
// - 初始化channel中的各个成员变量
func New(opts *Options) (*LTQD, error) {
	var err error
	//设置选项
	dataPath := opts.DataPath
	if opts.DataPath == "" {
		cwd, _ := os.Getwd()
		dataPath = cwd
	}

	//创建ltqd
	l := &LTQD{
		topics:     make(map[string]*Topic),
		exitChan:   make(chan int, 1),
		dl:         NewDirLock(dataPath),
		notifyChan: make(chan interface{}, 1),
	}
	l.setOpts(opts)
	//给文件目录加锁
	err = l.dl.Lock()
	if err != nil {
		return nil, fmt.Errorf("failed to lock data-path: %v", err)
	}

	l.tcpServer = &tcpServer{ltqd: l}
	l.tcpListener, err = net.Listen(TypeOfAddr(opts.TCPAddress), opts.TCPAddress)
	if err != nil {
		return nil, fmt.Errorf("listen (%v) failed - %v", opts.TCPAddress, err)
	}

	if opts.HTTPAddress != "" {
		l.httpListener, err = net.Listen(TypeOfAddr(opts.HTTPAddress), opts.HTTPAddress)
		if err != nil {
			return nil, fmt.Errorf("listen (%v) failed - %v", opts.HTTPAddress, err)
		}
	}

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
	fmtLogf(Debug, "TOPIC(%v): created", t.name)
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
func (l *LTQD) Main() error {
	exitCh := make(chan error)
	var once sync.Once
	exitFunc := func(err error) {
		once.Do(func() {
			if err != nil {
				fmtLogf(Debug, "%v", err)
			}
			exitCh <- err
		})
	}

	l.waitGroup.Wrap(func() {
		exitFunc(TCPServer(l.tcpListener, l.tcpServer))
	})
	if l.httpListener != nil {
		httpServer := newHTTPServer(l)
		l.waitGroup.Wrap(func() {
			exitFunc(Serve(l.httpListener, httpServer, "HTTP"))
		})
	}

	l.waitGroup.Wrap(l.queueScanLoop)
	// l.waitGroup.Wrap(l.lookupLoop)

	err := <-exitCh
	return err
}

// queueScanLoop
// - 通过多个queueScanWorker 并发处理扫描处理各个channel
// - 通过概率过期算法 优化scan的策略
func (l *LTQD) queueScanLoop() {
	workCh := make(chan *Channel, l.getOpts().QueueScanSelectionCount)
	responseCh := make(chan bool, l.getOpts().QueueScanSelectionCount)
	closeCh := make(chan int)

	workTicker := time.NewTicker(l.getOpts().QueueScanInterval)
	refreshTicker := time.NewTicker(l.getOpts().QueueScanRefreshInterval)

	channels := l.channels()
	l.resizePool(len(channels), workCh, responseCh, closeCh)

	for {
		select {
		case <-workTicker.C:
			if len(channels) == 0 {
				continue
			}
		case <-refreshTicker.C:
			channels = l.channels()
			l.resizePool(len(channels), workCh, responseCh, closeCh)
			continue
		case <-l.exitChan:
			fmtLogf(Debug, "QUEUESCAN: closing")
			close(closeCh)
			workTicker.Stop()
			refreshTicker.Stop()
			return
		}

		num := l.getOpts().QueueScanSelectionCount
		if num > len(channels) {
			num = len(channels)
		}

		for {
			for _, i := range UniqRands(num, len(channels)) {
				workCh <- channels[i]
			}

			numDirty := 0
			for i := 0; i < num; i++ {
				if <-responseCh {
					numDirty++
				}
			}

			if float64(numDirty)/float64(num) <= l.getOpts().QueueScanDirtyPercent {
				break
			}
		}
	}

}

// resizePool
// 根据channel数量动态调整queueScanWorker的数量 对worker数量进行增减
func (l *LTQD) resizePool(num int, workCh chan *Channel, responseCh chan bool, closeCh chan int) {
	idealPoolSize := int(float64(num) * 0.25)
	if idealPoolSize < 1 {
		idealPoolSize = 1
	} else if idealPoolSize > l.getOpts().QueueScanWorkerPoolMax {
		idealPoolSize = l.getOpts().QueueScanWorkerPoolMax
	}
	for {
		if idealPoolSize == l.poolSize {
			break
		} else if idealPoolSize < l.poolSize {
			// contract
			closeCh <- 1
			l.poolSize--
		} else {
			// expand
			l.waitGroup.Wrap(func() {
				l.queueScanWorker(workCh, responseCh, closeCh)
			})
			l.poolSize++
		}
	}
}

// lookupLoop
// 每15s向lookup节点发送heartbeat
// 通过notifyChan接收来自topic/channel的创建/删除消息 向lookup节点register/unregister
// 通过optsNotificationChan接收opts变更的消息 更新lookup节点的地址
func lookupLoop() {

}

func (l *LTQD) queueScanWorker(workCh chan *Channel, responseCh chan bool, closeCh chan int) {
	for {
		select {
		case c := <-workCh:
			now := time.Now().UnixNano()
			dirty := false
			if c.processInFlightQueue(now) {
				dirty = true
			}
			responseCh <- dirty
		case <-closeCh:
			return
		}
	}
}

// 获取当前实例下面的所有topics的channel
func (l *LTQD) channels() []*Channel {
	var channels []*Channel
	l.RLock()
	for _, t := range l.topics {
		t.RLock()
		for _, c := range t.channels {
			channels = append(channels, c)
		}
		t.RUnlock()
	}
	l.RUnlock()
	return channels
}

func (l *LTQD) LoadMetadata() error {
	atomic.StoreInt32(&l.isLoading, 1)
	defer atomic.StoreInt32(&l.isLoading, 0)

	fn := newMetadataFile(l.getOpts())

	data, err := readOrEmpty(fn)
	if err != nil {
		return err
	}
	if data == nil {
		return nil // fresh start
	}

	var m Metadata
	err = json.Unmarshal(data, &m)
	if err != nil {
		return fmt.Errorf("failed to parse metadata in %s - %s", fn, err)
	}

	for _, t := range m.Topics {
		if !IsValidTopicName(t.Name) {
			fmtLogf(Debug, "skipping creation of invalid topic %s", t.Name)
			continue
		}
		topic := l.GetTopic(t.Name)
		for _, c := range t.Channels {
			if !IsValidChannelName(c.Name) {
				fmtLogf(Debug, "skipping creation of invalid channel %s", c.Name)
				continue
			}
			topic.GetChannel(c.Name)
		}
		topic.Start()
	}
	return nil
}

func (l *LTQD) Exit() {
	if !atomic.CompareAndSwapInt32(&l.isExiting, 0, 1) {
		// avoid double call
		return
	}
	if l.tcpListener != nil {
		l.tcpListener.Close()
	}

	if l.tcpServer != nil {
		l.tcpServer.Close()
	}

	if l.httpListener != nil {
		l.httpListener.Close()
	}

	l.Lock()
	err := l.PersistMetadata()
	if err != nil {
		fmtLogf(Debug, "failed to persist metadata - %s", err)
	}
	fmtLogf(Debug, "LTQ: closing topics")
	for _, topic := range l.topics {
		topic.Close()
	}
	l.Unlock()

	fmtLogf(Debug, "LTQ: stopping subsystems")
	close(l.exitChan)
	l.waitGroup.Wait()
	//给文件目录解锁
	l.dl.Unlock()
	fmtLogf(Debug, "LTQ: bye")
}

func (l *LTQD) PersistMetadata() error {
	fileName := newMetadataFile(l.getOpts())

	fmtLogf(Debug, "LTQ: persisting topic/channel metadata to %s", fileName)

	data, err := json.Marshal(l.GetMetadata())
	if err != nil {
		return err
	}
	maxInt := big.NewInt(1<<31 - 1)
	n, err := rand.Int(rand.Reader, maxInt)
	if err != nil {
		return fmt.Errorf("failed to generate random number: %v", err)
	}
	tmpFileName := fmt.Sprintf("%s.%d.tmp", fileName, n.Int64())
	err = writeSyncFile(tmpFileName, data)
	if err != nil {
		return err
	}
	err = os.Rename(tmpFileName, fileName)
	if err != nil {
		return err
	}

	return nil
}

func (l *LTQD) GetMetadata() *Metadata {
	meta := &Metadata{}
	for _, topic := range l.topics {
		topicData := TopicMetadata{
			Name: topic.name,
		}
		topic.Lock()
		for _, channel := range topic.channels {
			topicData.Channels = append(topicData.Channels, ChannelMetadata{
				Name: channel.name,
			})
		}
		topic.Unlock()
		meta.Topics = append(meta.Topics, topicData)
	}
	return meta
}

func (l *LTQD) Notify(v interface{}) {
	loading := atomic.LoadInt32(&l.isLoading) == 1
	l.waitGroup.Wrap(func() {
		select {
		case <-l.exitChan:
		case l.notifyChan <- v: //通知lookupd有更新
			if loading {
				return
			}
			l.Lock()
			err := l.PersistMetadata()
			if err != nil {
				fmtLogf(Debug, "failed to persist metadata - %s", err)
			}
			l.Unlock()
		}
	})
}

type Metadata struct {
	Topics []TopicMetadata `json:"topics"`
}

type TopicMetadata struct {
	Name     string            `json:"name"`
	Channels []ChannelMetadata `json:"channels"`
}

type ChannelMetadata struct {
	Name string `json:"name"`
}

func newMetadataFile(opts *Options) string {
	return path.Join(opts.DataPath, "ltqd.dat")
}

func readOrEmpty(fn string) ([]byte, error) {
	data, err := os.ReadFile(fn)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("failed to read metadata from %s - %s", fn, err)
		}
	}
	return data, nil
}

func writeSyncFile(fn string, data []byte) error {
	f, err := os.OpenFile(fn, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}

	_, err = f.Write(data)
	if err == nil {
		err = f.Sync()
	}
	f.Close()
	return err
}
