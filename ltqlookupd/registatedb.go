package ltqlookupd

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

type PeerInfo struct {
	lastUpdate int64
	id         string
	Hostname   string `json:"hostname"`
	TCPPort    int    `json:"tcp_port"`
}

// 注册信息，分类可以是topic或者channel， 下面绑定了所有对应分类的实例信息
type RegistrationDB struct {
	sync.RWMutex
	registrationMap map[Registration]ProducerMap
}

type Registration struct {
	Category string
	Key      string
	//key的subkey是空
	//channel的subkey是channel的名字
	SubKey string
}

type ProducerMap map[string]*Producer

type Producer struct {
	peerInfo *PeerInfo
}

type Registrations []Registration
type Producers []*Producer

func NewRegistrationDB() *RegistrationDB {
	return &RegistrationDB{
		registrationMap: make(map[Registration]ProducerMap),
	}
}

func (r *RegistrationDB) AddProducer(k Registration, p *Producer) bool {
	r.Lock()
	defer r.Unlock()
	_, ok := r.registrationMap[k]
	if !ok {
		r.registrationMap[k] = make(map[string]*Producer)
	}
	producers := r.registrationMap[k]
	_, found := producers[p.peerInfo.id]
	if !found {
		producers[p.peerInfo.id] = p
	}
	return !found
}

func (r *RegistrationDB) needFilter(key string, subkey string) bool {
	return key == "*" || subkey == "*"
}

func (r *RegistrationDB) PrintAllRegistrationDB() {
	r.RLock()
	defer r.RUnlock()

	for reg, producerMap := range r.registrationMap {
		fmtLogf(Debug, "Registration: Category=%v, Key=%v, SubKey=%v\n", reg.Category, reg.Key, reg.SubKey)
		for id, producer := range producerMap {
			if producer != nil && producer.peerInfo != nil {
				fmtLogf(Debug, "Producer ID: %v, PeerInfo: %+v\n", id, producer.peerInfo)
			} else {
				fmtLogf(Debug, "Producer ID: %v, PeerInfo: nil\n", id)
			}
		}
	}
}

func (r *RegistrationDB) FindRegistrations(category string, key string, subkey string) Registrations {
	r.RLock()
	defer r.RUnlock()
	//非通配符查找
	if !r.needFilter(key, subkey) {
		//在map中查找
		k := Registration{category, key, subkey}
		if _, ok := r.registrationMap[k]; ok {
			return Registrations{k}
		}
		return Registrations{}
	}
	//通配符查找
	results := Registrations{}
	for k := range r.registrationMap {
		if !k.IsMatch(category, key, subkey) {
			continue
		}
		results = append(results, k)
	}
	return results
}

func (k Registration) IsMatch(category string, key string, subkey string) bool {
	//判断分类是否相同
	if category != k.Category {
		return false
	}
	if key != "*" && k.Key != key {
		return false
	}
	if subkey != "*" && k.SubKey != subkey {
		return false
	}
	return true
}

// 拿出所有的subkey
func (rr Registrations) SubKeys() []string {
	subkeys := make([]string, len(rr))
	for i, k := range rr {
		subkeys[i] = k.SubKey
	}
	return subkeys
}

// 找出所有节点的信息
func (r *RegistrationDB) FindProducers(category string, key string, subkey string) Producers {
	r.RLock()
	defer r.RUnlock()
	//非通配符查找，直接在map中查找
	if !r.needFilter(key, subkey) {
		k := Registration{category, key, subkey}
		return ProducerMap2Slice(r.registrationMap[k])
	}

	//通配符查找
	// 1. 先找到所有的注册信息
	// 2. 遍历所有的注册信息，找到对应的节点
	// 3. 去重
	// 4. 返回所有的节点
	results := make(map[string]struct{})
	var retProducers Producers
	for k, producers := range r.registrationMap {
		if !k.IsMatch(category, key, subkey) {
			continue
		}
		for _, producer := range producers {
			_, found := results[producer.peerInfo.id]
			if !found {
				results[producer.peerInfo.id] = struct{}{}
				retProducers = append(retProducers, producer)
			}
		}
	}
	return retProducers
}

func ProducerMap2Slice(pm ProducerMap) Producers {
	var producers Producers
	for _, producer := range pm {
		producers = append(producers, producer)
	}

	return producers
}

// 找到活跃的实例连接
func (pp Producers) FilterByActive(inactivityTimeout time.Duration) Producers {
	now := time.Now()
	results := Producers{}
	for _, p := range pp {
		cur := time.Unix(0, atomic.LoadInt64(&p.peerInfo.lastUpdate))
		//如果当前时间减去上次更新时间大于超时时间，说明该连接已经不活跃
		//直接跳过
		if now.Sub(cur) > inactivityTimeout {
			continue
		}
		results = append(results, p)
	}
	return results
}

// 从给定的Producers集合中提取出每个生产者的PeerInfo
func (pp Producers) PeerInfo() []*PeerInfo {
	results := []*PeerInfo{}
	for _, p := range pp {
		results = append(results, p.peerInfo)
	}
	return results
}

// 从给定的Producers集合中随机选择一个PeerInfo
func (pp Producers) RandomPeerInfo() *PeerInfo {
	if len(pp) == 0 {
		return nil
	}
	//随机选择一个实例
	index := rand.Intn(len(pp))
	return pp[index].peerInfo
}

// 添加注册信息
func (r *RegistrationDB) AddRegistrationToProducer(k Registration, p *PeerInfo) error {
	r.Lock()
	defer r.Unlock()
	_, ok := r.registrationMap[k]
	if !ok {
		r.registrationMap[k] = make(map[string]*Producer)
	}
	return nil
}

// 查找所有的注册信息
func (r *RegistrationDB) LookupRegistrations(id string) Registrations {
	r.RLock()
	defer r.RUnlock()
	results := Registrations{}
	for k, producers := range r.registrationMap {
		if _, exists := producers[id]; exists {
			results = append(results, k)
		}
	}
	return results
}

func (r *RegistrationDB) RemoveProducer(k Registration, id string) (bool, int) {
	r.Lock()
	defer r.Unlock()
	producers, ok := r.registrationMap[k]
	if !ok {
		return false, 0
	}
	removed := false
	if _, exists := producers[id]; exists {
		removed = true
	}

	delete(producers, id)
	return removed, len(producers)
}
