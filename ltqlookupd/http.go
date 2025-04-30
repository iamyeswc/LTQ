package ltqlookupd

import (
	"errors"
	"io"
	"net/http"
	"net/url"

	"github.com/julienschmidt/httprouter"
)

type httpServer struct {
	ltqlookupd *LTQLOOKUPD

	router http.Handler
}

func newHTTPServer(ltqlookupd *LTQLOOKUPD) *httpServer {
	log := Log(fmtLogf)

	router := httprouter.New()
	s := &httpServer{
		ltqlookupd: ltqlookupd,
		router:     router,
	}

	//for consumer: 根据topic查询所有的ltqd的实例的信息, consumer需要根据查询的信息连接所有的实例
	router.Handle("GET", "/clookup", Decorate(s.cLookup, log, V1))

	//for producer: 根据topic查询所有的ltqd的实例的信息
	//提供参数fifo给producer, producer可以选择是否使用fifo
	//如果使用fifo, 那么ltqd会将消息放到后端的磁盘队列中，同时只返回其中一个ltqd的实例（如果没有查到topic, 会随机选择一个ltqd的实例新建topic）
	//如果不使用fifo, 那么ltqd会将消息放到内存队列和磁盘队列中，同时随机返回一个ltqd的实例
	router.Handle("GET", "/plookup", Decorate(s.pLookup, log, V1))

	return s
}

func (s *httpServer) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	s.router.ServeHTTP(w, req)
}

func (s *httpServer) cLookup(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := NewReqParams(req)
	if err != nil {
		return nil, Err{400, "INVALID_REQUEST"}
	}

	topicName, err := reqParams.Get("topic")
	if err != nil {
		return nil, Err{400, "MISSING_ARG_TOPIC"}
	}

	registration := s.ltqlookupd.DB.FindRegistrations("topic", topicName, "")
	if len(registration) == 0 {
		return nil, Err{404, "TOPIC_NOT_FOUND"}
	}

	channels := s.ltqlookupd.DB.FindRegistrations("channel", topicName, "*").SubKeys()
	producers := s.ltqlookupd.DB.FindProducers("topic", topicName, "")
	producers = producers.FilterByActive(s.ltqlookupd.opts.InactiveProducerTimeout)
	return map[string]interface{}{
		"channels":  channels,
		"producers": producers.PeerInfo(),
	}, nil
}
func (s *httpServer) pLookup(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := NewReqParams(req)
	if err != nil {
		return nil, Err{400, "INVALID_REQUEST"}
	}

	topicName, err := reqParams.Get("topic")
	if err != nil {
		return nil, Err{400, "MISSING_ARG_TOPIC"}
	}

	fifo, err := reqParams.Get("fifo")
	if err != nil {
		return nil, Err{400, "MISSING_ARG_FIFO"}
	}
	if fifo != "true" && fifo != "false" {
		return nil, Err{400, "INVALID_ARG_FIFO"}
	}
	if fifo == "false" {
		//找到所有的ltqd的实例, 随机选择topic里面的一个实例
		producers := s.ltqlookupd.DB.FindProducers("topic", topicName, "")
		producers = producers.FilterByActive(s.ltqlookupd.opts.InactiveProducerTimeout)
		//随机选择一个实例
		if len(producers) == 0 {
			return nil, Err{404, "TOPIC_NOT_FOUND"}
		}
		return producers.RandomPeerInfo(), nil
	} else {
		//fifo为true, 需要在ltqd中创建topic
		//如果没有找到topic, 随机选择一个ltqd的实例新建topic
		registration := s.ltqlookupd.DB.FindRegistrations("topic", topicName, "")
		if len(registration) == 0 {
			//随机选择一个ltqd的实例新建topic
			producers := s.ltqlookupd.DB.FindProducers("client", "", "").FilterByActive(s.ltqlookupd.opts.InactiveProducerTimeout)
			//随机选择一个实例
			producer := producers.RandomPeerInfo()
			//创建topic
			key := Registration{"topic", topicName, ""}
			err := s.ltqlookupd.DB.AddRegistrationToProducer(key, producer)
			if err != nil {
				return nil, Err{500, "CREATE_TOPIC_FAILED"}
			}
			return producer, nil
		} else {
			//如果找到了topic, 那么就返回随机的ltqd的实例的信息
			producers := s.ltqlookupd.DB.FindProducers("topic", topicName, "")
			producers = producers.FilterByActive(s.ltqlookupd.opts.InactiveProducerTimeout)
			return producers.RandomPeerInfo(), nil
		}

	}

}

type ReqParams struct {
	url.Values
	Body []byte
}

func NewReqParams(req *http.Request) (*ReqParams, error) {
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		return nil, err
	}

	data, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}

	return &ReqParams{reqParams, data}, nil
}

func (r *ReqParams) Get(key string) (string, error) {
	v, ok := r.Values[key]
	if !ok {
		return "", errors.New("key not in query params")
	}
	return v[0], nil
}
