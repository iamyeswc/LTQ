package ltqd

import (
	"io"
	"net/http"
	"net/url"

	"github.com/julienschmidt/httprouter"
)

type httpServer struct {
	ltqd *LTQD

	router http.Handler
}

func newHTTPServer(ltqd *LTQD) *httpServer {
	log := Log(fmtLogf)

	router := httprouter.New()
	s := &httpServer{
		ltqd:   ltqd,
		router: router,
	}

	router.Handle("POST", "/pub", Decorate(s.doPUB, log, V1))

	return s
}

func (s *httpServer) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	s.router.ServeHTTP(w, req)
}

func (s *httpServer) doPUB(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {

	if req.ContentLength > s.ltqd.getOpts().MaxMsgSize {
		return nil, Err{413, "MSG_TOO_BIG"}
	}

	readMax := s.ltqd.getOpts().MaxMsgSize + 1
	body, err := io.ReadAll(io.LimitReader(req.Body, readMax))
	if err != nil {
		return nil, Err{500, "INTERNAL_ERROR"}
	}
	if int64(len(body)) == readMax {
		return nil, Err{413, "MSG_TOO_BIG"}
	}
	if len(body) == 0 {
		return nil, Err{400, "MSG_EMPTY"}
	}

	_, topic, err := s.getTopicFromQuery(req)
	if err != nil {
		return nil, err
	}

	msg := NewMessage(topic.GenerateID(), body)
	err = topic.PutMessage(msg)
	if err != nil {
		return nil, Err{503, "EXITING"}
	}

	return "OK", nil
}

func (s *httpServer) getTopicFromQuery(req *http.Request) (url.Values, *Topic, error) {
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		fmtLogf(Debug, "failed to parse request params - %s", err)
		return nil, nil, Err{400, "INVALID_REQUEST"}
	}

	topicNames, ok := reqParams["topic"]
	if !ok {
		return nil, nil, Err{400, "MISSING_ARG_TOPIC"}
	}
	topicName := topicNames[0]

	if !IsValidTopicName(topicName) {
		return nil, nil, Err{400, "INVALID_TOPIC"}
	}

	return reqParams, s.ltqd.GetTopic(topicName), nil
}
