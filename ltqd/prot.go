package ltqd

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"regexp"
	"strconv"
	"sync/atomic"
	"time"
	"unsafe"
)

var separatorBytes = []byte(" ")
var okBytes = []byte("OK")

var validTopicChannelNameRegex = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

type Client interface {
	Close() error
}

type Protocol struct {
	ltqd *LTQD
}

func (p *Protocol) NewClient(conn net.Conn) Client {
	clientID := atomic.AddInt64(&p.ltqd.clientIDSequence, 1)
	//创建一个到当前ltqd的客户端
	return newClient(clientID, conn, p.ltqd)
}

func (p *Protocol) IOLoop(c Client) error {
	var err error
	var line []byte

	client := c.(*client)

	messagePumpStartedChan := make(chan bool)
	go p.messagePump(client, messagePumpStartedChan)
	<-messagePumpStartedChan

	for {
		line, err = client.Reader.ReadSlice('\n')
		if err != nil {
			if err == io.EOF {
				err = nil
			} else {
				err = fmt.Errorf("failed to read command - %v", err)
			}
			break
		}

		line = line[:len(line)-1]
		if len(line) > 0 && line[len(line)-1] == '\r' {
			line = line[:len(line)-1]
		}
		params := bytes.Split(line, separatorBytes)

		// fmtLogf(Debug, "PROTOCOL: [%v] - %v", client, params)
		// for i := 0; i < len(params); i++ {
		// 	fmtLogf(Debug, "PROTOCOL=>: %s", string(params[i]))
		// }

		var response []byte
		response, err = p.Exec(client, params)
		if err != nil {
			// fmtLogf(Debug, "[%v] - err:%v", client, err)

			sendErr := p.Send(client, []byte(err.Error()))
			if sendErr != nil {
				fmtLogf(Debug, "[%v] - err:%v", client, sendErr)
				break
			}
			continue
		}

		if response != nil {
			err = p.Send(client, response)
			if err != nil {
				err = fmt.Errorf("failed to send response - %v", err)
				break
			}
		}
	}

	fmtLogf(Debug, "PROTOCOL: [%v] exiting ioloop", client)
	close(client.ExitChan)
	if client.Channel != nil {
		client.Channel.RemoveClient(client.ID)
	}

	return err
}

func (p *Protocol) messagePump(client *client, startedChan chan bool) {
	var err error
	var memoryMsgChan chan *Message
	var backendMsgChan <-chan []byte
	var subChannel *Channel

	subEventChan := client.SubEventChan
	identifyEventChan := client.IdentifyEventChan
	msgTimeout := client.MsgTimeout

	flushed := true

	close(startedChan)

	for {
		// if subChannel != nil {
		// 	fmtLogf(Debug, "subchan len:%d", subChannel.messageCount)
		// } else {
		// 	fmtLogf(Debug, "subchan is nil")
		// }
		var b []byte
		var msg *Message

		if subChannel == nil || !client.IsReadyForMessages() {
			memoryMsgChan = nil
			backendMsgChan = nil
			client.writeLock.Lock()
			err = client.Flush()
			client.writeLock.Unlock()
			if err != nil {
				fmtLogf(Debug, "PROTOCOL: [%v] messagePump error - %v", client, err)
				return
			}
			flushed = true
		} else if flushed {
			memoryMsgChan = subChannel.memoryMsgChan
			backendMsgChan = subChannel.backendMsgChan.ReadChan()
		} else {
			memoryMsgChan = subChannel.memoryMsgChan
			backendMsgChan = subChannel.backendMsgChan.ReadChan()
		}

		select {
		case subChannel = <-subEventChan:
			// fmtLogf(Debug, "subEventChan=======")
			subEventChan = nil
		case identifyData := <-identifyEventChan:
			msgTimeout = identifyData.MsgTimeout
		case b = <-backendMsgChan:
			// fmtLogf(Debug, "backendMsgChan=======")
		case msg = <-memoryMsgChan:
			// fmtLogf(Debug, "memoryMsgChan=======")
		case <-client.ReadyStateChan:
			// fmtLogf(Debug, "ReadyStateChan=======")
		case <-client.ExitChan:
			fmtLogf(Debug, "PROTOCOL: [%v] exiting messagePump", client)
			return
		}
		if len(b) != 0 {
			msg, err = decodeMessage(b)
			if err != nil {
				fmtLogf(Debug, "failed to decode message - %v", err)
				continue
			}
		}
		if msg != nil {
			msg.Attempts++
			subChannel.StartInFlightTimeout(msg, client.ID, msgTimeout)
			err = p.SendMessage(client, msg)
			if err != nil {
				fmtLogf(Debug, "PROTOCOL: [%v] messagePump error - %v", client, err)
				return
			}
			// fmtLogf(Debug, "send message done, msg:%s, id:%s", string(msg.Body), string(msg.ID[:]))
			flushed = false
		}

	}

}

func (p *Protocol) SendMessage(client *client, msg *Message) error {
	// fmtLogf(Debug, "PROTOCOL: writing msg(%v) to client(%v) - %v", msg.ID, client, msg.Body)

	buf := bufferPoolGet()
	defer bufferPoolPut(buf)

	_, err := msg.WriteTo(buf)
	if err != nil {
		return err
	}

	err = p.Send(client, buf.Bytes())
	if err != nil {
		return err
	}

	return nil
}

func (p *Protocol) Send(client *client, data []byte) error {
	client.writeLock.Lock()

	_, err := SendFramedResponse(client.Writer, data)
	if err != nil {
		client.writeLock.Unlock()
		return err
	}

	err = client.Flush()

	client.writeLock.Unlock()

	return err
}

func SendFramedResponse(w io.Writer, data []byte) (int, error) {
	// fmtLogf(Debug, "SendFramedResponse====")
	beBuf := make([]byte, 4)
	size := uint32(len(data))

	binary.BigEndian.PutUint32(beBuf, size)
	n, err := w.Write(beBuf)
	if err != nil {
		return n, err
	}

	n, err = w.Write(data)
	// fmtLogf(Debug, "SendFramedResponse====,%d", n)
	return n + 4, err
}

func (p *Protocol) Exec(client *client, params [][]byte) ([]byte, error) {
	// fmtLogf(Debug, "param:%v", string(params[0]))
	if bytes.Equal(params[0], []byte("IDENTIFY")) {
		return p.IDENTIFY(client, params)
	}

	switch {
	case bytes.Equal(params[0], []byte("FIN")):
		return p.FIN(client, params)
	case bytes.Equal(params[0], []byte("RDY")):
		return p.RDY(client, params)
	case bytes.Equal(params[0], []byte("PUB")):
		return p.PUB(client, params)
	case bytes.Equal(params[0], []byte("SUB")):
		return p.SUB(client, params)
	}
	return nil, fmt.Errorf("unknown command - %v", params[0])
}

func (p *Protocol) SUB(client *client, params [][]byte) ([]byte, error) {
	if atomic.LoadInt32(&client.State) != stateInit {
		return nil, fmt.Errorf("cannot SUB in current state")
	}

	if len(params) < 3 {
		return nil, fmt.Errorf("SUB insufficient number of parameters")
	}

	topicName := string(params[1])
	if !IsValidTopicName(topicName) {
		return nil, fmt.Errorf(fmt.Sprintf("SUB topic name %q is not valid", topicName))
	}

	channelName := string(params[2])
	if !IsValidChannelName(channelName) {
		return nil, fmt.Errorf(fmt.Sprintf("SUB channel name %q is not valid", channelName))
	}

	var channel *Channel
	for i := 1; ; i++ {
		topic := p.ltqd.GetTopic(topicName)
		channel = topic.GetChannel(channelName)
		if err := channel.AddClient(client.ID, client); err != nil {
			return nil, fmt.Errorf("SUB failed " + err.Error())
		}

		if channel.Exiting() || topic.Exiting() {
			channel.RemoveClient(client.ID)
			if i < 2 {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			return nil, fmt.Errorf("SUB failed to deleted topic/channel")
		}
		break
	}
	atomic.StoreInt32(&client.State, stateSubscribed)
	// fmtLogf(Debug, "SUB ====> channel %s", channel.name)
	client.Channel = channel
	//更新messagepump
	client.SubEventChan <- channel

	return okBytes, nil
}

func (p *Protocol) RDY(client *client, params [][]byte) ([]byte, error) {
	state := atomic.LoadInt32(&client.State)

	if state == stateClosing {
		fmtLogf(Debug, "PROTOCOL: [%v] ignoring RDY after CLS in state ClientStateClosing",
			client)
		return nil, nil
	}

	if state != stateSubscribed {
		return nil, fmt.Errorf("cannot RDY in current state")
	}

	count := int64(1)
	if len(params) > 1 {
		// fmtLogf(Debug, "rdy param: %s", string(params[1]))
		tmp, _ := strconv.Atoi(string(params[1]))
		count = int64(tmp)
		// fmtLogf(Debug, "count: %d", count)
	}

	if count < 0 || count > p.ltqd.getOpts().MaxRdyCount {
		return nil, fmt.Errorf(fmt.Sprintf("RDY count %d out of range 0-%d", count, p.ltqd.getOpts().MaxRdyCount))
	}

	client.SetReadyCount(count)

	return nil, nil
}

func (p *Protocol) IDENTIFY(client *client, params [][]byte) ([]byte, error) {
	// fmtLogf(Debug, "start IDENTIFY")
	var err error

	if atomic.LoadInt32(&client.State) != stateInit {
		return nil, fmt.Errorf("cannot IDENTIFY in current state")
	}

	// fmtLogf(Debug, "IDENTIFY bodylen")
	bodyLen, err := readLen(client.Reader, client.lenSlice)
	if err != nil {
		return nil, fmt.Errorf("IDENTIFY failed to read body size")
	}
	// fmtLogf(Debug, "IDENTIFY len:%d", bodyLen)

	if int64(bodyLen) > p.ltqd.getOpts().MaxBodySize {
		return nil, fmt.Errorf(fmt.Sprintf("IDENTIFY body too big %d > %d", bodyLen, p.ltqd.getOpts().MaxBodySize))
	}

	if bodyLen <= 0 {
		return nil, fmt.Errorf(fmt.Sprintf("IDENTIFY invalid body size %d", bodyLen))
	}

	body := make([]byte, bodyLen)
	_, err = io.ReadFull(client.Reader, body)
	if err != nil {
		return nil, fmt.Errorf("IDENTIFY failed to read body")
	}

	var identifyData identifyData
	err = json.Unmarshal(body, &identifyData)
	if err != nil {
		return nil, fmt.Errorf("IDENTIFY failed to decode JSON body")
	}

	// fmtLogf(Debug, "PROTOCOL identify: [%v] %+v", client, identifyData)

	err = client.Identify(identifyData)
	if err != nil {
		return nil, fmt.Errorf("IDENTIFY " + err.Error())
	}

	resp, err := json.Marshal(struct {
		MaxRdyCount   int64 `json:"max_rdy_count"`
		MaxMsgTimeout int64 `json:"max_msg_timeout"`
		MsgTimeout    int64 `json:"msg_timeout"`
	}{
		MaxRdyCount:   p.ltqd.getOpts().MaxRdyCount,
		MaxMsgTimeout: int64(p.ltqd.getOpts().MaxMsgTimeout / time.Millisecond),
		MsgTimeout:    int64(client.MsgTimeout / time.Millisecond),
	})
	if err != nil {
		return nil, fmt.Errorf("IDENTIFY failed " + err.Error())
	}

	err = p.Send(client, resp)
	if err != nil {
		return nil, fmt.Errorf("IDENTIFY failed " + err.Error())
	}

	return nil, nil
}

func (p *Protocol) FIN(client *client, params [][]byte) ([]byte, error) {
	// fmtLogf(Debug, "FIN===>start")
	state := atomic.LoadInt32(&client.State)
	if state != stateSubscribed && state != stateClosing {
		return nil, fmt.Errorf("cannot FIN in current state")
	}

	if len(params) < 2 {
		return nil, fmt.Errorf("FIN insufficient number of params")
	}
	// fmtLogf(Debug, "FIN===>check param")

	id, err := getMessageID(params[1])
	if err != nil {
		return nil, fmt.Errorf("FIN failed " + err.Error())
	}
	// fmtLogf(Debug, "FIN===>id:%s", string(id[:]))

	err = client.Channel.FinishMessage(client.ID, *id)
	if err != nil {
		return nil, fmt.Errorf(fmt.Sprintf("FIN %v failed %v", *id, err.Error()))
	}

	// fmtLogf(Debug, "FIN===>FinishMessage")

	return nil, nil
}

func (p *Protocol) PUB(client *client, params [][]byte) ([]byte, error) {
	var err error

	if len(params) < 2 {
		return nil, fmt.Errorf("PUB insufficient number of parameters")
	}

	topicName := string(params[1])
	if !IsValidTopicName(topicName) {
		return nil, fmt.Errorf(fmt.Sprintf("PUB topic name %q is not valid", topicName))
	}

	bodyLen, err := readLen(client.Reader, client.lenSlice)
	if err != nil {
		return nil, fmt.Errorf("PUB failed to read message body size")
	}

	if bodyLen <= 0 {
		return nil, fmt.Errorf(fmt.Sprintf("PUB invalid message body size %d", bodyLen))
	}

	if int64(bodyLen) > p.ltqd.getOpts().MaxMsgSize {
		return nil, fmt.Errorf(fmt.Sprintf("PUB message too big %d > %d", bodyLen, p.ltqd.getOpts().MaxMsgSize))
	}

	messageBody := make([]byte, bodyLen)
	_, err = io.ReadFull(client.Reader, messageBody)
	if err != nil {
		return nil, fmt.Errorf("PUB failed to read message body")
	}

	topic := p.ltqd.GetTopic(topicName)
	msg := NewMessage(topic.GenerateID(), messageBody)
	err = topic.PutMessage(msg)
	if err != nil {
		return nil, fmt.Errorf("PUB failed " + err.Error())
	}

	return okBytes, nil
}

func IsValidTopicName(name string) bool {
	return isValidName(name)
}

func IsValidChannelName(name string) bool {
	return isValidName(name)
}

func isValidName(name string) bool {
	if len(name) > 64 || len(name) < 1 {
		return false
	}
	return validTopicChannelNameRegex.MatchString(name)
}

func getMessageID(p []byte) (*MessageID, error) {
	if len(p) != MsgIDLength {
		return nil, errors.New("invalid message ID")
	}
	return (*MessageID)(unsafe.Pointer(&p[0])), nil
}

func readLen(r io.Reader, tmp []byte) (int32, error) {
	_, err := io.ReadFull(r, tmp)
	if err != nil {
		return 0, err
	}
	return int32(binary.BigEndian.Uint32(tmp)), nil
}
