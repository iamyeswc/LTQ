package ltqd

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"regexp"
	"sync/atomic"
	"time"
	"unsafe"
)

var separatorBytes = []byte(" ")
var okBytes = []byte("OK")

var validTopicChannelNameRegex = regexp.MustCompile(`^[.a-zA-Z0-9_-]?$`)

const (
	frameTypeResponse int32 = 0
	frameTypeError    int32 = 1
	frameTypeMessage  int32 = 2
)

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

		fmtLogf(Debug, "PROTOCOL: [%v] - %v", client, params)

		var response []byte
		response, err = p.Exec(client, params)
		if err != nil {
			fmtLogf(Debug, "[%v] - err:%v", client, err)

			sendErr := p.Send(client, frameTypeError, []byte(err.Error()))
			if sendErr != nil {
				fmtLogf(Debug, "[%v] - err:%v", client, sendErr)
				break
			}
			continue
		}

		if response != nil {
			err = p.Send(client, frameTypeResponse, response)
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
	msgTimeout := client.MsgTimeout

	close(startedChan)

	for {
		var b []byte
		var msg *Message

		// we're buffered (if there isn't any more data we should flush)...
		// select on the flusher ticker channel, too
		memoryMsgChan = subChannel.memoryMsgChan
		backendMsgChan = subChannel.backendMsgChan.ReadChan()

		select {
		case subChannel = <-subEventChan:
			// you can't SUB anymore
			subEventChan = nil
		case b = <-backendMsgChan:
			// decodeMessage then handle msg
		case msg = <-memoryMsgChan:
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
		}

	}

}

func (p *Protocol) SendMessage(client *client, msg *Message) error {
	fmtLogf(Debug, "PROTOCOL: writing msg(%v) to client(%v) - %v", msg.ID, client, msg.Body)

	buf := bufferPoolGet()
	defer bufferPoolPut(buf)

	_, err := msg.WriteTo(buf)
	if err != nil {
		return err
	}

	err = p.Send(client, frameTypeMessage, buf.Bytes())
	if err != nil {
		return err
	}

	return nil
}

func (p *Protocol) Send(client *client, frameType int32, data []byte) error {
	client.writeLock.Lock()

	_, err := SendFramedResponse(client.Writer, frameType, data)
	if err != nil {
		client.writeLock.Unlock()
		return err
	}

	if frameType != frameTypeMessage {
		err = client.Flush()
	}

	client.writeLock.Unlock()

	return err
}

func SendFramedResponse(w io.Writer, frameType int32, data []byte) (int, error) {
	beBuf := make([]byte, 4)
	size := uint32(len(data)) + 4

	binary.BigEndian.PutUint32(beBuf, size)
	n, err := w.Write(beBuf)
	if err != nil {
		return n, err
	}

	binary.BigEndian.PutUint32(beBuf, uint32(frameType))
	n, err = w.Write(beBuf)
	if err != nil {
		return n + 4, err
	}

	n, err = w.Write(data)
	return n + 8, err
}

func (p *Protocol) Exec(client *client, params [][]byte) ([]byte, error) {
	switch {
	case bytes.Equal(params[0], []byte("FIN")):
		return p.FIN(client, params)
	// case bytes.Equal(params[0], []byte("RDY")):
	// 	return p.RDY(client, params)
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
	client.Channel = channel
	//更新messagepump
	client.SubEventChan <- channel

	return okBytes, nil
}

func (p *Protocol) RDY(client *client, params [][]byte) ([]byte, error) {
	state := atomic.LoadInt32(&client.State)

	if state == stateClosing {
		// just ignore ready changes on a closing channel
		fmtLogf(Debug, "PROTOCOL: [%v] ignoring RDY after CLS in state ClientStateV2Closing",
			client)
		return nil, nil
	}

	if state != stateSubscribed {
		return nil, fmt.Errorf("cannot RDY in current state")
	}

	count := int64(1)
	if len(params) > 1 {
		b10, err := ByteToBase10(params[1])
		if err != nil {
			return nil, fmt.Errorf(fmt.Sprintf("RDY could not parse count %v", params[1]))
		}
		count = int64(b10)
	}

	if count < 0 || count > p.ltqd.getOpts().MaxRdyCount {
		return nil, fmt.Errorf(fmt.Sprintf("RDY count %d out of range 0-%d", count, p.ltqd.getOpts().MaxRdyCount))
	}

	return nil, nil
}

func (p *Protocol) FIN(client *client, params [][]byte) ([]byte, error) {
	state := atomic.LoadInt32(&client.State)
	if state != stateSubscribed && state != stateClosing {
		return nil, fmt.Errorf("cannot FIN in current state")
	}

	if len(params) < 2 {
		return nil, fmt.Errorf("FIN insufficient number of params")
	}

	id, err := getMessageID(params[1])
	if err != nil {
		return nil, fmt.Errorf("FIN failed " + err.Error())
	}

	err = client.Channel.FinishMessage(client.ID, *id)
	if err != nil {
		return nil, fmt.Errorf(fmt.Sprintf("FIN %v failed %v", *id, err.Error()))
	}

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

func ByteToBase10(b []byte) (n uint64, err error) {
	base := uint64(10)

	n = 0
	for i := 0; i < len(b); i++ {
		var v byte
		d := b[i]
		switch {
		case '0' <= d && d <= '9':
			v = d - '0'
		default:
			n = 0
			err = errors.New("failed to convert to Base10")
			return
		}
		n *= base
		n += uint64(v)
	}

	return n, err
}
