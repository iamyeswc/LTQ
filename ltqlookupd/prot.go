package ltqlookupd

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"regexp"
	"strings"
	"sync/atomic"
	"time"
)

var separatorBytes = []byte(" ")
var okBytes = []byte("OK")

var validTopicChannelNameRegex = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

type Client interface {
	Close() error
}

type Protocol struct {
	ltqlookupd *LTQLOOKUPD
}

func (p *Protocol) NewClient(conn net.Conn) Client {
	return newClient(conn)
}

func (p *Protocol) IOLoop(c Client) error {
	var err error
	var line string

	client := c.(*client)

	reader := bufio.NewReader(client)
	for {
		line, err = reader.ReadString('\n')
		if err != nil {
			break
		}

		line = strings.TrimSpace(line)
		params := strings.Split(line, " ")

		fmtLogf(Debug, "PROTOCOL: [%v] - %v", client, params)

		var response []byte
		response, err = p.Exec(client, reader, params)
		if err != nil {
			fmtLogf(Debug, "[%v] - err:%v", client, err)

			_, sendErr := p.SendResponse(client, []byte(err.Error()))
			if sendErr != nil {
				fmtLogf(Debug, "[%v] - err:%v", client, sendErr)
				break
			}
			continue
		}

		if response != nil {
			_, err = p.SendResponse(client, response)
			if err != nil {
				err = fmt.Errorf("failed to send response - %v", err)
				break
			}
		}
	}

	fmtLogf(Debug, "PROTOCOL: [%v] exiting ioloop", client)

	//断开连接的时候删除所有的client
	if client.peerInfo != nil {
		registrations := p.ltqlookupd.DB.LookupRegistrations(client.peerInfo.id)
		for _, r := range registrations {
			if removed, _ := p.ltqlookupd.DB.RemoveProducer(r, client.peerInfo.id); removed {
				fmtLogf(Debug, "DB: client(%v) UNREGISTER category:%v key:%v subkey:%v", client, r.Category, r.Key, r.SubKey)
			}
		}
	}
	return err
}

func (p *Protocol) Exec(client *client, reader *bufio.Reader, params []string) ([]byte, error) {
	switch params[0] {
	case "PING":
		return p.PING(client, params)
	case "IDENTIFY":
		return p.IDENTIFY(client, reader, params[1:])
	case "REGISTER":
		return p.REGISTER(client, reader, params[1:])
	case "UNREGISTER":
		return p.UNREGISTER(client, reader, params[1:])
	}
	return nil, fmt.Errorf("unknown command - %v", params[0])
}

func (p *Protocol) PING(client *client, params []string) ([]byte, error) {
	if client.peerInfo != nil {
		//更新上一次的时间
		cur := time.Unix(0, atomic.LoadInt64(&client.peerInfo.lastUpdate))
		now := time.Now()
		fmtLogf(Debug, "CLIENT(%v): pinged (last ping %v)", client.peerInfo.id, now.Sub(cur))
		atomic.StoreInt64(&client.peerInfo.lastUpdate, now.UnixNano())
	}
	return []byte("OK"), nil
}

func (p *Protocol) REGISTER(client *client, reader *bufio.Reader, params []string) ([]byte, error) {
	//注册topic和channel
	if client.peerInfo == nil {
		return nil, fmt.Errorf("client must IDENTIFY")
	}

	topic, channel, err := getTopicChan("REGISTER", params)
	if err != nil {
		return nil, err
	}

	if channel != "" {
		key := Registration{"channel", topic, channel}
		if p.ltqlookupd.DB.AddProducer(key, &Producer{peerInfo: client.peerInfo}) {
			fmtLogf(Debug, "DB: client(%v) REGISTER category:%v key:%v subkey:%v", client, "channel", topic, channel)
		}
	}
	key := Registration{"topic", topic, ""}
	if p.ltqlookupd.DB.AddProducer(key, &Producer{peerInfo: client.peerInfo}) {
		fmtLogf(Debug, "DB: client(%v) REGISTER category:%v key:%v subkey:%v", client, "topic", topic, "")
	}

	return []byte("OK"), nil
}

func (p *Protocol) UNREGISTER(client *client, reader *bufio.Reader, params []string) ([]byte, error) {
	if client.peerInfo == nil {
		return nil, fmt.Errorf("client must IDENTIFY")
	}

	topic, channel, err := getTopicChan("UNREGISTER", params)
	if err != nil {
		return nil, err
	}

	if channel != "" {
		key := Registration{"channel", topic, channel}
		removed, _ := p.ltqlookupd.DB.RemoveProducer(key, client.peerInfo.id)
		if removed {
			fmtLogf(Debug, "DB: client(%v) UNREGISTER category:%v key:%v subkey:%v", client, "channel", topic, channel)
		}
	} else {
		registrations := p.ltqlookupd.DB.FindRegistrations("channel", topic, "*")
		for _, r := range registrations {
			removed, _ := p.ltqlookupd.DB.RemoveProducer(r, client.peerInfo.id)
			if removed {
				fmtLogf(Debug, "client(%v) unexpected UNREGISTER category:%v key:%v subkey:%v", client, "channel", topic, r.SubKey)
			}
		}

		key := Registration{"topic", topic, ""}
		removed, _ := p.ltqlookupd.DB.RemoveProducer(key, client.peerInfo.id)
		if removed {
			fmtLogf(Debug, "DB: client(%v) UNREGISTER category:%v key:%v subkey:%v", client, "topic", topic, "")
		}
	}

	return []byte("OK"), nil
}

func getTopicChan(command string, params []string) (string, string, error) {
	if len(params) == 0 {
		return "", "", fmt.Errorf(fmt.Sprintf("%v insufficient number of params", command))
	}

	topicName := params[0]
	var channelName string
	if len(params) >= 2 {
		channelName = params[1]
	}

	if !IsValidTopicName(topicName) {
		return "", "", fmt.Errorf(fmt.Sprintf("%v topic name '%v' is not valid", command, topicName))
	}

	if channelName != "" && !IsValidChannelName(channelName) {
		return "", "", fmt.Errorf(fmt.Sprintf("%v channel name '%v' is not valid", command, channelName))
	}

	return topicName, channelName, nil
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

func (p *Protocol) IDENTIFY(client *client, reader *bufio.Reader, params []string) ([]byte, error) {
	var err error

	if client.peerInfo != nil {
		return nil, fmt.Errorf("cannot IDENTIFY again")
	}

	var bodyLen int32
	err = binary.Read(reader, binary.BigEndian, &bodyLen)
	if err != nil {
		return nil, fmt.Errorf("IDENTIFY failed to read body size")
	}

	body := make([]byte, bodyLen)
	_, err = io.ReadFull(reader, body)
	if err != nil {
		return nil, fmt.Errorf("IDENTIFY failed to read body")
	}

	// body is a json structure with producer information
	peerInfo := PeerInfo{id: client.RemoteAddr().String()}
	err = json.Unmarshal(body, &peerInfo)
	if err != nil {
		return nil, fmt.Errorf("IDENTIFY failed to decode JSON body")
	}
	// require all fields
	if peerInfo.TCPPort == 0 {
		return nil, fmt.Errorf("IDENTIFY missing fields")
	}

	atomic.StoreInt64(&peerInfo.lastUpdate, time.Now().UnixNano())

	fmtLogf(Debug, "CLIENT(%v): TCP:%d", client, peerInfo.TCPPort)

	client.peerInfo = &peerInfo
	if p.ltqlookupd.DB.AddProducer(Registration{"client", "", ""}, &Producer{peerInfo: client.peerInfo}) {
		fmtLogf(Debug, "DB: client(%v) REGISTER category:%v key:%v subkey:%v", client, "client", "", "")
	}

	// build a response
	data := make(map[string]interface{})
	data["tcp_port"] = p.ltqlookupd.RealTCPAddr().Port
	data["http_port"] = p.ltqlookupd.RealHTTPAddr().Port
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("ERROR: unable to get hostname %v", err)
	}
	data["hostname"] = hostname

	response, err := json.Marshal(data)
	if err != nil {
		fmtLogf(Debug, "marshaling %v", data)
		return []byte("OK"), nil
	}
	return response, nil
}

func (p *Protocol) SendResponse(w io.Writer, data []byte) (int, error) {
	err := binary.Write(w, binary.BigEndian, int32(len(data)))
	if err != nil {
		return 0, err
	}

	n, err := w.Write(data)
	if err != nil {
		return 0, err
	}

	return (n + 4), nil
}
