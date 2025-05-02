package ltqd

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"time"
)

type LookupPeer struct {
	addr            string
	conn            net.Conn
	state           int32
	connectCallback func(*LookupPeer)
	maxBodySize     int64
	Info            peerInfo
}

type peerInfo struct {
	TCPPort  int    `json:"tcp_port"`
	HTTPPort int    `json:"http_port"`
	Version  string `json:"version"`
}

var byteSpace = []byte(" ")
var byteNewLine = []byte("\n")

func newLookupPeer(addr string, maxBodySize int64, connectCallback func(*LookupPeer)) *LookupPeer {
	return &LookupPeer{
		addr:            addr,
		state:           stateDisconnected,
		maxBodySize:     maxBodySize,
		connectCallback: connectCallback,
	}
}

func connectCallback(l *LTQD, hostname string) func(*LookupPeer) {
	//构建包含IDENTIFY 命令, 发送命令并接收 ltqlookupd 的响应, 如果响应无效，关闭连接并返回
	return func(lp *LookupPeer) {
		ci := make(map[string]interface{})
		ci["tcp_port"] = l.getOpts().BroadcastTCPPort
		ci["hostname"] = hostname

		cmd, err := Identify(ci)
		if err != nil {
			lp.Close()
			return
		}

		resp, err := lp.Command(cmd)
		if err != nil {
			fmtLogf(Debug, "LOOKUPD(%v): %v - %v", lp, cmd, err)
			return
		} else if bytes.Equal(resp, []byte("E_INVALID")) {
			fmtLogf(Debug, "LOOKUPD(%v): lookupd returned %v", lp, resp)
			lp.Close()
			return
		}

		//把返回内容解析到 peerInfo 结构体中
		err = json.Unmarshal(resp, &lp.Info)
		if err != nil {
			fmtLogf(Debug, "LOOKUPD(%v): parsing response - %v", lp, resp)
			lp.Close()
			return
		}
		fmtLogf(Debug, "LOOKUPD(%v): peer info %+v", lp, lp.Info)

		//构建注册命令, 发送命令并接收 ltqlookupd 的响应
		var commands []*Command
		l.RLock()
		for _, topic := range l.topics {
			topic.RLock()
			if len(topic.channels) == 0 {
				commands = append(commands, Register(topic.name, ""))
			} else {
				for _, channel := range topic.channels {
					commands = append(commands, Register(channel.topicName, channel.name))
				}
			}
			topic.RUnlock()
		}
		l.RUnlock()

		for _, cmd := range commands {
			fmtLogf(Debug, "LOOKUPD(%v): %v", lp, cmd)
			_, err := lp.Command(cmd)
			if err != nil {
				fmtLogf(Debug, "LOOKUPD(%v): %v - %v", lp, cmd, err)
				return
			}
		}
	}
}

func (lp *LookupPeer) Command(cmd *Command) ([]byte, error) {
	initialState := lp.state
	if lp.state != stateConnected {
		err := lp.Connect()
		if err != nil {
			return nil, err
		}
		lp.state = stateConnected
		if initialState == stateDisconnected {
			lp.connectCallback(lp)
		}
		if lp.state != stateConnected {
			return nil, fmt.Errorf("LookupPeer connectCallback() failed")
		}
	}
	if cmd == nil {
		return nil, nil
	}
	_, err := cmd.WriteTo(lp)
	if err != nil {
		lp.Close()
		return nil, err
	}
	resp, err := readResponseBounded(lp, lp.maxBodySize)
	if err != nil {
		lp.Close()
		return nil, err
	}
	return resp, nil
}

func (lp *LookupPeer) Close() error {
	lp.state = stateDisconnected
	if lp.conn != nil {
		return lp.conn.Close()
	}
	return nil
}

func (lp *LookupPeer) Connect() error {
	fmtLogf(Debug, "LOOKUP connecting to %v", lp.addr)
	conn, err := net.DialTimeout("tcp", lp.addr, time.Second)
	if err != nil {
		return err
	}
	lp.conn = conn
	return nil
}

func (lp *LookupPeer) Write(data []byte) (int, error) {
	lp.conn.SetWriteDeadline(time.Now().Add(time.Second))
	return lp.conn.Write(data)
}

func (lp *LookupPeer) Read(data []byte) (int, error) {
	lp.conn.SetReadDeadline(time.Now().Add(time.Second))
	return lp.conn.Read(data)
}

func readResponseBounded(r io.Reader, limit int64) ([]byte, error) {
	var msgSize int32

	// message size
	err := binary.Read(r, binary.BigEndian, &msgSize)
	if err != nil {
		return nil, err
	}

	if int64(msgSize) > limit {
		return nil, fmt.Errorf("response body size (%d) is greater than limit (%d)",
			msgSize, limit)
	}

	// message binary data
	buf := make([]byte, msgSize)
	_, err = io.ReadFull(r, buf)
	if err != nil {
		return nil, err
	}

	return buf, nil
}
