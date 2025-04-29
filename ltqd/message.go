package ltqd

import (
	"encoding/binary"
	"fmt"
	"time"
)

const (
	MsgIDLength       = 16
	minValidMsgLength = MsgIDLength + 8 + 2 // Timestamp + Attempts
)

type Message struct {
	Timestamp int64
	Attempts  uint16
	Body      []byte
	ID        [MsgIDLength]byte
}

func (m *Message) NewMessage(id [MsgIDLength]byte, body []byte) *Message {
	timestamp := time.Now().UnixNano()
	return &Message{
		Timestamp: timestamp,
		Attempts:  uint16(0),
		ID:        id,
		Body:      body,
	}
}

// decodeMessage 反序列化数据（作为 []byte）并创建一个新的 Message
//
//	[x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x]...
//	|       (int64)        ||    ||      (以 ASCII 编码的十六进制字符串)           || (二进制)
//	|       8 字节         ||    ||                 16 字节                        || N 字节
//	------------------------------------------------------------------------------------------...
//	  纳秒时间戳           ^^                   消息 ID                       消息体
//	                       (uint16)
//	                        2 字节
//	                       尝试次数

func decodeMessage(b []byte) (*Message, error) {
	var msg Message

	if len(b) < minValidMsgLength {
		return nil, fmt.Errorf("invalid message buffer size (%d)", len(b))
	}

	msg.Timestamp = int64(binary.BigEndian.Uint64(b[:8]))
	msg.Attempts = binary.BigEndian.Uint16(b[8:10])
	copy(msg.ID[:], b[10:10+MsgIDLength])
	msg.Body = b[10+MsgIDLength:]

	return &msg, nil
}

func writeMessageToBackend(msg *Message, bq BackendQueue) error {
	buf := bufferPoolGet()
	defer bufferPoolPut(buf)
	_, err := msg.WriteTo(buf)
	if err != nil {
		return err
	}
	return bq.Put(buf.Bytes())
}
