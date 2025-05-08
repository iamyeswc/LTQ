package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"
	"time"
)

type Producer struct {
	Hostname string `json:"hostname"`
	TCPPort  int    `json:"tcp_port"`
	HTTPPort int    `json:"http_port"`
}

type Response1 struct {
	Producers []Producer `json:"producers"`
}

type Response2 struct {
	Channels  []string   `json:"channels"`
	Producers []Producer `json:"producers"`
}

type identifyData struct {
	ClientID   string `json:"client_id"`
	Hostname   string `json:"hostname"`
	MsgTimeout int    `json:"msg_timeout"`
}

const (
	MsgIDLength       = 16
	minValidMsgLength = MsgIDLength + 8 + 2 // Timestamp + Attempts
)

type MessageID [MsgIDLength]byte

type Message struct {
	Timestamp int64
	Attempts  uint16
	Body      []byte
	ID        MessageID
}

// BenchmarkProducer 测试生产者性能
func BenchmarkProducer(b *testing.B) {
	ltqLookupAddress := "http://127.0.0.1:4161"
	topicName := "benchmark_topic"
	order := "true"
	url := fmt.Sprintf("%v/plookup?topic=%v&order=%v", ltqLookupAddress, topicName, order)

	// 发送 GET 请求
	resp, err := http.Get(url)
	if err != nil {
		fmt.Printf("Failed to send request: %v\n", err)
		return
	}
	defer resp.Body.Close()

	// 读取响应
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Printf("Failed to read response: %v\n", err)
		return
	}

	// 打印响应
	fmt.Printf("Response: %v\n", string(body))

	var response Response1
	err = json.Unmarshal(body, &response)
	if err != nil {
		fmt.Printf("Failed to parse JSON response: %v\n", err)
		return
	}

	// 打印 Producer 信息
	if len(response.Producers) == 0 {
		fmt.Println("No producers found for the topic")
		return
	}

	producer := response.Producers[0] // 选择第一个 Producer
	fmt.Printf("Using Producer: %+v\n", producer)

	// 构造 TCP 地址
	address := fmt.Sprintf("%v:%d", producer.Hostname, producer.TCPPort)

	// 初始化 TCP 连接
	conn, err := net.Dial("tcp", address)
	if err != nil {
		b.Fatalf("Failed to connect to LTQD: %v", err)
	}
	defer conn.Close()

	// 构造消息
	message := "Hello, Benchmark!"

	// 重置计时器
	b.ResetTimer()

	// 执行基准测试
	for i := 0; i < b.N; i++ {
		err := sendPUB(conn, topicName, fmt.Sprintf("%s #%d", message, i))
		if err != nil {
			b.Fatalf("Failed to send PUB request: %v", err)
		}
	}
}

// BenchmarkConsumer 测试消费者性能
func BenchmarkConsumer(b *testing.B) {
	ltqlookupdAddress := "http://127.0.0.1:4161"
	topicName := "benchmark_topic"
	channelName := "benchmark_channel"
	url := fmt.Sprintf("%v/clookup?topic=%v&channel=%v", ltqlookupdAddress, topicName, channelName)

	resp, err := http.Get(url)
	if err != nil {
		fmt.Printf("Failed to send request: %v\n", err)
		return
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Printf("Failed to read response: %v\n", err)
		return
	}

	var response Response2
	err = json.Unmarshal(body, &response)
	if err != nil {
		fmt.Printf("Failed to parse JSON response: %v\n", err)
		return
	}

	// 打印 Producer 信息
	if len(response.Producers) == 0 {
		fmt.Println("No producers found for the topic")
		return
	}
	producer := response.Producers[0] // 选择第一个 Producer
	fmt.Printf("Using Producer: %+v\n", producer)

	// 构造 TCP 地址
	address := fmt.Sprintf("%v:%d", producer.Hostname, producer.TCPPort)

	// 初始化 TCP 连接
	conn, err := net.Dial("tcp", address)
	if err != nil {
		b.Fatalf("Failed to connect to LTQD: %v", err)
	}
	defer conn.Close()

	// 发送 IDNETIFY 请求
	identify := identifyData{
		ClientID:   "benchmark_consumer",
		Hostname:   producer.Hostname,
		MsgTimeout: 60000, //1 minute
	}
	err = sendIDENTIFY(conn, identify)
	if err != nil {
		fmt.Printf("Failed to send IDNETIFY request: %v\n", err)
		return
	}

	// 发送 SUB 请求
	err = sendSUB(conn, topicName, channelName)
	if err != nil {
		fmt.Printf("Failed to send SUB request: %v\n", err)
		return
	}

	// 重置计时器
	b.ResetTimer()

	// 执行基准测试
	for i := 0; i < b.N; i++ {
		// 发送RDY命令
		num := 1
		err = sendRDY(conn, num)
		if err != nil {
			fmt.Printf("Failed to send RDY request: %v\n", err)
			return
		}

		// 读取数据
		for num > 0 {
			fmt.Println("num:", num)
			sizeBuf := make([]byte, 4)
			_, err := io.ReadFull(conn, sizeBuf)
			fmt.Println("len buf:", len(sizeBuf))
			if err != nil {
				fmt.Printf("Failed to read message size: %v\n", err)
				return
			}
			msgSize := binary.BigEndian.Uint32(sizeBuf)
			fmt.Println("len msgSize:", msgSize)

			// 读取完整消息内容
			msgBuf := make([]byte, msgSize)
			_, err = io.ReadFull(conn, msgBuf)
			fmt.Println("len msgSize3:", msgSize)
			if err != nil {
				fmt.Printf("Failed to read message body: %v\n", err)
				return
			}

			buf := bytes.NewReader(msgBuf)

			// 解析字段
			var timestamp int64
			var attempts uint16
			messageID := make([]byte, 16)

			binary.Read(buf, binary.BigEndian, &timestamp)
			binary.Read(buf, binary.BigEndian, &attempts)
			buf.Read(messageID)

			// 剩余部分是 body
			body := make([]byte, buf.Len())
			buf.Read(body)

			fmt.Printf(">>> Message received:\n")
			fmt.Printf("Timestamp: %d\n", timestamp)
			fmt.Printf("Attempts: %d\n", attempts)
			fmt.Printf("MessageID: %s\n", string(messageID))
			fmt.Printf("Body: %s\n", string(body))

			err = sendFIN(conn, messageID)
			if err != nil {
				fmt.Printf("Failed to send FIN request: %v\n", err)
			} else {
				num--
			}

		}
	}
}

// 发送 PUB 请求
func sendPUB(conn net.Conn, topic, message string) error {
	var err error
	// 构造 PUB 命令
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("PUB %v\n", topic)) // PUB 命令和 Topic 名称
	messageLength := uint32(len(message))
	lengthBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(lengthBytes, messageLength)
	buf.Write(lengthBytes)   // 消息长度
	buf.WriteString(message) // 消息内容

	// 发送命令
	_, err = conn.Write(buf.Bytes())
	if err != nil {
		return fmt.Errorf("failed to send PUB command: %v", err)
	}

	// 读取响应
	response := make([]byte, 1024)
	n, err := conn.Read(response)
	if err != nil {
		return fmt.Errorf("failed to read PUB response: %v", err)
	}
	fmt.Printf("PUB Response: %v\n", string(response[:n]))
	return nil
}

// 发送 IDNETIFY 请求
func sendIDENTIFY(conn net.Conn, data identifyData) error {
	fmt.Println("start=====sendIDENTIFY====")
	jsonData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("json marshal error: %w", err)
	}

	lengthBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lengthBuf, uint32(len(jsonData)))
	var buf bytes.Buffer
	buf.WriteString("IDENTIFY\n")
	buf.Write(lengthBuf)
	buf.Write(jsonData)

	command := buf.Bytes()

	_, err = conn.Write([]byte(command))
	if err != nil {
		return fmt.Errorf("failed to send length prefix: %w", err)
	}

	response := make([]byte, 1024)
	n, err := conn.Read(response)
	if err != nil {
		return fmt.Errorf("failed to read IDENTIFY response: %w", err)
	}
	fmt.Printf("IDENTIFY Response: %s\n", string(response[:n]))

	return nil
}

// 发送 SUB 请求
func sendSUB(conn net.Conn, topic, channel string) error {
	var err error
	// 构造 SUB 命令
	command := fmt.Sprintf("SUB %s %s\n", topic, channel)

	// 发送命令
	_, err = conn.Write([]byte(command))
	if err != nil {
		return fmt.Errorf("failed to send SUB command: %v", err)
	}

	// 读取响应
	response := make([]byte, 1024)
	n, err := conn.Read(response)
	if err != nil {
		return fmt.Errorf("failed to read SUB response: %v", err)
	}
	fmt.Printf("SUB Response: %v\n", string(response[:n]))
	return nil
}

// 发送RDY命令
func sendRDY(conn net.Conn, num int) error {
	var err error

	// 构造 RDY 命令
	command := fmt.Sprintf("RDY %d\n", num)

	// 发送命令
	_, err = conn.Write([]byte(command))
	if err != nil {
		return fmt.Errorf("failed to send RDY command: %v", err)
	}

	return nil
}

// 发送RDY命令
func sendFIN(conn net.Conn, id []byte) error {
	var err error

	// 构造 FIN 命令
	command := fmt.Sprintf("FIN %s\n", string(id))

	// 发送命令
	_, err = conn.Write([]byte(command))
	if err != nil {
		return fmt.Errorf("failed to send FIN command: %v", err)
	}

	// 读取响应
	response := make([]byte, 1024)
	conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	_, err = conn.Read(response)
	if err != nil {
		if ne, ok := err.(net.Error); ok && ne.Timeout() {
			// 超时但不是错误，说明没返回内容，正常
			return nil
		}
		return fmt.Errorf("read error: %v", err)
	}
	// fmt.Printf("Response: %s\n", string(response[:n]))
	return nil
}
