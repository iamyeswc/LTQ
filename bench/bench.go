package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"time"
)

type Producer struct {
	Hostname string `json:"hostname"`
	TCPPort  int    `json:"tcp_port"`
	HTTPPort int    `json:"http_port"`
}

type Responsep struct {
	Producers []Producer `json:"producers"`
}

type Response struct {
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

const (
	ltqlookupdAddress = "http://127.0.0.1:4161"
	topicName         = "exampletopic"
	messageCount      = 1000
	numProducers      = 10
	numConsumers      = 10
)

func main() {
	var wg sync.WaitGroup
	startTime := time.Now()

	// 启动消费者
	for i := 0; i < numConsumers; i++ {
		wg.Add(1)
		go func(cusumerID int) {
			defer wg.Done()
			consumer(fmt.Sprintf("consumer-%d", cusumerID))
		}(i)
	}

	// 启动生产者
	for i := 0; i < numProducers; i++ {
		wg.Add(1)
		go func(producerID int) {
			defer wg.Done()
			producer(producerID)
		}(i)
	}

	wg.Wait()
	duration := time.Since(startTime)
	fmt.Printf("Total time taken: %v\n", duration)
	fmt.Printf("Throughput: %.2f messages/sec\n", float64(numProducers*messageCount)/duration.Seconds())
}

func producer(producerID int) {
	// 构造请求 URL
	ltqlookupdAddress := "http://127.0.0.1:4161"
	topicName := "exampletopic"
	order := "true"
	url := fmt.Sprintf("%v/plookup?topic=%v&order=%v", ltqlookupdAddress, topicName, order)

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

	var response Responsep
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

	// 发送 PUB 请求
	for i := 0; i < messageCount; i++ {
		message := fmt.Sprintf("Message from producer %d: %d", producerID, i)
		err := sendPUB(address, topicName, message)
		if err != nil {
			fmt.Printf("Producer %d failed to send message %d: %v\n", producerID, i, err)
		}
	}
}

// 发送 PUB 请求
func sendPUB(address, topic, message string) error {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return fmt.Errorf("failed to connect to LTQD: %v", err)
	}
	defer conn.Close()

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

func consumer(cusumerid string) {
	// 从 ltqlookupd 获取 Producer 信息
	ltqlookupdAddress := "http://127.0.0.1:4161"
	topicName := "exampletopic"
	channelName := "examplechannel"
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

	var response Response
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

	conn, err := net.Dial("tcp", address)
	if err != nil {
		fmt.Println("failed to connect to LTQD:", err)
		return
	}
	defer conn.Close()

	// 发送 IDNETIFY 请求
	identify := identifyData{
		ClientID:   cusumerid,
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

	fmt.Println("consumer end....")

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
