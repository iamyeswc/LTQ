package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
)

type Producer struct {
	Hostname string `json:"hostname"`
	TCPPort  int    `json:"tcp_port"`
	HTTPPort int    `json:"http_port"`
}

type Response struct {
	Producers []Producer `json:"producers"`
}

func main() {
	// 启动生产者
	for i := 0; i < 10000; i++ {
		producer()
	}
	fmt.Println("All producers finished sending messages.")
}

func producer() {
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

	// 发送 PUB 请求
	err = sendPUB(address, topicName, "Hello, LTQD!")
	if err != nil {
		fmt.Printf("Failed to send PUB request: %v\n", err)
		return
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
