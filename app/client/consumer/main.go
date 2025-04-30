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

//和ltqlookupd进行http交互, 拿到ltqd的地址
//和ltqd进行tcp交互

// Response 和 Producer 结构体定义
type Response struct {
	Channels  []string   `json:"channels"`
	Producers []Producer `json:"producers"`
}

type Producer struct {
	RemoteAddress    string `json:"remote_address"`
	Hostname         string `json:"hostname"`
	BroadcastAddress string `json:"broadcast_address"`
	TCPPort          int    `json:"tcp_port"`
	HTTPPort         int    `json:"http_port"`
}

func main() {
	// 从 ltqlookupd 获取 Producer 信息
	ltqlookupdAddress := "http://127.0.0.1:4161" // 替换为实际的 ltqlookupd 地址
	topicName := "example_topic"                 // 替换为实际的 topic 名称
	url := fmt.Sprintf("%s/clookup?topic=%s", ltqlookupdAddress, topicName)

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
	address := fmt.Sprintf("%s:%d", producer.Hostname, producer.TCPPort)

	// 发送 SUB 请求
	channelName := "example_channel" // 替换为实际的 Channel 名称
	err = sendSUB(address, topicName, channelName)
	if err != nil {
		fmt.Printf("Failed to send SUB request: %v\n", err)
		return
	}
}

// 发送 SUB 请求
func sendSUB(address, topic, channel string) error {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return fmt.Errorf("failed to connect to LTQD: %v", err)
	}
	defer conn.Close()

	// 构造 SUB 命令
	subCommand := fmt.Sprintf("SUB %s %s\n", topic, channel)

	// 发送命令
	_, err = conn.Write([]byte(subCommand))
	if err != nil {
		return fmt.Errorf("failed to send SUB command: %v", err)
	}

	// 读取响应
	response := make([]byte, 1024)
	n, err := conn.Read(response)
	if err != nil {
		return fmt.Errorf("failed to read SUB response: %v", err)
	}
	fmt.Printf("SUB Response: %s\n", string(response[:n]))
	return nil
}
