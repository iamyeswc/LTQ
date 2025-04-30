package main

import (
	"fmt"
	"io"
	"net/http"
)

//和ltqlookupd进行http交互, 拿到ltqd的地址
//和ltqd进行tcp交互

type Producer struct {
	RemoteAddress    string `json:"remote_address"`
	Hostname         string `json:"hostname"`
	BroadcastAddress string `json:"broadcast_address"`
	TCPPort          int    `json:"tcp_port"`
	HTTPPort         int    `json:"http_port"`
}

func main() {
	// 构造请求 URL
	ltqlookupdAddress := "http://127.0.0.1:4161" // 替换为实际的 ltqlookupd 地址
	topicName := "example_topic"                 // 替换为实际的 topic 名称
	fifo := "true"                               // 替换为实际的 FIFO 参数（true 或 false）
	url := fmt.Sprintf("%s/plookup?topic=%s&fifo=%s", ltqlookupdAddress, topicName, fifo)

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
	fmt.Printf("Response: %s\n", string(body))
}
