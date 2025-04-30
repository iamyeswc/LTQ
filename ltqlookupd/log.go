package ltqlookupd

import (
	"fmt"
)

type LogLevel int

const (
	Info LogLevel = iota
	Debug
	Error
)

type AppLogFunc func(lvl LogLevel, f string, args ...interface{})

// 适配器函数
func fmtLogf(lvl LogLevel, f string, args ...interface{}) {
	// 这里可以根据 lvl 选择不同的打印方式或格式
	fmt.Printf("[ltqlookupd][%v] ", lvlToString(lvl))
	fmt.Printf(f, args...)
	fmt.Println() // 打印换行
}

// 将 LogLevel 转换为字符串的辅助函数
func lvlToString(lvl LogLevel) string {
	switch lvl {
	case Info:
		return "INFO"
	case Debug:
		return "DEBUG"
	case Error:
		return "ERROR"
	default:
		return "UNKNOWN"
	}
}
