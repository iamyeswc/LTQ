package ltqd

import (
	"crypto/md5"
	"hash/crc32"
	"io"
	"os"
	"time"
)

type Options struct {
	// basic options
	ID int64 `flag:"node-id" cfg:"id"`

	TCPAddress       string `flag:"tcp-address"`
	BroadcastTCPPort int    `flag:"broadcast-tcp-port"`

	// diskqueue options
	DataPath        string        `flag:"data-path"`
	MemQueueSize    int64         `flag:"mem-queue-size"`
	MaxBytesPerFile int64         `flag:"max-bytes-per-file"`
	SyncEvery       int64         `flag:"sync-every"`
	SyncTimeout     time.Duration `flag:"sync-timeout"`

	// msg and command options
	MaxMsgSize int64         `flag:"max-msg-size"`
	MsgTimeout time.Duration `flag:"msg-timeout"`

	MaxRdyCount int64 `flag:"max-rdy-count"`

	QueueScanSelectionCount  int           `flag:"queue-scan-selection-count"`
	QueueScanInterval        time.Duration `flag:"queue-scan-interval"`
	QueueScanRefreshInterval time.Duration `flag:"queue-scan-refresh-interval"`
	QueueScanDirtyPercent    float64       `flag:"queue-scan-dirty-percent"`
	QueueScanWorkerPoolMax   int           `flag:"queue-scan-worker-pool-max"`

	LTQLookupdTCPAddresses []string      `flag:"lookupd-tcp-address" cfg:"ltqlookupd_tcp_addresses"`
	MaxBodySize            int64         `flag:"max-body-size"`
	MaxMsgTimeout          time.Duration `flag:"max-msg-timeout"`
}

func NewOptions() *Options {
	hostname, err := os.Hostname()
	if err != nil {
		fmtLogf(Debug, "os.Hostname() failed, err: %v", err)
	}

	h := md5.New()
	io.WriteString(h, hostname)
	defaultID := int64(crc32.ChecksumIEEE(h.Sum(nil)) % 1024)

	return &Options{
		ID: defaultID,

		TCPAddress:       "0.0.0.0:4150",
		BroadcastTCPPort: 0,

		MemQueueSize:    10000,
		MaxBytesPerFile: 100 * 1024 * 1024,
		SyncEvery:       2500,
		SyncTimeout:     2 * time.Second,

		MaxMsgSize: 1024 * 1024,
		MsgTimeout: 5 * time.Second,

		MaxRdyCount: 10000,

		QueueScanSelectionCount:  20,
		QueueScanInterval:        100 * time.Millisecond,
		QueueScanRefreshInterval: 5 * time.Second,
		QueueScanDirtyPercent:    0.25,
		QueueScanWorkerPoolMax:   4,

		LTQLookupdTCPAddresses: []string{"0.0.0.0:4160"},
		MaxBodySize:            5 * 1024 * 1024,
		MaxMsgTimeout:          2 * time.Minute,
	}
}
