package ltqlookupd

import (
	"os"
	"time"
)

type Options struct {
	TCPAddress  string `flag:"tcp-address"`
	HTTPAddress string `flag:"http-address"`

	InactiveProducerTimeout time.Duration `flag:"inactive-producer-timeout"`
}

func NewOptions() *Options {
	hostname, err := os.Hostname()
	if err != nil {
		fmtLogf(Debug, "os.Hostname() failed, err: %v", err)
	}
	fmtLogf(Debug, "hostname is: %v", hostname)

	return &Options{
		TCPAddress:  "0.0.0.0:4160",
		HTTPAddress: "0.0.0.0:4161",

		InactiveProducerTimeout: 300 * time.Second,
	}
}
