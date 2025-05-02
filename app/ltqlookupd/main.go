package main

import (
	"fmt"
	"ltq/ltqlookupd"
	"os"
	"sync"
	"syscall"

	"github.com/judwhite/go-svc"
)

type program struct {
	once       sync.Once
	ltqlookupd *ltqlookupd.LTQLOOKUPD
}

func main() {
	prg := &program{}
	if err := svc.Run(prg, syscall.SIGINT, syscall.SIGTERM); err != nil {
		logFatal("%v", err)
	}
}

func (p *program) Init(env svc.Environment) error {
	return nil
}

func (p *program) Start() error {
	opts := ltqlookupd.NewOptions()

	ltqlookupd, err := ltqlookupd.New(opts)
	if err != nil {
		logFatal("failed to instantiate ltqlookupd - %v", err)
	}
	p.ltqlookupd = ltqlookupd

	go func() {
		err := p.ltqlookupd.Main()
		if err != nil {
			p.Stop()
			os.Exit(1)
		}
	}()

	return nil
}

func (p *program) Stop() error {
	p.once.Do(func() {
		p.ltqlookupd.Exit()
	})
	return nil
}

func logDebug(f string, args ...interface{}) {
	fmt.Printf("[ltqlookupd]: ")
	fmt.Printf(f, args...)
	fmt.Println()
}

func logFatal(f string, args ...interface{}) {
	fmt.Printf("[ltqlookupd]: ")
	fmt.Printf(f, args...)
	fmt.Println()
	os.Exit(1)
}
