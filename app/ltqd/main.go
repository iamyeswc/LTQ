package main

import (
	"context"
	"fmt"
	"ltq/ltqd"
	"os"
	"sync"
	"syscall"

	"github.com/judwhite/go-svc"
)

type program struct {
	once sync.Once
	ltqd *ltqd.LTQD
}

func main() {
	prg := &program{}
	if err := svc.Run(prg, syscall.SIGINT, syscall.SIGTERM); err != nil {
		logFatal("%v", err)
	}
}

func (p *program) Init(env svc.Environment) error {
	opts := ltqd.NewOptions()
	ltqd, err := ltqd.New(opts)
	if err != nil {
		logFatal("failed to instantiate ltqd - %v", err)
	}
	p.ltqd = ltqd

	return nil
}

func (p *program) Start() error {
	err := p.ltqd.LoadMetadata()
	if err != nil {
		logFatal("failed to load metadata - %v", err)
	}
	err = p.ltqd.PersistMetadata()
	if err != nil {
		logFatal("failed to persist metadata - %v", err)
	}

	go func() {
		err := p.ltqd.Main()
		if err != nil {
			p.Stop()
			os.Exit(1)
		}
	}()

	return nil
}

func (p *program) Stop() error {
	p.once.Do(func() {
		p.ltqd.Exit()
	})
	return nil
}

func (p *program) Handle(s os.Signal) error {
	return svc.ErrStop
}

// Context returns a context that will be canceled when ltqd initiates the shutdown
func (p *program) Context() context.Context {
	return p.ltqd.Context()
}

func logFatal(f string, args ...interface{}) {
	fmt.Printf("[ltqd] ", f, args...)
	fmt.Println() // 打印换行
}
