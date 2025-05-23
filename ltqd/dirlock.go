package ltqd

import (
	"fmt"
	"os"
	"syscall"
)

type DirLock struct {
	dir string
	f   *os.File
}

func NewDirLock(dir string) *DirLock {
	return &DirLock{
		dir: dir,
	}
}

func (l *DirLock) Lock() error {
	f, err := os.Open(l.dir)
	if err != nil {
		return err
	}
	l.f = f
	err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err != nil {
		return fmt.Errorf("cannot flock directory %v - %v (possibly in use by another instance of ltqd)", l.dir, err)
	}
	return nil
}

func (l *DirLock) Unlock() error {
	defer l.f.Close()
	return syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
}
