package siserlogger

import (
	"path/filepath"
	"sync"
	"time"

	"github.com/kjk/common/filerotate"
	"github.com/kjk/common/siser"
)

type File struct {
	siser   *siser.Writer
	file    *filerotate.File
	name    string
	RecName string
	mu      sync.Mutex
	dir     string
}

func NewDaily(dir string, name string, didRotateFn func(path string)) (*File, error) {
	absDir, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	res := &File{
		dir:     absDir,
		name:    name,
		RecName: name,
	}

	didRotateInternal := func(path string, didRotate bool) {
		if didRotate && didRotateFn != nil {
			didRotateFn(path)
		}
	}

	res.file, err = filerotate.NewDaily(absDir, name, didRotateInternal)
	if err != nil {
		return nil, err
	}
	res.siser = siser.NewWriter(res.file)
	return res, nil
}

func (l *File) Write(d []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.siser == nil {
		return nil
	}
	_, err := l.siser.Write(d, time.Now(), l.RecName)
	return err
}
