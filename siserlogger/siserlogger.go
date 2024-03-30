package siserlogger

import (
	"path/filepath"
	"sync"
	"time"

	"github.com/kjk/common/filerotate"
	"github.com/kjk/common/siser"
)

type Logger struct {
	siser   *siser.Writer
	file    *filerotate.File
	name    string
	RecName string
	mu      sync.Mutex
	dir     string
}

func NewDaily(dir string, name string, didRotateFn func(path string)) (*Logger, error) {
	absDir, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	res := &Logger{
		dir:     absDir,
		name:    name,
		RecName: name,
	}

	didRotateInternal := func(path string, didRotate bool) {
		if didRotate && didRotateFn != nil {
			didRotateFn(path)
		}
	}

	res.file, err = filerotate.NewDaily(absDir, "slog-"+name+"-", didRotateInternal)
	if err != nil {
		return nil, err
	}
	res.siser = siser.NewWriter(res.file)
	return res, nil
}

func (l *Logger) Write(d []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.siser == nil {
		return nil
	}
	_, err := l.siser.Write(d, time.Now(), l.RecName)
	return err
}
