package siserlogger

import (
	"path/filepath"
	"sync"
	"time"

	"github.com/kjk/common/filerotate"
	"github.com/kjk/common/siser"
)

type File struct {
	// name of the record written to siser log
	RecName string

	siser *siser.Writer
	file  *filerotate.File
	name  string
	mu    sync.Mutex
	dir   string
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

func (f *File) Write(d []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.siser == nil {
		return nil
	}
	_, err := f.siser.Write(d, time.Now(), f.RecName)
	if err == nil {
		err = f.file.Flush()
	}
	return err
}

func (f *File) Close() error {
	err := f.file.Close()
	f.siser = nil
	f.file = nil
	return err
}
