package siserlogger

import (
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/kjk/common/filerotate"
	"github.com/kjk/common/siser"
	"github.com/kjk/common/u"
)

type File struct {
	// name of the record written to siser log
	RecName string

	siser          *siser.Writer
	file           *filerotate.File
	fileNameSuffix string
	mu             sync.Mutex
	dir            string
}

func NewDaily(dir string, fileNameSuffix string, didRotateFn func(path string)) (*File, error) {
	absDir, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	res := &File{
		dir:            absDir,
		fileNameSuffix: fileNameSuffix,
		RecName:        fileNameSuffix,
	}

	didRotateInternal := func(path string, didRotate bool) {
		if !didRotate {
			return
		}
		dst := path + ".br"
		err := u.BrCompressFileDefault(dst, path)
		if err == nil {
			os.Remove(path)
			path = dst
		}
		if didRotateFn != nil {
			didRotateFn(path)
		}
	}

	res.file, err = filerotate.NewDaily(absDir, fileNameSuffix, didRotateInternal)
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
		err = f.file.Sync()
	}
	return err
}

func (f *File) Close() error {
	err := f.file.Close()
	f.siser = nil
	f.file = nil
	return err
}
