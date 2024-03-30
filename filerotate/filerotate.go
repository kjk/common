package filerotate

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Config struct {
	DidClose           func(path string, didRotate bool)
	PathIfShouldRotate func(creationTime time.Time, now time.Time) string
}

type File struct {
	sync.Mutex

	// Path is the path of the current file
	Path string

	creationTime time.Time

	//Location *time.Location

	config Config
	file   *os.File

	// position in the file of last Write or Write2, exposed for tests
	lastWritePos int64
}

func IsSameDay(t1, t2 time.Time) bool {
	return t1.YearDay() == t2.YearDay()
}

func IsSameHour(t1, t2 time.Time) bool {
	return t1.YearDay() == t2.YearDay() && t1.Hour() == t2.Hour()
}

func New(config *Config) (*File, error) {
	if nil == config {
		return nil, fmt.Errorf("must provide config")
	}
	if config.PathIfShouldRotate == nil {
		return nil, fmt.Errorf("must provide config.ShouldRotate")
	}
	file := &File{
		config: *config,
	}
	err := file.reopenIfNeeded()
	if err != nil {
		return nil, err
	}
	return file, nil
}

func MakeDailyRotateInDir(dir string, prefix string) func(time.Time, time.Time) string {
	return func(creationTime time.Time, now time.Time) string {
		if IsSameDay(creationTime, now) {
			return ""
		}
		name := now.Format("2006-01-02") + ".txt"
		if prefix != "" {
			name = prefix + name
		}
		return filepath.Join(dir, name)
	}
}

func MakeHourlyRotateInDir(dir string, prefix string) func(time.Time, time.Time) string {
	return func(creationTime time.Time, now time.Time) string {
		if IsSameHour(creationTime, now) {
			return ""
		}
		name := now.Format("2006-01-02_15") + ".txt"
		if prefix != "" {
			name = prefix + name
		}
		return filepath.Join(dir, name)
	}
}

// NewDaily creates a new file, rotating daily in a given directory
func NewDaily(dir string, prefix string, didClose func(path string, didRotate bool)) (*File, error) {
	daily := MakeDailyRotateInDir(dir, prefix)
	config := Config{
		DidClose:           didClose,
		PathIfShouldRotate: daily,
	}
	return New(&config)
}

// NewHourly creates a new file, rotating hourly in a given directory
func NewHourly(dir string, prefix string, didClose func(path string, didRotate bool)) (*File, error) {
	hourly := MakeHourlyRotateInDir(dir, prefix)
	config := Config{
		DidClose:           didClose,
		PathIfShouldRotate: hourly,
	}
	return New(&config)
}

func (f *File) close(didRotate bool) error {
	if f.file == nil {
		return nil
	}
	err := f.file.Close()
	f.file = nil
	if err == nil && f.config.DidClose != nil {
		f.config.DidClose(f.Path, didRotate)
	}
	return err
}

/*
func nowInMaybeLocation(loc *time.Location) time.Time {
	now := time.Now()
	if loc != nil {
		now = now.In(loc)
	}
	return now
}
*/

func (f *File) open(path string) error {
	f.Path = path
	f.creationTime = time.Now()
	// we can't assume that the dir for the file already exists
	dir := filepath.Dir(f.Path)
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return err
	}

	// would be easier to open with os.O_APPEND but Seek() doesn't work in that case
	flag := os.O_CREATE | os.O_WRONLY
	f.file, err = os.OpenFile(f.Path, flag, 0644)
	if err != nil {
		return err
	}
	_, err = f.file.Seek(0, io.SeekEnd)
	return err
}

func (f *File) reopenIfNeeded() error {
	now := time.Now()
	newPath := f.config.PathIfShouldRotate(f.creationTime, now)
	if newPath == "" {
		return nil
	}
	err := f.close(true)
	if err != nil {
		return err
	}
	return f.open(newPath)
}

func (f *File) write(d []byte, flush bool) (int64, int, error) {
	err := f.reopenIfNeeded()
	if err != nil {
		return 0, 0, err
	}
	f.lastWritePos, err = f.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, 0, err
	}
	n, err := f.file.Write(d)
	if err != nil {
		return 0, n, err
	}
	if flush {
		err = f.file.Sync()
	}
	return f.lastWritePos, n, err
}

// Write writes data to a file
func (f *File) Write(d []byte) (int, error) {
	f.Lock()
	defer f.Unlock()

	_, n, err := f.write(d, false)
	return n, err
}

// Write2 writes data to a file, optionally flushes. To enable users to later
// seek to where the data was written, it returns name of the file where data
// was written, offset at which the data was written, number of bytes and error
func (f *File) Write2(d []byte, flush bool) (string, int64, int, error) {
	f.Lock()
	defer f.Unlock()

	writtenAtPos, n, err := f.write(d, flush)
	return f.Path, writtenAtPos, n, err
}

func (f *File) Close() error {
	f.Lock()
	defer f.Unlock()

	return f.close(false)
}

// Flush flushes the file
func (f *File) Flush() error {
	f.Lock()
	defer f.Unlock()

	return f.file.Sync()
}
