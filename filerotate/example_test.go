package filerotate_test

import (
	"compress/gzip"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/kjk/common/filerotate"
)

// Daily rotating log file. A new file named e.g. "2024-05-17-app.log" is
// created in the "logs" directory every day.
//
// didClose is optional (can be nil). It's called after a file has been closed.
// didRotate is true if the file was closed because we rotated to a new file
// and false if it was closed because you called Close().
//
// A typical use is to post-process a finished log file (compress it,
// upload to S3 etc.). It's called while holding the file's lock, so if it
// does slow work, do it in a goroutine.
func ExampleNewDaily() {
	didClose := func(path string, didRotate bool) {
		fmt.Printf("closed file '%s', didRotate: %v\n", path, didRotate)
		if didRotate {
			// the day is over and this file will no longer be written to
			go compressFile(path)
		}
	}

	f, err := filerotate.NewDaily("logs", "app.log", didClose)
	if err != nil {
		log.Fatalf("filerotate.NewDaily() failed with '%s'\n", err)
	}
	defer f.Close()

	// *filerotate.File implements io.Writer so it can be used
	// with standard log package
	logger := log.New(f, "", log.LstdFlags)
	logger.Printf("hello\n")

	// or written to directly
	_, _ = f.Write([]byte("hello again\n"))
	fmt.Printf("current log file: %s\n", f.Path)
}

// Daily rotating file without post-processing.
func ExampleNewDaily_noDidClose() {
	f, err := filerotate.NewDaily("logs", "app.log", nil)
	if err != nil {
		log.Fatalf("filerotate.NewDaily() failed with '%s'\n", err)
	}
	defer f.Close()
	_, _ = f.Write([]byte("hello\n"))
}

// Custom rotation schedule using filerotate.New().
//
// PathIfShouldRotate is called before every write. creationTime is when
// the current file was opened (zero time before the first file is opened).
// It should return "" to keep writing to the current file or a path of a new
// file to rotate to.
func ExampleNew() {
	dir := "logs"
	config := filerotate.Config{
		PathIfShouldRotate: func(creationTime time.Time, now time.Time) string {
			// rotate every week
			y1, w1 := creationTime.ISOWeek()
			y2, w2 := now.ISOWeek()
			if y1 == y2 && w1 == w2 {
				return ""
			}
			name := fmt.Sprintf("%04d-week-%02d.log", y2, w2)
			return filepath.Join(dir, name)
		},
		DidClose: func(path string, didRotate bool) {
			fmt.Printf("closed '%s'\n", path)
		},
	}
	f, err := filerotate.New(&config)
	if err != nil {
		log.Fatalf("filerotate.New() failed with '%s'\n", err)
	}
	defer f.Close()
	_, _ = f.Write([]byte("hello\n"))
}

// compressFile compresses path to path.gz and deletes the original
func compressFile(path string) {
	src, err := os.Open(path)
	if err != nil {
		return
	}
	defer src.Close()
	dst, err := os.Create(path + ".gz")
	if err != nil {
		return
	}
	w := gzip.NewWriter(dst)
	_, err = io.Copy(w, src)
	err2 := w.Close()
	err3 := dst.Close()
	if err != nil || err2 != nil || err3 != nil {
		os.Remove(path + ".gz")
		return
	}
	src.Close()
	os.Remove(path)
}
