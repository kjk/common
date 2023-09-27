package atomicfile

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"time"
)

// Some references:
// - https://www.slideshare.net/nan1nan1/eat-my-data
// - https://lwn.net/Articles/457667/

var (
	// ErrCancelled is returned by calls subsequent to Cancel()
	ErrCancelled = errors.New("cancelled")

	// ensure we implement desired interface
	_ io.WriteCloser = &File{}
)

// File allows writing to a file atomically
// i.e. if the while file is not written successfully, we make sure
// to clean things up
type File struct {
	dstPath string
	dir     string
	tmpFile *os.File
	err     error

	tmpPath string // for debugging
}

// New creates new File
func New(path string) (*File, error) {
	dir, fName := filepath.Split(path)
	dir, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	if fName == "" {
		return nil, &os.PathError{Op: "open", Path: path, Err: os.ErrInvalid}
	}

	tmpFile, err := os.CreateTemp(dir, fName)
	if err != nil {
		return nil, err
	}

	return &File{
		dstPath: path,
		dir:     dir,
		tmpFile: tmpFile,
		tmpPath: tmpFile.Name(),
	}, nil
}

func (f *File) handleError(err error) error {
	if err == nil {
		return nil
	}
	// remember the first errro
	if f.err == nil {
		f.err = err
	}
	// cleanup i.e. delete temporary file
	_ = f.Close()
	return err
}

// Write writes data to a file
func (f *File) Write(d []byte) (int, error) {
	if f.err != nil {
		return 0, f.err
	}
	n, err := f.tmpFile.Write(d)
	return n, f.handleError(err)
}

func (f *File) SetWriteDeadline(t time.Time) error {
	if f.err != nil {
		return f.err
	}
	err := f.tmpFile.SetWriteDeadline(t)
	return f.handleError(err)
}

func (f *File) Sync() error {
	if f.err != nil {
		return f.err
	}
	err := f.tmpFile.Sync()
	return f.handleError(err)
}

func (f *File) Truncate(size int64) error {
	if f.err != nil {
		return f.err
	}
	err := f.tmpFile.Truncate(size)
	return f.handleError(err)
}

func (f *File) Seek(offset int64, whence int) (ret int64, err error) {
	if f.err != nil {
		return 0, f.err
	}
	ret, err = f.tmpFile.Seek(offset, whence)
	return ret, f.handleError(err)
}

func (f *File) WriteAt(b []byte, off int64) (n int, err error) {
	if f.err != nil {
		return 0, f.err
	}
	n, err = f.tmpFile.WriteAt(b, off)
	return n, f.handleError(err)
}

func (f *File) WriteString(s string) (n int, err error) {
	if f.err != nil {
		return 0, f.err
	}
	n, err = f.tmpFile.WriteString(s)
	return n, f.handleError(err)
}

func (f *File) alreadyClosed() bool {
	return f.tmpFile == nil
}

// RemoveIfNotClosed removes the temp file if we didn't Close
// the file yet. Destination file will not be created.
// Use it with defer to ensure cleanup in case of a panic on the
// same goroutine that happens before Close.
// RemoveIfNotClosed after Close is a no-op.
func (f *File) RemoveIfNotClosed() {
	if f == nil {
		return
	}
	if f.alreadyClosed() {
		// a no-op if already closed
		return
	}

	f.err = ErrCancelled
	_ = f.Close()
}

// Close closes the file. Can be called multiple times to make it
// easier to use via defer
func (f *File) Close() error {
	if f.alreadyClosed() {
		// return the first error we encountered
		return f.err
	}
	tmpFile := f.tmpFile
	f.tmpFile = nil

	// cleanup things (delete temporary files) if:
	// - there was an error in Write()
	// - thre was an error in Sync()
	// - Close() failed
	// - rename to destination failed

	// https://www.joeshaw.org/dont-defer-close-on-writable-files/
	errSync := tmpFile.Sync()
	errClose := tmpFile.Close()

	// delete the temporary file in case of errors
	didRename := false
	defer func() {
		if !didRename {
			// ignoring error on this one
			_ = os.Remove(f.tmpPath)
		}
	}()

	// if there was an error during write, return that error
	if f.err != nil {
		return f.err
	}

	err := errSync
	if err == nil {
		err = errClose
	}

	if err == nil {
		// this will over-write dstPath (if it exists)
		err = os.Rename(f.tmpPath, f.dstPath)
		didRename = (err == nil)
		// for extra protection against crashes elsewhere,
		// sync directory after rename
		fdir, _ := os.Open(f.dir)
		if fdir != nil {
			// ignore errors as those are a nice have, not must have
			_ = fdir.Sync()
			_ = fdir.Close()
		}
	}

	if f.err == nil {
		f.err = err
	}
	return f.err
}
