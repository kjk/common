package u

import (
	"io"
	"io/fs"
	"strings"
	"time"
)

// MemoryFS is a custom file system that uses a map to store file data.
type MemoryFS struct {
	m       map[string][]byte
	modTime time.Time
}

func NewMemoryFS(m map[string][]byte) *MemoryFS {
	return &MemoryFS{
		m:       m,
		modTime: time.Now(),
	}
}

func NewMemoryFSForZipData(zipData []byte) (*MemoryFS, error) {
	m, err := ReadZipData(zipData)
	if err != nil {
		return nil, err
	}
	return NewMemoryFS(m), nil
}

// Open implements the fs.FS interface for MemoryFS.
func (m MemoryFS) Open(name string) (fs.File, error) {
	data, exists := m.m[name]
	if !exists {
		return nil, fs.ErrNotExist
	}

	return &memoryFile{name: name, data: data, modTime: m.modTime}, nil
}

// optimization for fs.ReadFile https://pkg.go.dev/io/fs#ReadFile
func (m MemoryFS) ReadFile(name string) ([]byte, error) {
	data, exists := m.m[name]
	if !exists {
		return nil, fs.ErrNotExist
	}
	return data, nil
}

// memoryFile is a custom type that satisfies the fs.File interface.
type memoryFile struct {
	name    string
	data    []byte
	off     int
	modTime time.Time
}

// Read implements the fs.File interface for memoryFile.
func (f *memoryFile) Read(b []byte) (int, error) {
	if f.off >= len(f.data) {
		return 0, io.EOF
	}
	n := copy(b, f.data[f.off:])
	f.off += n
	return n, nil
}

// Close implements the fs.File interface for memoryFile.
func (f *memoryFile) Close() error {
	return nil
}

// Stat implements the fs.File interface for memoryFile.
func (f *memoryFile) Stat() (fs.FileInfo, error) {
	return fileInfo{name: f.name, size: int64(len(f.data)), modTime: f.modTime}, nil
}

// fileInfo is a custom type that satisfies the fs.FileInfo interface.
type fileInfo struct {
	name    string
	size    int64
	modTime time.Time
}

// Name returns the name of the file.
func (fi fileInfo) Name() string {
	return fi.name
}

// Size returns the size of the file.
func (fi fileInfo) Size() int64 {
	return fi.size
}

// Mode returns the file mode (always regular file).
func (fi fileInfo) Mode() fs.FileMode {
	return 0
}

// ModTime returns the modification time (not implemented).
func (fi fileInfo) ModTime() time.Time {
	return fi.modTime
}

// IsDir returns false (always a regular file).
func (fi fileInfo) IsDir() bool {
	return false
}

// Sys returns nil (no system info available).
func (fi fileInfo) Sys() interface{} {
	return nil
}

func FsFileExists(fsys fs.FS, path string) bool {
	// paths must use '/' (unix) not '\' (windows)
	PanicIf(strings.Contains(path, "\\"))

	if mfs, ok := fsys.(*MemoryFS); ok {
		return mfs.m[path] != nil
	}

	if fstat, ok := fsys.(fs.StatFS); ok {
		st, err := fstat.Stat(path)
		return err == nil && !st.IsDir()
	}

	f, err := fsys.Open(path)
	if err != nil {
		return false
	}
	f.Close()
	return true
}
