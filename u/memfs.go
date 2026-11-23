package u

import (
	"bytes"
	"io"
	"io/fs"
	"path"
	"slices"
	"strings"
	"time"
)

// MemoryFS is a read-only file system backed by a map of path => file data.
// Paths use '/' as separator and must be valid fs.FS paths (see fs.ValidPath).
// Directories are implied by file paths (e.g. "a/b.txt" implies directory "a").
// Keys ending with "/" (e.g. directory entries from zip files) are treated
// as directories.
//
// It implements fs.FS, fs.ReadFileFS, fs.ReadDirFS and fs.StatFS.
// The map must not be modified after creating MemoryFS.
type MemoryFS struct {
	m       map[string][]byte
	modTime time.Time
	// dir path => sorted names of direct children (files and dirs)
	dirs map[string][]string
}

var (
	_ fs.FS         = &MemoryFS{}
	_ fs.ReadFileFS = &MemoryFS{}
	_ fs.ReadDirFS  = &MemoryFS{}
	_ fs.StatFS     = &MemoryFS{}
)

func NewMemoryFS(m map[string][]byte) *MemoryFS {
	res := &MemoryFS{
		m:       m,
		modTime: time.Now(),
		dirs:    map[string][]string{".": nil},
	}
	for name := range m {
		name = strings.TrimSuffix(name, "/")
		if name == "" || name == "." {
			continue
		}
		// register name in its parent dir and all parent dirs in their parents
		for {
			dir := path.Dir(name)
			children, dirKnown := res.dirs[dir]
			base := path.Base(name)
			if !slices.Contains(children, base) {
				res.dirs[dir] = append(children, base)
			}
			if dirKnown || dir == "." {
				break
			}
			name = dir
		}
	}
	// ensure that dirs that only exist as "dir/" keys are known
	for name := range m {
		if strings.HasSuffix(name, "/") {
			dir := strings.TrimSuffix(name, "/")
			if _, ok := res.dirs[dir]; !ok {
				res.dirs[dir] = nil
			}
		}
	}
	for _, children := range res.dirs {
		slices.Sort(children)
	}
	return res
}

func NewMemoryFSForZipData(zipData []byte) (*MemoryFS, error) {
	m, err := ReadZipData(zipData)
	if err != nil {
		return nil, err
	}
	return NewMemoryFS(m), nil
}

// getFile returns data for a file (not a directory)
func (m MemoryFS) getFile(name string) ([]byte, bool) {
	if strings.HasSuffix(name, "/") {
		return nil, false
	}
	d, ok := m.m[name]
	return d, ok
}

func (m MemoryFS) isDir(name string) bool {
	_, ok := m.dirs[name]
	return ok
}

func (m MemoryFS) stat(name string) (fileInfo, bool) {
	if d, ok := m.getFile(name); ok {
		return fileInfo{name: path.Base(name), size: int64(len(d)), modTime: m.modTime}, true
	}
	if m.isDir(name) {
		return fileInfo{name: path.Base(name), modTime: m.modTime, isDir: true}, true
	}
	return fileInfo{}, false
}

// Open implements fs.FS
func (m MemoryFS) Open(name string) (fs.File, error) {
	if !fs.ValidPath(name) {
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrInvalid}
	}
	if d, ok := m.getFile(name); ok {
		return &memoryFile{
			info:   fileInfo{name: path.Base(name), size: int64(len(d)), modTime: m.modTime},
			reader: bytes.NewReader(d),
		}, nil
	}
	if m.isDir(name) {
		return &memoryDir{fsys: m, path: name}, nil
	}
	return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrNotExist}
}

// ReadFile implements fs.ReadFileFS (an optimization for fs.ReadFile).
// Returns a copy because callers are allowed to modify returned data.
func (m MemoryFS) ReadFile(name string) ([]byte, error) {
	if !fs.ValidPath(name) {
		return nil, &fs.PathError{Op: "readfile", Path: name, Err: fs.ErrInvalid}
	}
	d, ok := m.getFile(name)
	if !ok {
		if m.isDir(name) {
			return nil, &fs.PathError{Op: "readfile", Path: name, Err: fs.ErrInvalid}
		}
		return nil, &fs.PathError{Op: "readfile", Path: name, Err: fs.ErrNotExist}
	}
	return bytes.Clone(d), nil
}

// Stat implements fs.StatFS
func (m MemoryFS) Stat(name string) (fs.FileInfo, error) {
	if !fs.ValidPath(name) {
		return nil, &fs.PathError{Op: "stat", Path: name, Err: fs.ErrInvalid}
	}
	fi, ok := m.stat(name)
	if !ok {
		return nil, &fs.PathError{Op: "stat", Path: name, Err: fs.ErrNotExist}
	}
	return fi, nil
}

// ReadDir implements fs.ReadDirFS. Entries are sorted by name.
func (m MemoryFS) ReadDir(name string) ([]fs.DirEntry, error) {
	if !fs.ValidPath(name) {
		return nil, &fs.PathError{Op: "readdir", Path: name, Err: fs.ErrInvalid}
	}
	if !m.isDir(name) {
		return nil, &fs.PathError{Op: "readdir", Path: name, Err: fs.ErrNotExist}
	}
	return m.dirEntries(name), nil
}

func (m MemoryFS) dirEntries(dir string) []fs.DirEntry {
	children := m.dirs[dir]
	res := make([]fs.DirEntry, 0, len(children))
	for _, child := range children {
		p := child
		if dir != "." {
			p = dir + "/" + child
		}
		fi, _ := m.stat(p)
		res = append(res, fs.FileInfoToDirEntry(fi))
	}
	return res
}

// memoryFile is an open file. Implements fs.File, io.Seeker and io.ReaderAt
// (needed e.g. by http.FileServer to serve range requests).
type memoryFile struct {
	info   fileInfo
	reader *bytes.Reader
}

func (f *memoryFile) Read(b []byte) (int, error) {
	return f.reader.Read(b)
}

func (f *memoryFile) Seek(offset int64, whence int) (int64, error) {
	return f.reader.Seek(offset, whence)
}

func (f *memoryFile) ReadAt(b []byte, off int64) (int, error) {
	return f.reader.ReadAt(b, off)
}

func (f *memoryFile) Close() error {
	return nil
}

func (f *memoryFile) Stat() (fs.FileInfo, error) {
	return f.info, nil
}

// memoryDir is an open directory. Implements fs.ReadDirFile.
type memoryDir struct {
	fsys    MemoryFS
	path    string
	entries []fs.DirEntry
	off     int
	read    bool
}

func (d *memoryDir) Read(b []byte) (int, error) {
	return 0, &fs.PathError{Op: "read", Path: d.path, Err: fs.ErrInvalid}
}

func (d *memoryDir) Close() error {
	return nil
}

func (d *memoryDir) Stat() (fs.FileInfo, error) {
	fi, _ := d.fsys.stat(d.path)
	return fi, nil
}

// ReadDir implements fs.ReadDirFile
func (d *memoryDir) ReadDir(n int) ([]fs.DirEntry, error) {
	if !d.read {
		d.entries = d.fsys.dirEntries(d.path)
		d.read = true
	}
	remaining := d.entries[d.off:]
	if n <= 0 {
		d.off = len(d.entries)
		return remaining, nil
	}
	if len(remaining) == 0 {
		return nil, io.EOF
	}
	n = min(n, len(remaining))
	d.off += n
	return remaining[:n], nil
}

// fileInfo implements fs.FileInfo
type fileInfo struct {
	name    string
	size    int64
	modTime time.Time
	isDir   bool
}

// Name returns base name of the file
func (fi fileInfo) Name() string {
	return fi.name
}

func (fi fileInfo) Size() int64 {
	return fi.size
}

func (fi fileInfo) Mode() fs.FileMode {
	if fi.isDir {
		return fs.ModeDir | 0555
	}
	return 0444
}

func (fi fileInfo) ModTime() time.Time {
	return fi.modTime
}

func (fi fileInfo) IsDir() bool {
	return fi.isDir
}

func (fi fileInfo) Sys() any {
	return nil
}

func FsFileExists(fsys fs.FS, path string) bool {
	// paths must use '/' (unix) not '\' (windows)
	PanicIf(strings.Contains(path, "\\"))

	if mfs, ok := fsys.(*MemoryFS); ok {
		_, ok := mfs.getFile(path)
		return ok
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
