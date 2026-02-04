package u

import (
	"archive/zip"
	"bufio"
	"crypto/sha1"
	"fmt"
	"io"
	"io/fs"
	"iter"
	"os"
	"path"
	"path/filepath"
)

// PathExists returns true if path exists
func PathExists(path string) bool {
	_, err := os.Lstat(path)
	return err == nil
}

// FileExists returns true if path exists and is a regular file
func FileExists(path string) bool {
	st, err := os.Lstat(path)
	return err == nil && st.Mode().IsRegular()
}

// DirExists returns true if path exists and is a directory
func DirExists(path string) bool {
	st, err := os.Lstat(path)
	return err == nil && st.IsDir()
}

// PathIsDir returns true if a path exists and is a directory
// Returns false, nil if a path exists and is not a directory (e.g. a file)
// Returns undefined, error if there was an error e.g. because a path doesn't exists
func PathIsDir(path string) (isDir bool, err error) {
	fi, err := os.Stat(path)
	if err != nil {
		return false, err
	}
	return fi.IsDir(), nil
}

// FileSize gets file size, -1 if file doesn't exist
func FileSize(path string) int64 {
	st, err := os.Lstat(path)
	if err == nil {
		return st.Size()
	}
	return -1
}

// CopyFile copies a file from src to dst
// It'll create destination directory if necessary
func CopyFile(dst string, src string) error {
	err := os.MkdirAll(filepath.Dir(dst), 0755)
	if err != nil {
		return err
	}
	fin, err := os.Open(src)
	if err != nil {
		return err
	}
	defer fin.Close()
	fout, err := os.Create(dst)
	if err != nil {
		return err
	}

	_, err = io.Copy(fout, fin)
	err2 := fout.Close()
	if err != nil || err2 != nil {
		os.Remove(dst)
	}
	return err
}

func FileSha1Hex(path string) (string, error) {
	sha1OfFile := func(path string) ([]byte, error) {
		f, err := os.Open(path)
		if err != nil {
			//fmt.Printf("os.Open(%s) failed with %s\n", path, err.Error())
			return nil, err
		}
		defer f.Close()
		h := sha1.New()
		_, err = io.Copy(h, f)
		if err != nil {
			//fmt.Printf("io.Copy() failed with %s\n", err.Error())
			return nil, err
		}
		return h.Sum(nil), nil
	}

	sha1, err := sha1OfFile(path)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", sha1), nil
}

func DataSha1Hex(d []byte) string {
	sha1 := sha1.Sum(d)
	return fmt.Sprintf("%x", sha1[:])
}

// ReadLinesFromReader reads lines from io.Reader
func ReadLinesFromReader(r io.Reader) ([]string, error) {
	scanner := bufio.NewScanner(r)
	var res []string
	for scanner.Scan() {
		line := scanner.Bytes()
		res = append(res, string(line))
	}
	return res, scanner.Err()
}

// ReadLines reads file as lines
func ReadLines(filePath string) ([]string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return ReadLinesFromReader(file)
}

// CloseNoError is like io.Closer Close() but ignores an error
// use as: defer CloseNoError(f)
func CloseNoError(f io.Closer) {
	_ = f.Close()
}

func ReadZipFile(path string) (map[string][]byte, error) {
	r, err := zip.OpenReader(path)
	if err != nil {
		return nil, err
	}
	defer CloseNoError(r)
	res := map[string][]byte{}
	for _, f := range r.File {
		rc, err := f.Open()
		if err != nil {
			return nil, err
		}
		d, err := io.ReadAll(rc)
		err2 := rc.Close()
		if err != nil {
			return nil, err
		}
		if err2 != nil {
			return nil, err2
		}
		res[f.Name] = d
	}
	return res, nil
}

// call cb on each file in a directory
func IterDir(dir string, cb func(path string, de fs.DirEntry) (bool, error)) error {
	dirsToVisit := []string{dir}
	for len(dirsToVisit) > 0 {
		dir = dirsToVisit[0]
		dirsToVisit = dirsToVisit[1:]
		entries, err := os.ReadDir(dir)
		if err != nil {
			return err
		}
		for _, de := range entries {
			name := de.Name()
			if de.IsDir() {
				newDir := filepath.Join(dir, name)
				dirsToVisit = append(dirsToVisit, newDir)
				continue
			}
			shouldStop, err := cb(filepath.Join(dir, name), de)
			if err != nil {
				return err
			}
			if shouldStop {
				return nil
			}
		}
	}
	return nil
}

// call cb on each file in fsys
// startDir should be ". for root directory, "" and "/" do not work
func IterReadDirFS(fsys fs.ReadDirFS, startDir string, cb func(string, fs.DirEntry) error) error {
	if startDir == "" || startDir == "/" {
		return fmt.Errorf("startDir is '%s', should be '.' for ", startDir)
	}
	for v := range DirFSIter(fsys, startDir) {
		if err := cb(v.Path, v.DirEntry); err != nil {
			return err
		}
	}
	return nil
}

type DirEntryWithPath struct {
	Path     string
	DirEntry fs.DirEntry
}

// DirFSIter iterates over all entries and sub-entries in fsys starting from startDir.
// For each entry, it yields path relative to the root of fsys.
// Only provides files, not dirs.
func DirFSIter(fsys fs.ReadDirFS, startDir string) iter.Seq[*DirEntryWithPath] {
	return func(yield func(*DirEntryWithPath) bool) {
		dirsToVisit := []string{startDir}
		var v DirEntryWithPath
		for len(dirsToVisit) > 0 {
			dir := dirsToVisit[0]
			dirsToVisit = dirsToVisit[1:]
			entries, err := fsys.ReadDir(dir)
			if err != nil {
				return
			}

			for _, entry := range entries {
				filePath := path.Join(dir, entry.Name())

				if entry.IsDir() {
					dirsToVisit = append(dirsToVisit, filePath)
					continue
				}
				v.Path = filePath
				v.DirEntry = entry
				if !yield(&v) {
					return
				}
			}
		}
	}
}

func CountFilesInFS(fsys fs.ReadDirFS) int {
	n := 0
	for range DirFSIter(fsys, ".") {
		n++
	}
	return n
}

type syncer interface {
	Sync() error
}

func MaybeSync(w io.Writer) error {
	if f, ok := w.(syncer); ok {
		return f.Sync()
	}
	return nil
}

func WriteToFile(path string, content string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	os.Remove(path)
	return os.WriteFile(path, []byte(content), 0644)
}
