package u

import (
	"io"
	"os"
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
