package u

import (
	"archive/zip"
	"bytes"
	"io"
	"io/fs"
	"testing"
	"testing/fstest"

	"github.com/kjk/common/assert"
)

func TestMemoryFS(t *testing.T) {
	fileData := map[string][]byte{
		"file1.txt": []byte("This is the content of file1.txt"),
		"file2.txt": []byte("This is the content of file2.txt"),
	}
	fsys := NewMemoryFS(fileData)
	{
		path := "file1.txt"
		d, err := fs.ReadFile(fsys, path)
		assert.NoError(t, err)
		exp := fileData[path]
		assert.Equal(t, exp, d)
	}
	{
		path := "file2.txt"
		d, err := fs.ReadFile(fsys, path)
		assert.NoError(t, err)
		exp := fileData[path]
		assert.Equal(t, exp, d)
	}
	{
		path := "file3.txt"
		d, err := fs.ReadFile(fsys, path)
		assert.Error(t, err)
		assert.Nil(t, d)
		var pe *fs.PathError
		assert.True(t, errorsAs(err, &pe))
	}
}

func errorsAs(err error, pe **fs.PathError) bool {
	p, ok := err.(*fs.PathError)
	if ok {
		*pe = p
	}
	return ok
}

func TestMemoryFSConformance(t *testing.T) {
	fileData := map[string][]byte{
		"index.html":       []byte("<html></html>"),
		"a/b.txt":          []byte("b"),
		"a/c/d.txt":        []byte("d"),
		"a/c/e.txt":        []byte(""),
		"empty-dir/":       nil, // directory entry as in zip files
		"x/y/z/deep.txt":   []byte("deep"),
		"x/sibling.txt":    []byte("sibling"),
		"x/y/another.json": []byte("{}"),
	}
	fsys := NewMemoryFS(fileData)
	err := fstest.TestFS(fsys, "index.html", "a/b.txt", "a/c/d.txt", "a/c/e.txt",
		"empty-dir", "x/y/z/deep.txt", "x/sibling.txt", "x/y/another.json")
	assert.NoError(t, err)

	// fs.WalkDir works
	var paths []string
	err = fs.WalkDir(fsys, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			paths = append(paths, path)
		}
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, 7, len(paths))

	// ReadFile returns a copy
	d, err := fsys.ReadFile("a/b.txt")
	assert.NoError(t, err)
	d[0] = 'X'
	assert.Equal(t, "b", string(fileData["a/b.txt"]))

	// files are seekable (needed by http.FileServer for range requests)
	f, err := fsys.Open("index.html")
	assert.NoError(t, err)
	seeker, ok := f.(io.Seeker)
	assert.True(t, ok)
	_, err = seeker.Seek(6, io.SeekStart)
	assert.NoError(t, err)
	rest, err := io.ReadAll(f)
	assert.NoError(t, err)
	assert.Equal(t, "</html>", string(rest))

	assert.True(t, FsFileExists(fsys, "a/b.txt"))
	assert.False(t, FsFileExists(fsys, "a"))
	assert.False(t, FsFileExists(fsys, "empty-dir/"))
}

func TestMemoryFSForZipData(t *testing.T) {
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	_, err := zw.Create("sub/")
	assert.NoError(t, err)
	w, err := zw.Create("sub/a.txt")
	assert.NoError(t, err)
	_, _ = w.Write([]byte("a"))
	assert.NoError(t, zw.Close())

	fsys, err := NewMemoryFSForZipData(buf.Bytes())
	assert.NoError(t, err)
	assert.NoError(t, fstest.TestFS(fsys, "sub/a.txt"))
	entries, err := fs.ReadDir(fsys, "sub")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(entries))
	assert.Equal(t, "a.txt", entries[0].Name())
}
