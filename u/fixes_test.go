package u

import (
	"archive/zip"
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/kjk/common/assert"
)

func TestMakeExcludeDirsFilterAbsPath(t *testing.T) {
	// used to loop forever on absolute paths
	f := MakeExcludeDirsFilter("node_modules")
	abs, err := filepath.Abs(filepath.Join("foo", "bar.txt"))
	assert.NoError(t, err)
	assert.True(t, f(abs))
	abs, err = filepath.Abs(filepath.Join("node_modules", "x", "bar.txt"))
	assert.NoError(t, err)
	assert.False(t, f(abs))
}

func TestMakeAllowedFileFilterForExtsNoMutate(t *testing.T) {
	exts := []string{".GO", ".Txt"}
	f := MakeAllowedFileFilterForExts(exts...)
	assert.True(t, f("foo.go"))
	assert.Equal(t, ".GO", exts[0])
}

func TestCapitalizeUTF8(t *testing.T) {
	assert.Equal(t, "Élan", Capitalize("élan"))
	assert.Equal(t, "Foo", Capitalize("FOO"))
	assert.Equal(t, "", Capitalize(""))
}

func TestZipDirToWriterTrailingSlash(t *testing.T) {
	dir := t.TempDir()
	assert.NoError(t, os.WriteFile(filepath.Join(dir, "bar.txt"), []byte("bar"), 0644))
	for _, d := range []string{dir, dir + string(filepath.Separator)} {
		var buf bytes.Buffer
		assert.NoError(t, ZipDirToWriter(&buf, d))
		zr, err := zip.NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		assert.NoError(t, err)
		assert.Equal(t, 1, len(zr.File))
		assert.Equal(t, "bar.txt", zr.File[0].Name)
	}
}

func makeZip(t *testing.T, names ...string) []byte {
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	for _, name := range names {
		w, err := zw.Create(name)
		assert.NoError(t, err)
		if name[len(name)-1] != '/' {
			_, _ = w.Write([]byte(name))
		}
	}
	assert.NoError(t, zw.Close())
	return buf.Bytes()
}

func TestUnzipDataToDir(t *testing.T) {
	dir := t.TempDir()
	// directory entries used to be written as files
	d := makeZip(t, "sub/", "sub/a.txt")
	assert.NoError(t, UnzipDataToDir(d, dir))
	got, err := os.ReadFile(filepath.Join(dir, "sub", "a.txt"))
	assert.NoError(t, err)
	assert.Equal(t, "sub/a.txt", string(got))

	// zip-slip must be rejected
	d = makeZip(t, "../evil.txt")
	assert.Error(t, UnzipDataToDir(d, filepath.Join(dir, "inner")))
	assert.False(t, FileExists(filepath.Join(dir, "evil.txt")))
}

func TestGetCallstackFramesIncludesLast(t *testing.T) {
	frames := GetCallstackFrames(0)
	assert.True(t, len(frames) > 0)
}
