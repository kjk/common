package u

import (
	"bytes"
	"io"
	"os"
	"testing"

	"github.com/kjk/common/assert"
)

func testGzip(t *testing.T, path string) {
	d, err := os.ReadFile(path)
	assert.Nil(t, err)

	dstPath := path + ".gz"
	err = GzipFile(dstPath, path)
	defer os.Remove(dstPath)
	assert.Nil(t, err)
	r, err := OpenFileMaybeCompressed(dstPath)
	assert.Nil(t, err)
	defer r.Close()
	var dst bytes.Buffer
	_, err = io.Copy(&dst, r)
	assert.Nil(t, err)
	d2 := dst.Bytes()
	assert.Equal(t, d, d2)
}

func TestGzip(t *testing.T) {
	testGzip(t, "compress.go")
}
