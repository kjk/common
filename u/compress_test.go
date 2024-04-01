package u

import (
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

	d2, err := ReadFileMaybeCompressed(dstPath)
	assert.Nil(t, err)
	assert.Equal(t, d, d2)
}

func TestGzip(t *testing.T) {
	testGzip(t, "compress.go")
}

func TestZstdData(t *testing.T) {
	d := []byte("hello, world")
	for len(d) < 1024*10 {
		d = append(d, d...)
	}
	d2, err := ZstdCompressData(d)
	assert.Nil(t, err)
	d3, err := ZstdDecompressData(d2)
	assert.Nil(t, err)
	assert.Equal(t, d, d3)
}

func testZstdFile(t *testing.T, path string) {
	d, err := os.ReadFile(path)
	assert.Nil(t, err)

	dstPath := path + ".zstd"
	err = ZstdCompressFile(dstPath, path)
	defer os.Remove(dstPath)
	assert.Nil(t, err)

	{
		d2, err := ZstdReadFile(dstPath)
		assert.Nil(t, err)
		assert.Equal(t, d, d2)
	}
	{
		d2, err := ReadFileMaybeCompressed(dstPath)
		assert.Nil(t, err)
		assert.Equal(t, d, d2)
	}
}

func TestZstdFile(t *testing.T) {
	testZstdFile(t, "compress.go")
}
