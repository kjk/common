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
	err = GzipCompressFile(dstPath, path)
	defer os.Remove(dstPath)
	assert.Nil(t, err)

	{
		d2, err := GzipReadFile(dstPath)
		assert.Nil(t, err)
		assert.Equal(t, d, d2)
	}
	{
		d2, err := ReadFileMaybeCompressed(dstPath)
		assert.Nil(t, err)
		assert.Equal(t, d, d2)
	}
}

func TestGzip(t *testing.T) {
	testGzip(t, "compress.go")
}

func testCompressDecompressData(t *testing.T, compress func([]byte) ([]byte, error), decompress func([]byte) ([]byte, error)) {
	d := []byte("hello, world")
	for len(d) < 1024*10 {
		d = append(d, d...)
	}
	d2, err := compress(d)
	assert.Nil(t, err)
	d3, err := decompress(d2)
	assert.Nil(t, err)
	assert.Equal(t, d, d3)
}

func TestZstdData(t *testing.T) {
	testCompressDecompressData(t, ZstdCompressDataBest, ZstdDecompressData)
	testCompressDecompressData(t, ZstdCompressDataDefault, ZstdDecompressData)
}

func TestBrData(t *testing.T) {
	testCompressDecompressData(t, BrCompressDataBest, BrDecompressData)
	testCompressDecompressData(t, BrCompressDataDefault, BrDecompressData)
}

func TestGzipData(t *testing.T) {
	testCompressDecompressData(t, GzipCompressData, GzipDecompressData)
}

func testCompressDecompressFile(t *testing.T, path string, dstPath string, compress func(string, string) error, decompress func(string) ([]byte, error)) {
	d, err := os.ReadFile(path)
	assert.Nil(t, err)

	err = compress(dstPath, path)
	defer os.Remove(dstPath)
	assert.Nil(t, err)

	{
		d2, err := decompress(dstPath)
		assert.Nil(t, err)
		assert.Equal(t, d, d2)
	}
	{
		d2, err := ReadFileMaybeCompressed(dstPath)
		assert.Nil(t, err)
		assert.Equal(t, d, d2)
	}
}

func TestGzipFile(t *testing.T) {
	path := "compress.go"
	testCompressDecompressFile(t, path, path+".gz", GzipCompressFile, GzipReadFile)
}

func TestBrFile(t *testing.T) {
	path := "compress.go"
	testCompressDecompressFile(t, path, path+".br", BrCompressFileBest, BrReadFile)
}

func TestZstdFile(t *testing.T) {
	path := "compress.go"
	testCompressDecompressFile(t, path, path+".zstd", ZstdCompressFileBest, ZstdReadFile)
	testCompressDecompressFile(t, path, path+".zstd", ZstdCompressFileDefault, ZstdReadFile)
}
