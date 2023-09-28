package u

import (
	"testing"

	"github.com/kjk/common/assert"
)

func TestMemoryFS(t *testing.T) {
	fileData := map[string][]byte{
		"file1.txt": []byte("This is the content of file1.txt"),
		"file2.txt": []byte("This is the content of file2.txt"),
	}
	fs := NewMemoryFS(fileData)
	{
		path := "file1.txt"
		d, err := FsReadFile(fs, path)
		assert.NoError(t, err)
		exp := fileData[path]
		assert.Equal(t, exp, d)
	}
	{
		path := "file2.txt"
		d, err := FsReadFile(fs, path)
		assert.NoError(t, err)
		exp := fileData[path]
		assert.Equal(t, exp, d)
	}
	{
		path := "file3.txt"
		d, err := FsReadFile(fs, path)
		assert.Error(t, err)
		assert.Nil(t, d)
	}
}
