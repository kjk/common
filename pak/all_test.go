package pak

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/kjk/common/assert"
)

func must(err error) {
	if err != nil {
		panic(err.Error())
	}
}

type test struct {
	isFile bool
	data   []byte
	// path is for both data and files
	path string
	// for files, use to over-ride the path of the file
	pathOverride string
	size         int64
	tp           string
}

func mkFile(path string) *test {
	size, err := getFileSize(path)
	must(err)
	d, err := ioutil.ReadFile(path)
	must(err)
	return &test{
		isFile: true,
		path:   path,
		size:   size,
		data:   d,
	}
}

func mkData(d []byte, path string) *test {
	return &test{
		data: d,
		path: path,
		size: int64(len(d)),
	}
}

func writeTests(t *testing.T, archivePath string, tests []*test) {
	w := NewWriter()

	for _, test := range tests {
		var meta Metadata
		// check additional metadata
		if test.tp != "" {
			meta.Set("Type", test.tp)
		}
		if test.isFile {
			if test.pathOverride != "" {
				meta.Set("Path", test.pathOverride)
			}
			err := w.AddFile(test.path, meta)
			assert.NoError(t, err)
		} else {
			err := w.AddData(test.data, test.path, meta)
			assert.NoError(t, err)
		}
	}

	f, err := os.Create(archivePath)
	assert.NoError(t, err)
	err = w.Write(f)
	assert.NoError(t, err)
	err = f.Close()
	assert.NoError(t, err)
}

func readAndVerifyTests(t *testing.T, archivePath string, tests []*test) {
	a, err := ReadArchive(archivePath)
	assert.NoError(t, err)

	entries := a.Entries
	assert.NoError(t, err)
	assert.Equal(t, len(entries), len(tests))

	for i, e := range entries {
		test := tests[i]
		d, err := a.ReadEntry(e)
		assert.NoError(t, err)
		assert.Equal(t, test.data, d)

		path := test.path
		if test.pathOverride != "" {
			path = test.pathOverride
		}

		assert.Equal(t, path, e.Path)
		v, _ := e.Metadata.Get("Path")
		assert.Equal(t, path, v)

		assert.Equal(t, test.size, e.Size)
		assert.Equal(t, test.data, d)
		expMetaSize := 3
		if test.tp != "" {
			v, _ = e.Metadata.Get("Type")
			assert.Equal(t, test.tp, v)
			expMetaSize++
		}
		assert.Equal(t, expMetaSize, e.Metadata.Size())
	}
}

func TestWriteRead(t *testing.T) {
	tests := []*test{
		mkFile("all_test.go"),
		mkFile("metadata.go"),
		mkFile("reader.go"),
		mkFile("writer.go"),
		mkData([]byte{}, "empty_file.txt"),
		mkFile("readme.md"),
		mkData([]byte{0x0, 0x1, 0x3, 0x0}, "some_data.dat"),
	}
	tests[1].tp = "sum file"
	tests[2].pathOverride = "LICENSE.overwritten"

	files, err := ioutil.ReadDir(".")
	assert.NoError(t, err)
	for _, fi := range files {
		if !fi.Mode().IsRegular() {
			continue
		}
		d, err := ioutil.ReadFile(fi.Name())
		must(err)
		test := mkData(d, fi.Name()+".data")
		tests = append(tests, test)
	}

	writeReadArchiveTests(t, "test_archive.txt", tests)
}

func writeReadArchiveTests(t *testing.T, archivePath string, tests []*test) {
	writeTests(t, archivePath, tests)

	// on error leave the archive file for inspection
	defer func() {
		if !t.Failed() {
			err := os.Remove(archivePath)
			if err != nil {
				fmt.Printf("os.Remove('%s') failed with '%s'\n", archivePath, err)
			}
		}
	}()
	readAndVerifyTests(t, archivePath, tests)

}

func TestBug(t *testing.T) {
	tests := []*test{
		// siser had issues when reading a record followed by record
		// starting with 0xa
		mkData([]byte{0xa, 0x1, 0x3, 0x0}, "some_data.dat"),
		mkFile("reader.go"),
	}

	writeReadArchiveTests(t, "test_archive_bug.txt", tests)
}

func TestMetadata(t *testing.T) {
	var m Metadata
	assert.Equal(t, 0, m.Size())
	var added, ok bool
	var v string
	added = m.Set("Foo", "bar")
	assert.True(t, added)
	v, ok = m.Get("Foo")
	assert.True(t, ok)
	assert.Equal(t, v, "bar")
	added = m.Set("foo", "bar")
	assert.True(t, added) // case insensitive
	assert.Equal(t, 2, m.Size())
	added = m.Set("Foo", "bar2") // update
	assert.False(t, added)
	assert.Equal(t, 2, m.Size())
	v, ok = m.Get("Foo")
	assert.True(t, ok)
	assert.Equal(t, v, "bar2")
}
