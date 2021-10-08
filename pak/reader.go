package pak

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"

	"github.com/kjk/common/siser"
)

var (
	// ErrNoPath is returned when path is not provided
	ErrNoPath = errors.New("no Path provided")
)

// Entry represents a single file in the archive
type Entry struct {
	// Metadata is arbitrary metadata.
	// Has at least Size, Path and Sha1 values
	Metadata Metadata

	// Path of the file. Recomended to use '/' for path separator
	Path string

	// offset within the file
	Offset int64

	// size of the entry, in bytes
	Size int64

	// sha1 of content, in hex format
	Sha1 string

	// fields only used when writing
	// set if this was AddFile()
	srcFilePath string
	// data from AddData() or content of file from AddFile()
	data []byte
}

// Archive represents an archive
type Archive struct {
	Path    string
	Entries []*Entry

	// if true, will disable validating sha1 on reading
	DisableValidateSha1 bool
}

// ReadArchive reads archive from a file
func ReadArchive(path string) (*Archive, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	a, err := ReadArchiveFromReader(f)
	if err != nil {
		return nil, err
	}
	a.Path = path
	return a, nil
}

// ReadArchiveFromReader reads archive entries
func ReadArchiveFromReader(r io.Reader) (*Archive, error) {
	br := bufio.NewReader(r)
	sr := siser.NewReader(br)

	// read the header which is a siser-formatted block of data
	// containing siser-formatted records for entries
	sr.ReadNextData()
	if sr.Err() != nil {
		return nil, sr.Err()
	}
	if sr.Name != archiveName {
		return nil, fmt.Errorf("expected header named '%s', got '%s'", archiveName, sr.Name)
	}
	// this is where data starts in the file
	// this is the size of the header
	entriesOffset := sr.NextRecordPos
	dataBuf := bytes.NewBuffer(sr.Data)
	hdrDataBuf := bufio.NewReader(dataBuf)
	sr = siser.NewReader(hdrDataBuf)

	currOffset := entriesOffset

	var entries []*Entry
	for sr.ReadNextRecord() {
		var meta Metadata
		for _, e := range sr.Record.Entries {
			meta.Set(e.Key, e.Value)
		}

		sizeStr, ok := meta.Get(MetaKeySize)
		if !ok {
			return nil, fmt.Errorf("missing '%s' value", MetaKeySize)
		}
		size, err := strconv.ParseInt(sizeStr, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("value '%s' for 'Size' is not a valid number. Error: %s", sizeStr, err)
		}

		path, ok := meta.Get(MetaKeyPath)
		if !ok {
			return nil, fmt.Errorf("missing '%s' value", MetaKeyPath)
		}
		sha1, ok := meta.Get(MetaKeySha1)
		if !ok {
			return nil, fmt.Errorf("missing '%s' value", MetaKeySha1)
		}

		e := &Entry{
			Metadata: meta,
			Path:     path,
			Offset:   currOffset,
			Size:     size,
			Sha1:     sha1,
		}
		entries = append(entries, e)

		currOffset += size
	}

	if sr.Err() != nil {
		return nil, sr.Err()
	}

	a := &Archive{
		Entries: entries,
	}
	return a, nil
}

// reads a part of a file of a given size at an offset
func readFileChunk(path string, offset, size int64) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	d := make([]byte, int(size))
	_, err = f.ReadAt(d, offset)
	if err != nil {
		return nil, err
	}
	return d, nil
}

// ReadEntry reads a given entry from file in Path
func (a *Archive) ReadEntry(e *Entry) ([]byte, error) {
	if a.Path == "" {
		return nil, ErrNoPath
	}
	d, err := readFileChunk(a.Path, e.Offset, e.Size)
	if err != nil {
		return nil, err
	}
	if !a.DisableValidateSha1 {
		sha1Got := sha1HexOfBytes(d)
		if e.Sha1 != sha1Got {
			return nil, fmt.Errorf("mismatched sha1 for file '%s'. Expected: %s, got: %s", e.Path, e.Sha1, sha1Got)
		}
	}
	return d, nil
}
