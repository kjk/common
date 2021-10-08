package pak

import (
	"bytes"
	"crypto/sha1"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"time"

	"github.com/kjk/common/siser"
)

const (
	// MetaKeyPath is name of mandatory "Path" meta-value
	MetaKeyPath = "Path"
	// MetaKeySize is name of mandatory "Size" meta-value
	MetaKeySize = "Size"
	// MetaKeySha1 is name of mandatory "Sha1" meta-value
	MetaKeySha1 = "Sha1"

	archiveName      = "pak-archive2"
	archiveEntryName = "pak-entry"
)

// Writer is for creating an archive
type Writer struct {
	// Entries is exposed so that we can re-arrange (e.g. sort)
	// them before calling Write
	Entries []*Entry

	// TODO: add option to conserve memory when writing
}

// NewWriter creates a new archive writer
func NewWriter() *Writer {
	return &Writer{}
}

func getFileSize(path string) (int64, error) {
	fi, err := os.Lstat(path)
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}

func sha1HexOfBytes(d []byte) string {
	h := sha1.New()
	h.Write(d)
	return fmt.Sprintf("%x", h.Sum(nil))
}

// AddFile adds a file from disk to the archive. If meta has "Path"
// value, it'll over-write path of the file in meta-data
func (w *Writer) AddFile(path string, meta Metadata) error {
	size, err := getFileSize(path)
	if err != nil {
		return err
	}

	// TODO: an option that preserves memory i.e. doesn't keep
	// data in memory. It'll be slower because it'll have to
	// read files twice
	d, err := ioutil.ReadFile(path)
	if err != nil {
		return err
	}

	e := &Entry{
		srcFilePath: path,
		data:        d,
		Path:        path,
		Size:        size,
		Sha1:        sha1HexOfBytes(d),
		Metadata:    meta,
	}

	// if "Path" is in meta-data, it over-writes physical path
	if v, ok := meta.Get("Path"); ok {
		if v == "" {
			return ErrNoPath
		}
		e.Path = v
	}

	w.Entries = append(w.Entries, e)
	return nil
}

// AddData adds a file from disk to the archive
func (w *Writer) AddData(d []byte, path string, meta Metadata) error {
	if path == "" {
		return ErrNoPath
	}
	sha1 := sha1HexOfBytes(d)
	e := &Entry{
		data:     d,
		Path:     path,
		Size:     int64(len(d)),
		Sha1:     sha1,
		Metadata: meta,
	}
	w.Entries = append(w.Entries, e)
	return nil
}

func serializeHeader(entries []*Entry) ([]byte, error) {
	var buf bytes.Buffer
	sw := siser.NewWriter(&buf)

	var r siser.Record
	for _, e := range entries {
		r.Reset()

		meta := e.Metadata
		meta.Set(MetaKeyPath, e.Path)
		meta.Set(MetaKeySize, strconv.FormatInt(e.Size, 10))
		meta.Set(MetaKeySha1, e.Sha1)

		for _, kv := range meta.Meta {
			r.Write(kv.Key, kv.Value)
		}

		r.Name = archiveEntryName

		_, err := sw.WriteRecord(&r)
		if err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

// WriteToFile writes an archive to a file
func (w *Writer) WriteToFile(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	err = w.Write(f)
	err2 := f.Close()

	if err != nil || err2 != nil {
		os.Remove(path)
		if err != nil {
			return err
		}
		return err2
	}
	return nil
}

// Write writes an archive to a writer
func (w *Writer) Write(wr io.Writer) (err error) {
	if wr == nil {
		return errors.New("must provide io.Writer to NewArchiveWriter")
	}

	if len(w.Entries) == 0 {
		return errors.New("there are 0 entries to write")
	}

	hdr, err := serializeHeader(w.Entries)
	if err != nil {
		return err
	}

	sw := siser.NewWriter(wr)
	if _, err = sw.Write(hdr, time.Now(), archiveName); err != nil {
		return err
	}

	// write files at the end of the archive
	for _, e := range w.Entries {
		d := e.data
		if d == nil && e.srcFilePath != "" {
			d2, err := ioutil.ReadFile(e.srcFilePath)
			if err != nil {
				return err
			}
			d = d2
		}

		if len(d) == 0 {
			// it's ok to have empty files
			continue
		}

		if _, err = wr.Write(d); err != nil {
			return err
		}
	}
	return nil
}
