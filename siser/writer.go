package siser

import (
	"bytes"
	"io"
	"strconv"
	"sync"
	"time"
)

// Writer writes records to in a structured format
type Writer struct {
	w io.Writer
	// NoTimestamp disables writing timestamp, which
	// makes serialized data not depend on when they were written
	NoTimestamp bool

	writeBuf bytes.Buffer
	mu       sync.Mutex
}

// NewWriter creates a writer
func NewWriter(w io.Writer) *Writer {
	return &Writer{
		w: w,
	}
}

// WriteRecord writes a record in a specified format
func (w *Writer) WriteRecord(r *Record) (int, error) {
	d := r.Marshal()
	n, err := w.Write(d, r.Timestamp, r.Name)
	r.Reset()
	return n, err
}

// Write writes a block of data with optional timestamp and name.
// Returns number of bytes written (length of d + lenght of metadata)
// and an error
func (w *Writer) Write(d []byte, t time.Time, name string) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// most writes should be small. if buffer gets big, don't keep it
	// around (unbounded cache is a mem leak)
	if w.writeBuf.Cap() > 100*1024 && len(d) < 50*1024 {
		w.writeBuf = bytes.Buffer{}
		w.writeBuf.Reset()
	}

	if w.NoTimestamp {
		t = time.Time{} // make it time.Zero() so that it's not written
	} else {
		if t.IsZero() {
			t = time.Now()
		}
	}

	d2 := MarshalLine(name, t, d, &w.writeBuf)
	n, err := w.w.Write(d2)
	return int(n), err
}

func MarshalLineNoTime(name string, d []byte, wb *bytes.Buffer) []byte {
	return MarshalLine(name, zeroTime, d, wb)
}

// if t is time.Zero(), it's not marshalled
func MarshalLine(name string, t time.Time, d []byte, wb *bytes.Buffer) []byte {
	if wb == nil {
		wb = &bytes.Buffer{}
	} else {
		wb.Reset()
	}
	// it's ok to estimate more, estimating less will require an alloc
	esitimatedSize := len(hdrPrefix) + len(name) + len(d) + 128 // 128 for timestamp and other
	wb.Grow(esitimatedSize)

	// for readability new record starts with "--- "
	wb.Write(hdrPrefix)
	// length of data
	dataLen := len(d)
	wb.WriteString(strconv.Itoa(dataLen))
	// timestamp if not zero
	if !t.IsZero() {
		wb.WriteString(" ")
		ms := TimeToUnixMillisecond(t)
		wb.WriteString(strconv.FormatInt(ms, 10))
	}
	if name != "" {
		wb.WriteString(" ")
		wb.WriteString(name)
	}
	wb.WriteByte('\n')
	// for readability, if the record doesn't end with newline,
	// we add one at the end. Makes decoding a bit harder but
	// not by much.
	if dataLen > 0 {
		wb.Write(d)
		if d[dataLen-1] != '\n' {
			wb.WriteByte('\n')
		}
	}
	return wb.Bytes()
}
