package siser

import (
	"bytes"
	"io"
	"strconv"
	"time"
)

// Writer writes records to in a structured format
type Writer struct {
	w io.Writer
	// NoTimestamp disables writing timestamp, which
	// makes serialized data not depend on when they were written
	NoTimestamp bool

	writeBuf bytes.Buffer
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
	// TODO(perf): if !needsNewline, only serialize header and do 2 writers
	// to avoid copying memory. Not sure if will be faster than single write

	// most writes should be small. if buffer gets big, don't keep it
	// around (unbounded cache is a mem leak)
	if w.writeBuf.Cap() > 100*1024 && len(d) < 50*1024 {
		w.writeBuf = bytes.Buffer{}
	}
	w.writeBuf.Truncate(0)

	// for readability new record starts with "--- "
	w.writeBuf.Write(hdrPrefix)
	if w.NoTimestamp {
		w.writeBuf.WriteString(strconv.Itoa(len(d)))
	} else {
		if t.IsZero() {
			t = time.Now()
		}
		ms := TimeToUnixMillisecond(t)
		w.writeBuf.WriteString(strconv.Itoa(len(d)) + " " + strconv.FormatInt(ms, 10))
	}
	if name != "" {
		w.writeBuf.WriteString(" " + name)
	}
	w.writeBuf.WriteByte('\n')
	w.writeBuf.Write(d)

	// for readability, if the record doesn't end with newline,
	// we add one at the end. Makes decoding a bit harder but
	// not by much.
	n := len(d)
	needsNewline := (n > 0) && (d[n-1] != '\n')
	if needsNewline {
		w.writeBuf.WriteByte('\n')
	}
	n2, err := w.writeBuf.WriteTo(w.w)
	return int(n2), err
}
