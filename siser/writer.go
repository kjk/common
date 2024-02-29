package siser

import (
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
	// TODO(perf): add re-usable Writer.writeBuf bytes.Buffer to
	// avoid allocating buf every time
	// forthermore, if !needsNewline, only serialize header and do 2 writers
	// to avoid copying memory. Not sure if will be faster than single write

	// for readability new record starts with this marker
	hdr := "--- "
	if w.NoTimestamp {
		hdr += strconv.Itoa(len(d))
	} else {
		if t.IsZero() {
			t = time.Now()
		}
		ms := TimeToUnixMillisecond(t)
		hdr += strconv.Itoa(len(d)) + " " + strconv.FormatInt(ms, 10)
	}
	if name != "" {
		hdr += " " + name
	}
	hdr += "\n"
	n := len(d)
	bufSize := len(hdr) + n
	// for readability, if the record doesn't end with newline,
	// we add one at the end. Makes decoding a bit harder but
	// not by much.
	needsNewline := (n > 0) && (d[n-1] != '\n')
	if needsNewline {
		bufSize += 1
	}

	buf := make([]byte, 0, bufSize)
	buf = append(buf, hdr...)
	buf = append(buf, d...)
	if needsNewline {
		buf = append(buf, '\n')
	}
	return w.w.Write(buf)
}
