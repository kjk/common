package siser

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strconv"
	"time"
)

// Reader is for reading (deserializing) records from a bufio.Reader
type Reader struct {
	r *bufio.Reader

	// hints that the data was written without a timestamp
	// (see Writer.NoTimestamp). We're permissive i.e. we'll
	// read timestamp if it's written even if NoTimestamp is true
	NoTimestamp bool

	// Record is available after ReadNextRecord().
	// It's over-written in next ReadNextRecord().
	Record *ReadRecord

	// Data / Name / Timestampe are available after ReadNextData.
	// They are over-written in next ReadNextData.
	Data      []byte
	Name      string
	Timestamp time.Time

	// position of the current record within the reader.
	// We keep track of it so that callers can index records
	// by offset and seek to it
	CurrRecordPos int64

	// position of the next record within the reader.
	NextRecordPos int64

	err error

	// true if reached end of file with io.EOF
	done bool
}

// NewReader creates a new reader
func NewReader(r *bufio.Reader) *Reader {
	return &Reader{
		r:      r,
		Record: &ReadRecord{},
	}
}

// Done returns true if we're finished reading from the reader
func (r *Reader) Done() bool {
	return r.err != nil || r.done
}

var hdrPrefix = []byte("--- ")

// ReadNextData reads next block from the reader, returns false
// when no more record. If returns false, check Err() to see
// if there were errors.
// After reading Data containst data, and Timestamp and (optional) Name
// contain meta-data
func (r *Reader) ReadNextData() bool {
	if r.Done() {
		return false
	}
	r.Name = ""
	r.CurrRecordPos = r.NextRecordPos

	// read header in the format:
	// "--- ${size} ${timestamp_in_unix_epoch_ms} ${name}\n"
	// or (if NoTimestamp):
	// "--- ${size} ${name}\n"
	// ${name} is optional
	hdr, err := r.r.ReadBytes('\n')
	if err != nil {
		if err == io.EOF {
			r.done = true
		} else {
			r.err = err
		}
		return false
	}
	recSize := len(hdr)

	// for backwards compatibility, "--- " header is optional
	hdr = bytes.TrimPrefix(hdr, hdrPrefix)

	rest := hdr[:len(hdr)-1] // remove '\n' from end
	idx := bytes.IndexByte(rest, ' ')
	var dataSize []byte
	if idx == -1 {
		if !r.NoTimestamp {
			// with timestamp, we need at least 2 values separated by space
			r.err = fmt.Errorf("unexpected header '%s'", string(hdr))
			return false
		}
		dataSize = rest
		rest = nil
	} else {
		dataSize = rest[:idx]
		rest = rest[idx+1:]
	}
	var name []byte
	var timestamp []byte
	idx = bytes.IndexByte(rest, ' ')
	if idx == -1 {
		if r.NoTimestamp {
			// no timestamp, just name
			name = rest
		} else {
			// no name, just timestamp
			timestamp = rest
		}
	} else {
		// timestamp and name
		timestamp = rest[:idx]
		name = rest[idx+1:]
	}

	size, err := strconv.ParseInt(string(dataSize), 10, 64)
	if err != nil {
		r.err = fmt.Errorf("unexpected header '%s'", string(hdr))
		return false
	}

	if len(timestamp) > 0 {
		timeMs, err := strconv.ParseInt(string(timestamp), 10, 64)
		if err != nil {
			r.err = fmt.Errorf("unexpected header '%s'", string(hdr))
			return false
		}
		r.Timestamp = TimeFromUnixMillisecond(timeMs)
	}
	r.Name = string(name)

	// we try to re-use r.Data as long as it doesn't grow too much
	// (limit to 1 MB)
	if cap(r.Data) > 1024*1024 {
		r.Data = nil
	}
	if size > int64(cap(r.Data)) {
		r.Data = make([]byte, size)
	} else {
		// re-use existing buffer
		r.Data = r.Data[:size]
	}
	n, err := io.ReadFull(r.r, r.Data)
	if err != nil {
		r.err = err
		return false
	}
	panicIf(n != len(r.Data))
	recSize += n

	// account for the fact that for readability we might
	// have padded data with '\n'
	// same as needsNewline logic in Writer.Write
	n = len(r.Data)
	needsNewline := (n > 0) && (r.Data[n-1] != '\n')
	if needsNewline {
		_, err = r.r.Discard(1)
		if err != nil {
			r.err = err
			return false
		}
		recSize++
	}
	r.NextRecordPos += int64(recSize)
	return true
}

// ReadNextRecord reads a key / value record.
// Returns false if there are no more record.
// Check Err() for errors.
// After reading information is in Record (valid until
// next read).
func (r *Reader) ReadNextRecord() bool {
	ok := r.ReadNextData()
	if !ok {
		return false
	}

	_, r.err = UnmarshalRecord(r.Data, r.Record)
	if r.err != nil {
		return false
	}
	r.Record.Name = r.Name
	r.Record.Timestamp = r.Timestamp
	return true
}

// Err returns error from last Read. We swallow io.EOF to make it easier
// to use
func (r *Reader) Err() error {
	return r.err
}
