package siser

import (
	"bytes"
	"fmt"
	"strconv"
	"time"
)

/*
Serialize/Deserialize array of key/value pairs in a format that is easy
to serialize/parse and human-readable.

The basic format is line-oriented: "key: value\n"

When value is long (> 120 chars) or has \n in it, we serialize it as:
key:+$len\n
value\n
*/

type Entry struct {
	Key   string
	Value string
}

var zeroTime time.Time

// Record represents list of key/value pairs that can
// be serialized/deserialized
type Record struct {
	buf  bytes.Buffer
	Name string
	// when writing, if not provided we use current time
	Timestamp time.Time
}

type ReadRecord struct {
	Record
	// Entries are available after Unmarshal/UnmarshalRecord
	Entries []Entry
}

// perf: re-use buf
func toStr(v any, buf *[]byte) string {
	if s, ok := v.(string); ok {
		return s
	}
	*buf = (*buf)[:0]
	if i, ok := v.(int); ok {
		*buf = strconv.AppendInt(*buf, int64(i), 10)
		return string(*buf)
	}
	*buf = fmt.Appendf(*buf, "%v", v)
	return string(*buf)
}

// Write writes key/value pairs to a record.
// After you write all key/value pairs, call Marshal()
// to get serialized value (valid until next call to Reset())
func (r *Record) Write(args ...any) error {
	n := len(args)
	if n == 0 || n%2 != 0 {
		return fmt.Errorf("invalid number of args: %d. Should be multiple of 2", len(args))
	}

	var buf []byte
	for i := 0; i < n; i += 2 {
		k := toStr(args[i], &buf)
		v := toStr(args[i+1], &buf)
		r.marshalKeyVal(k, v)
	}
	return nil
}

// WriteNonEmpty is like Write but won't write records with empty values
func (r *Record) WriteNonEmpty(args ...string) error {
	n := len(args)
	if n == 0 || n%2 != 0 {
		return fmt.Errorf("invalid number of args: %d. Should be multiple of 2", len(args))
	}
	for i := 0; i < n; i += 2 {
		k := args[i]
		if len(k) == 0 {
			return fmt.Errorf("empty key")
		}
		v := args[i+1]
		if len(v) == 0 {
			continue
		}
		r.marshalKeyVal(k, v)
	}
	return nil
}

// Reset to re-use the record when writing for efficiency
// it doesn't reset Name because common use case
// is writing the same record type
func (r *Record) Reset() {
	// if Timestamp is zero time Writer.Write() will use current time
	// at the time of writing
	r.Timestamp = zeroTime
	r.buf.Reset()
}

func (r *ReadRecord) Reset() {
	r.Record.Reset()
	r.Name = ""
	r.Timestamp = zeroTime
	if r.Entries != nil {
		r.Entries = r.Entries[0:0]
	}
}

func get(entries []Entry, key string) (string, bool) {
	for _, e := range entries {
		if e.Key == key {
			return e.Value, true
		}
	}
	return "", false
}

// Get returns a value for a given key
func (r *ReadRecord) Get(key string) (string, bool) {
	return get(r.Entries, key)
}

func nonEmptyEndsWithNewline(s string) bool {
	n := len(s)
	return n == 0 || s[n-1] == '\n'
}

func serializableOnLine(s string) bool {
	n := len(s)
	for i := 0; i < n; i++ {
		b := s[i]
		if b < 32 || b > 127 {
			return false
		}
	}
	return true
}

// return true if value needs to be serialized in long,
// size-prefixed format
func needsLongFormat(s string) bool {
	return len(s) == 0 || len(s) > 120 || !serializableOnLine(s)
}

func (r *Record) marshalKeyVal(key, val string) {
	r.buf.WriteString(key)

	isLong := needsLongFormat(val)
	if isLong {
		r.buf.WriteString(":+")
		slen := strconv.Itoa(len(val))
		r.buf.WriteString(slen)
		r.buf.WriteByte('\n')
		r.buf.WriteString(val)
		// for readability: ensure a newline at the end so
		// that header record always appears on new line
		if !nonEmptyEndsWithNewline(val) {
			r.buf.WriteByte('\n')
		}
	} else {
		r.buf.WriteString(": ")
		r.buf.WriteString(val)
		r.buf.WriteByte('\n')
	}
}

// Marshal converts record to bytes
func (r *Record) Marshal() []byte {
	return r.buf.Bytes()
}

func (r *ReadRecord) Marshal() []byte {
	for _, e := range r.Entries {
		r.Record.Write(e.Key, e.Value)
	}
	return r.Record.Marshal()
}

// UnmarshalRecord unmarshall record as marshalled with Record.Marshal
// For efficiency re-uses record r. If r is nil, will allocate new record.
func UnmarshalRecord(d []byte, r *ReadRecord) (*ReadRecord, error) {
	if r == nil {
		r = &ReadRecord{}
	} else {
		r.Reset()
		r.Name = ""
	}

	appendKeyVal := func(key, val string) {
		e := Entry{
			Key:   key,
			Value: val,
		}
		r.Entries = append(r.Entries, e)
	}

	for len(d) > 0 {
		idx := bytes.IndexByte(d, '\n')
		if idx == -1 {
			return nil, fmt.Errorf("missing '\n' marking end of header in '%s'", string(d))
		}
		line := d[:idx]
		d = d[idx+1:]
		idx = bytes.IndexByte(line, ':')
		if idx == -1 {
			return nil, fmt.Errorf("line in unrecognized format: '%s'", line)
		}
		key := line[:idx]
		val := line[idx+1:]
		// at this point val must be at least one character (' ' or '+')
		if len(val) < 1 {
			return nil, fmt.Errorf("line in unrecognized format: '%s'", line)
		}
		kind := val[0]
		val = val[1:]
		if kind == ' ' {
			appendKeyVal(string(key), string(val))
			continue
		}

		if kind != '+' {
			return nil, fmt.Errorf("line in unrecognized format: '%s'", line)
		}

		n, err := strconv.Atoi(string(val))
		if err != nil {
			return nil, err
		}
		if n < 0 {
			return nil, fmt.Errorf("negative length %d of data", n)
		}
		if n > len(d) {
			return nil, fmt.Errorf("length of value %d greater than remaining data of size %d", n, len(d))
		}
		val = d[:n]
		d = d[n:]
		// encoder might put optional newline
		if len(d) > 0 && d[0] == '\n' {
			d = d[1:]
		}
		appendKeyVal(string(key), string(val))
	}
	return r, nil
}

// Unmarshal resets record and decodes data as created by Marshal
// into it.
func (r *ReadRecord) Unmarshal(d []byte) error {
	rec, err := UnmarshalRecord(d, r)
	// if those fails it's a bug in the library
	panicIf(err == nil && rec == nil, "should return err or rec")
	panicIf(err != nil && rec != nil, "if error, rec should be nil")
	panicIf(rec != nil && rec != r, "if returned rec, must be same as r")
	return err
}
