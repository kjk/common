package siser

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
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

// Record represents list of key/value pairs that can
// be serialized/deserialized
type Record struct {
	// Entries are available after Unmarshal/UnmarshalRecord
	Entries []Entry
	buf     strings.Builder
	Name    string
	// when writing, if not provided we use current time
	Timestamp time.Time
}

func (r *Record) appendKeyVal(key, val string) {
	e := Entry{
		Key:   key,
		Value: val,
	}
	r.Entries = append(r.Entries, e)
}

// Write writes key/value pairs to a record.
// After you write all key/value pairs, call Marshal()
// to get serialized value (valid until next call to Reset())
func (r *Record) Write(args ...string) {
	n := len(args)
	if n == 0 || n%2 != 0 {
		panic(fmt.Sprintf("Invalid number of args: %d", len(args)))
	}
	for i := 0; i < n; i += 2 {
		r.marshalKeyVal(args[i], args[i+1])
		// TODO: this is for api compat with older version
		// remove it and fix the tests
		r.appendKeyVal(args[i], args[i+1])
	}
}

// Reset makes it easy to re-use Record (as opposed to allocating a new one
// each time)
func (r *Record) Reset() {
	if r.Entries != nil {
		r.Entries = r.Entries[0:0]
	}
	r.Name = ""
	var t time.Time
	r.Timestamp = t
	r.buf.Reset()
}

// Get returns a value for a given key
func (r *Record) Get(key string) (string, bool) {
	for _, e := range r.Entries {
		if e.Key == key {
			return e.Value, true
		}
	}
	return "", false
}

func nonEmptyEndsWithNewline(s string) bool {
	n := len(s)
	return n == 0 || s[n-1] == '\n'
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
	return []byte(r.buf.String())
}

// UnmarshalRecord unmarshall record as marshalled with Record.Marshal
// For efficiency re-uses record r. If r is nil, will allocate new record.
func UnmarshalRecord(d []byte, r *Record) (*Record, error) {
	if r == nil {
		r = &Record{}
	} else {
		r.Reset()
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
			r.appendKeyVal(string(key), string(val))
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
		r.appendKeyVal(string(key), string(val))
	}
	return r, nil
}

// Unmarshal resets record and decodes data as created by Marshal
// into it.
func (r *Record) Unmarshal(d []byte) error {
	rec, err := UnmarshalRecord(d, r)
	panicIf(err == nil && rec == nil, "should return err or rec")
	panicIf(err != nil && rec != nil, "if error, rec should be nil")
	panicIf(rec != nil && rec != r, "if returned rec, must be same as r")
	return err
}
