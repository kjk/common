package siser

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/kjk/common/assert"
)

var (
	largeValue      = ""
	serializedJSON  []byte
	serializedSiser []byte
)

func genLargeValue() {
	s := "0123456789"
	s += s // 20
	s += s // 40
	s += s // 80
	s += s // 160
	s += s // 320
	largeValue = s
}

func init() {
	genLargeValue()
	genSerializedSiser()
	genSerializedJSON()
}

func timeDiff(t1 time.Time, t2 time.Time) time.Duration {
	dur := t1.Sub(t2)
	if dur < 0 {
		dur = -dur
	}
	return dur
}

func timeDiffLessThanMs(t1 time.Time, t2 time.Time) bool {
	return timeDiff(t1, t2) < time.Millisecond
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

var (
	testNo int
)

// writes a valid test case for fuzzing corpus
// only if GEN_FUZZ_CORPUS env variable is set
func writeCorpus(d []byte) {
	if os.Getenv("GEN_FUZZ_CORPUS") == "" {
		return
	}
	dir := "fuzz_corpus"
	err := os.MkdirAll(dir, 0755)
	must(err)
	testNo++
	name := fmt.Sprintf("test%d.txt", testNo)
	path := filepath.Join(dir, name)
	err = os.WriteFile(path, d, 0644)
	must(err)
}

func TestUnmarshalErrors(t *testing.T) {
	invalidRecords := []string{
		"ha",
		"ha\n",
		"ha:\n",
		"ha:_\n",
		"ha:+32\nma",
		"ha:+2\nmara",
		"ha:+los\nma",
	}
	// test error paths in UnmarshalRecord
	for _, s := range invalidRecords {
		_, err := UnmarshalRecord([]byte(s), nil)
		assert.Error(t, err, "s: '%s'", s)
	}
}

func testWriterRoundTrip(t *testing.T, r *Record) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	n, err := w.WriteRecord(r)
	assert.NoError(t, err)
	d := buf.Bytes()
	assert.Equal(t, len(d), n)

	buf2 := bytes.NewBuffer(d)
	reader := NewReader(bufio.NewReader(buf2))
	ok := reader.ReadNextRecord()
	assert.True(t, ok)
	rec := reader.Record
	assert.Equal(t, rec.Name, r.Name)

	assert.True(t, r.Timestamp.IsZero() || timeDiffLessThanMs(rec.Timestamp, r.Timestamp), "rec.Timestamp: %s, r.Timestamp: %s, diff: %s", rec.Timestamp, r.Timestamp, timeDiff(rec.Timestamp, r.Timestamp))
}

// serialize and deserialize record and check the data is the same
func testRoundTrip(t *testing.T, r *Record) string {
	d := r.Marshal()
	rec, err := UnmarshalRecord(d, nil)
	assert.NoError(t, err)
	d2 := rec.Marshal()
	assert.Equal(t, d, d2)
	testWriterRoundTrip(t, r)

	if false {
		writeCorpus(d)
	}

	return string(d2)
}

type testRec struct {
	s    string
	name string
	pos  int
}

func mkTestRec(s string, name string) *testRec {
	return &testRec{
		s:    s,
		name: name,
	}
}

func writeData(t *testing.T, tests []*testRec) *bytes.Buffer {
	buf := &bytes.Buffer{}
	// fixed time so that we can
	unixNano := 5 * time.Second
	tm := time.Unix(0, int64(unixNano))
	w := NewWriter(buf)
	currPos := 0
	for _, test := range tests {
		test.pos = currPos
		n, err := w.Write([]byte(test.s), tm, test.name)
		assert.NoError(t, err)
		currPos += n
	}
	return buf
}

func readAndVerifyData(t *testing.T, buf *bytes.Buffer, tests []*testRec) int64 {
	unixNano := 5 * time.Second
	tm := time.Unix(0, int64(unixNano))
	r := NewReader(bufio.NewReader(buf))
	n := 0
	for n < len(tests) && r.ReadNextData() {
		test := tests[n]
		assert.Equal(t, test.s, string(r.Data))
		assert.Equal(t, test.name, string(r.Name))
		assert.True(t, r.Timestamp.Equal(tm))
		expPos := int64(test.pos)
		assert.Equal(t, expPos, r.CurrRecordPos)
		n++
	}
	assert.NoError(t, r.Err())
	return r.NextRecordPos
}

func TestWriter(t *testing.T) {
	tests := []*testRec{
		mkTestRec("hey\n", ""),
		mkTestRec("ho", "with name"),
	}
	exp := `--- 4 5000
hey
--- 2 5000 with name
ho
`
	buf := writeData(t, tests)
	d := buf.Bytes()
	assert.Equal(t, exp, string(d))

	readAndVerifyData(t, bytes.NewBuffer(d), tests)
}

func TestWriterBug(t *testing.T) {
	// we had a bug where file that starts with '\n' would cause problems
	// because of the padding we add in writer but didn't properly
	// account in reader
	tests := []*testRec{
		// "foo" ends with newline, so we won't add it when
		// writing a record
		mkTestRec("foo\n", "foo.txt"),
	}

	buf := writeData(t, tests)
	expPos := int64(buf.Len())
	buf.WriteString("\nstarts with new line")

	buf2 := bytes.NewBuffer(buf.Bytes())
	gotPos := readAndVerifyData(t, buf2, tests)
	assert.Equal(t, expPos, gotPos)
}

func TestRecordSerializeSimple(t *testing.T) {
	var r Record

	{
		d := r.Marshal()
		assert.Equal(t, 0, len(d))
	}

	r.Write("key", "val")
	d := r.Marshal()

	r2, err := UnmarshalRecord(d, nil)
	assert.NoError(t, err)

	{
		v, ok := r2.Get("key")
		assert.True(t, ok)
		assert.Equal(t, v, "val")
	}

	{
		v, ok := r2.Get("Key")
		assert.False(t, ok)
		assert.Equal(t, v, "")
	}

	{
		s := testRoundTrip(t, &r)
		assert.Equal(t, "key: val\n", s)
	}
}

func TestRecordSerializeSimple2(t *testing.T) {
	var r Record
	r.Write("k2", "a\nb")
	s := testRoundTrip(t, &r)
	assert.Equal(t, "k2:+3\na\nb\n", s)
	testWriterRoundTrip(t, &r)
}

func TestWriterNoTimestamp(t *testing.T) {
	tests := []struct {
		data string
		name string
		exp  string
	}{
		{
			data: "foo",
			name: "name",
			exp:  "--- 3 name\nfoo\n",
		},
		{
			data: "foo\n",
			name: "",
			exp:  "--- 4\nfoo\n",
		},
	}
	for _, test := range tests {
		var buf bytes.Buffer
		w := NewWriter(&buf)
		w.NoTimestamp = true
		var tm time.Time
		_, err := w.Write([]byte(test.data), tm, test.name)
		assert.Nil(t, err)
		got := buf.String()
		assert.Equal(t, test.exp, got)

		rbuf := bufio.NewReader(bytes.NewBufferString(got))
		r := NewReader(rbuf)
		r.NoTimestamp = true
		ok := r.ReadNextData()
		assert.True(t, ok)
		assert.Equal(t, string(r.Data), test.data)
		assert.Equal(t, r.Name, test.name)
	}
}

func TestRecordSerializeSimple3(t *testing.T) {
	var r Record
	r.Write("long key", largeValue)
	got := testRoundTrip(t, &r)
	exp := fmt.Sprintf("long key:+%d\n%s\n", len(largeValue), largeValue)
	assert.Equal(t, exp, got)
}

func testVals(t *testing.T, vals []any, exp string) {
	var r Record
	{
		for i := 0; i < len(vals); i += 2 {
			r.Write(vals[i], vals[i+1])
		}
		got := string(r.Marshal())
		assert.Equal(t, exp, got)
		r.Reset()
	}
	{
		r.Write(vals...)
		got := string(r.Marshal())
		assert.Equal(t, exp, got)
		r.Reset()
	}
}

func TestRecordSerializeSimple4(t *testing.T) {
	vals := []any{"k2", "a\nb", "", "no name", "bu", "gatti ", "no value", "", "bu", "  gatti"}
	exp := `k2:+3
a
b
: no name
bu: gatti{space}
no value:+0
bu:   gatti
`
	// stupid editors remove trailing spaces
	exp = strings.ReplaceAll(exp, "{space}", " ")
	testVals(t, vals, exp)
}

func TestRecordSerializeSimple5(t *testing.T) {
	vals := []any{3, true, false, 88.3, 8, 99}
	exp := `3: true
false: 88.3
8: 99
`
	// stupid editors remove trailing spaces
	exp = strings.ReplaceAll(exp, "{space}", " ")
	testVals(t, vals, exp)
}

func TestMany(t *testing.T) {
	testMany(t, "")
	testMany(t, "named")
}

func testMany(t *testing.T, name string) {
	var buf bytes.Buffer
	w := NewWriter(&buf)

	// we can't compare timestamp directly but as truncated to milliseconds
	now := time.Now()

	rec := &Record{}
	var positions []int64
	var currPos int64
	nRecs := 8
	for i := range nRecs {
		rec.Reset()
		rec.Name = name
		rec.Timestamp = now
		nRand := rand.Intn(1024)
		rec.Write("counter", strconv.Itoa(i), "random", strconv.Itoa(nRand))
		if i%12 == 0 {
			rec.Write("large", largeValue)
			// test a case where large value is last in the record as well
			// as being followed by another value
			if rand.Intn(1024) > 512 {
				rec.Write("after", "whatever")
			}
		}
		n, err := w.WriteRecord(rec)
		assert.Nil(t, err)
		positions = append(positions, currPos)
		currPos += int64(n)
	}

	f := bufio.NewReader(bytes.NewBuffer(buf.Bytes()))
	reader := NewReader(f)
	i := 0
	for reader.ReadNextRecord() {
		rec := reader.Record
		recPos := reader.CurrRecordPos
		assert.Equal(t, recPos, positions[i])
		if i < len(positions)-1 {
			nextRecPos := reader.NextRecordPos
			assert.Equal(t, nextRecPos, positions[i+1])
		}

		counter, ok := rec.Get("counter")
		assert.True(t, ok)
		exp := strconv.Itoa(i)
		assert.Equal(t, exp, counter)
		_, ok = rec.Get("random")
		assert.True(t, ok)
		assert.Equal(t, rec.Name, name)
		assert.True(t, timeDiffLessThanMs(rec.Timestamp, now), "timestamp: %s, now: %s", rec.Timestamp, now)
		i++
	}
	assert.NoError(t, reader.Err())
	assert.Equal(t, nRecs, i)
}

func TestWritePanics(t *testing.T) {
	rec := &Record{}
	assert.Error(t, rec.Write("foo"))
}

func TestCrashes(t *testing.T) {
	tests := []string{
		"/yBrZXk6Ky0yMAoA+f//+zA=",
		"/QA6Ky03OQowMQAAMw==",
	}
	for _, tc := range tests {
		d, err := base64.StdEncoding.DecodeString(tc)
		assert.NoError(t, err)
		_, _ = UnmarshalRecord(d, nil)
	}
}

var rec Record
var globalData []byte

type testRecJSON struct {
	URI       string        `json:"uri"`
	Code      int           `json:"code"`
	IP        string        `json:"ip"`
	Duration  time.Duration `json:"dur"`
	When      time.Time     `json:"when"`
	Size      int           `json:"size"`
	UserAgent string        `json:"ua"`
	Referer   string        `json:"referer"`
}

func BenchmarkSiserMarshalWriteMany(b *testing.B) {
	for n := 0; n < b.N; n++ {
		rec.Write("uri", "/atom.xml")
		rec.Write("code", 200)
		rec.Write("ip", "54.186.248.49")
		durMs := float64(1.41) / float64(time.Millisecond)
		durStr := strconv.FormatFloat(durMs, 'f', 2, 64)
		rec.Write("dur", durStr)
		rec.Write("when", time.Now().Format(time.RFC3339))
		rec.Write("size", 35286)
		rec.Write("ua", "Feedspot http://www.feedspot.com")
		rec.Write("referer", "http://blog.kowalczyk.info/feed")
		// assign to global to prevents optimizing the loop
		globalData = rec.Marshal()
	}
}

func BenchmarkSiserMarshalWriteSingle(b *testing.B) {
	for n := 0; n < b.N; n++ {
		durMs := float64(1.41) / float64(time.Millisecond)
		durStr := strconv.FormatFloat(durMs, 'f', 2, 64)
		rec.Write(
			"uri", "/atom.xml",
			"code", 200,
			"ip", "54.186.248.49",
			"dur", durStr,
			"when", time.Now().Format(time.RFC3339),
			"size", 35286,
			"ua", "Feedspot http://www.feedspot.com",
			"referer", "http://blog.kowalczyk.info/feed")
		// assign to global to prevents optimizing the loop
		globalData = rec.Marshal()
	}
}

func BenchmarkJSONMarshal(b *testing.B) {
	for n := 0; n < b.N; n++ {
		rec := testRecJSON{
			URI:       "/atom.xml",
			Code:      200,
			IP:        "54.186.248.49",
			Duration:  time.Microsecond * time.Duration(1410),
			When:      time.Now(),
			Size:      35286,
			UserAgent: "Feedspot http://www.feedspot.com",
			Referer:   "http://blog.kowalczyk.info/feed",
		}
		d, err := json.Marshal(rec)
		panicIfErr(err)
		// assign to global to prevents optimizing the loop
		globalData = d
	}
}

func genSerializedSiser() {
	var rec Record
	rec.Write("uri", "/atom.xml")
	rec.Write("code", 200)
	rec.Write("ip", "54.186.248.49")
	durMs := float64(1.41) / float64(time.Millisecond)
	durStr := strconv.FormatFloat(durMs, 'f', 2, 64)
	rec.Write("dur", durStr)
	rec.Write("when", time.Now().Format(time.RFC3339))
	rec.Write("size", strconv.Itoa(35286))
	rec.Write("ua", "Feedspot http://www.feedspot.com")
	rec.Write("referer", "http://blog.kowalczyk.info/feed")
	serializedSiser = rec.Marshal()
}

func genSerializedJSON() {
	rec := testRecJSON{
		URI:       "/atom.xml",
		Code:      200,
		IP:        "54.186.248.49",
		Duration:  time.Microsecond * time.Duration(1410),
		When:      time.Now(),
		Size:      35286,
		UserAgent: "Feedspot http://www.feedspot.com",
		Referer:   "http://blog.kowalczyk.info/feed",
	}
	d, err := json.Marshal(rec)
	panicIfErr(err)
	serializedJSON = d
}

func BenchmarkSiserUnmarshal(b *testing.B) {
	var rec ReadRecord
	var err error
	for n := 0; n < b.N; n++ {
		err = rec.Unmarshal(serializedSiser)
		panicIfErr(err)
	}
}

func BenchmarkJSONUnmarshal(b *testing.B) {
	var rec testRecJSON
	for n := 0; n < b.N; n++ {
		err := json.Unmarshal(serializedJSON, &rec)
		panicIfErr(err)
	}
}
