package appendstore

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type testRecord struct {
	Kind string
	Data []byte
	Meta string
}

var rng = rand.New(rand.NewSource(time.Now().UnixNano()))

func genRandomText(n int) []byte {
	if n == 0 {
		return nil
	}
	letters := []byte("abcdefghijklmnopqrstuvwxyz")
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	b[len(b)-1] = '\n' // for readability
	return b
}

func genRandomRecords(n int) []testRecord {
	records := make([]testRecord, n)
	for i := 0; i < n; i++ {
		records[i] = testRecord{
			Kind: "test_kind_" + string(rune('a'+rng.Intn(26))),
			Data: genRandomText(rng.Intn(1000)),
			Meta: "meta " + string(rune('a'+rng.Intn(26))),
		}
		if i%33 == 0 {
			records[i].Meta = ""
		}
	}
	return records
}

func verifyRecord(t *testing.T, i int, rec *Record, record testRecord) {
	assert(t, rec.Kind == record.Kind, fmt.Sprintf("Record %d: Kind mismatch, expected %s, got %s", i, record.Kind, rec.Kind))
	assert(t, rec.Meta == record.Meta, fmt.Sprintf("Record %d: Meta mismatch, expected %s, got %s", i, record.Meta, rec.Meta))
	assert(t, rec.Size == int64(len(record.Data)), fmt.Sprintf("Record %d: Length mismatch, expected %d, got %d", i, len(record.Data), rec.Size))
	assert(t, rec.TimestampMs <= time.Now().UTC().UnixMilli(), fmt.Sprintf("Record %d: Timestamp is in the future, got %d", i, rec.TimestampMs))
}

func TestParseIndexLine(t *testing.T) {
	var rec Record
	err := ParseIndexLine("123 456 789 test_kind meta data", &rec)
	assert(t, err == nil, fmt.Sprintf("ParseIndexLine failed: %v", err))

	assert(t, rec.Offset == 123, fmt.Sprintf("Expected Offset 123, got %d", rec.Offset))
	assert(t, rec.Size == 456, fmt.Sprintf("Expected Size 456, got %d", rec.Size))
	assert(t, rec.TimestampMs == 789, fmt.Sprintf("Expected TimestampMs 789, got %d", rec.TimestampMs))
	assert(t, rec.Kind == "test_kind", fmt.Sprintf("Expected Kind 'test_kind', got '%s'", rec.Kind))
	assert(t, rec.Meta == "meta data", fmt.Sprintf("Expected Meta 'meta data', got '%s'", rec.Meta))

	// test with SizeInFile
	err = ParseIndexLine("123 456:789 789 test_kind meta data", &rec)
	assert(t, err == nil, fmt.Sprintf("ParseIndexLine with SizeInFile failed: %v", err))
	assert(t, rec.Offset == 123, fmt.Sprintf("Expected Offset 123, got %d", rec.Offset))
	assert(t, rec.Size == 456, fmt.Sprintf("Expected Size 456, got %d", rec.Size))
	assert(t, rec.SizeInFile == 789, fmt.Sprintf("Expected SizeInFile 789, got %d", rec.SizeInFile))
	assert(t, rec.TimestampMs == 789, fmt.Sprintf("Expected TimestampMs 789, got %d", rec.TimestampMs))
	assert(t, rec.Kind == "test_kind", fmt.Sprintf("Expected Kind 'test_kind', got '%s'", rec.Kind))
	assert(t, rec.Meta == "meta data", fmt.Sprintf("Expected Meta 'meta data', got '%s'", rec.Meta))

	// Test with invalid line
	err = ParseIndexLine("invalid line", &rec)
	assert(t, err != nil, "Expected error for invalid index line, got nil")
}

func getLastRecord(store *Store) *Record {
	records := store.Records()
	return records[len(records)-1]
}

func TestStoreWriteAndRead(t *testing.T) {
	store := createStore(t, "test_")
	// Test with newline in metadata
	err := store.AppendRecord("test_kind", []byte("test data"), "meta\nwith\nnewlines")
	assert(t, err != nil, fmt.Sprintf("Expected AppendRecord to reject metadata with newlines, got error: %v", err))
	// Test kind with spaces
	err = store.AppendRecord("test kind", []byte("test data"), "meta")
	assert(t, err != nil, fmt.Sprintf("Expected AppendRecord to reject kind with spaces, got error: %v", err))
	// Test empty kind
	err = store.AppendRecord("", []byte("test data"), "meta")
	assert(t, err != nil, fmt.Sprintf("Expected AppendRecord to reject empty kind, got error: %v", err))
	// Test kind with newlines
	err = store.AppendRecord("test\nkind", []byte("test data"), "meta")
	assert(t, err != nil, fmt.Sprintf("Expected AppendRecord to reject kind with newlines, got error: %v", err))
	// Verify no records were added
	assert(t, len(store.Records()) == 0, fmt.Sprintf("Expected no records to be added, got %d records", len(store.Records())))

	testRecords := genRandomRecords(1000)
	currOff := int64(0)
	for i, recTest := range testRecords {
		if i%25 == 0 {
			// make sure we're robust against appending non-indexed data
			// this is useful if AppendRecord() fails with partial write, without recording that in the index
			// we still want things to work if this happens
			d := []byte("lalalala\n")
			appendToFileRobust(store.dataFilePath, d, 0)
			currOff += int64(len(d))
		}

		err := store.AppendRecord(recTest.Kind, recTest.Data, recTest.Meta)
		assert(t, err == nil, fmt.Sprintf("Failed to append record: %v", err))
		rec := getLastRecord(store)
		verifyRecord(t, i, rec, recTest)
		if rec.Size > 0 && rec.Offset != currOff {
			t.Fatalf("Record %d: Offset mismatch, expected %d, got %d", i, currOff, rec.Offset)
		}
		currOff += rec.Size
	}

	assert(t, len(store.Records()) == 1000, fmt.Sprintf("Expected 1000 records, got %d", len(store.Records())))

	// reopen the store
	err = OpenStore(store)
	assert(t, err == nil, fmt.Sprintf("Failed to open store: %v", err))

	recs := store.Records()
	for i, recTest := range testRecords {
		rec := recs[i]
		verifyRecord(t, i, rec, recTest)
		data, err := store.ReadRecord(rec)
		assert(t, err == nil, fmt.Sprintf("Failed to read record: %v", err))
		assert(t, bytes.Equal(data, recTest.Data), fmt.Sprintf("Record %d: Data mismatch, expected %s, got %s", i, recTest.Data, data))
	}
}

func createStore(t *testing.T, prefix string) *Store {
	tempDir := "store_test_data"
	err := os.MkdirAll(tempDir, 0755)
	assert(t, err == nil, fmt.Sprintf("Failed to create temp dir: %v", err))

	store := &Store{
		DataDir:       tempDir,
		IndexFileName: prefix + "index.txt",
		DataFileName:  prefix + "data.bin",
	}
	path := filepath.Join(tempDir, store.DataFileName)
	os.Remove(path)
	path = filepath.Join(tempDir, store.IndexFileName)
	os.Remove(path)

	err = OpenStore(store)
	assert(t, err == nil, fmt.Sprintf("Failed to open store: %v", err))
	return store
}

func TestRecordOverwrite(t *testing.T) {
	store := createStore(t, "overwrite_")
	store.OverWriteDataExpandPercent = 100
	kind := "file"
	meta := "foo.txt"
	d := []byte("lala")
	store.OverwriteRecord(kind, d, meta)
	rec1 := getLastRecord(store)
	{
		rec := rec1
		assert(t, rec.Kind == kind, fmt.Sprintf("Expected record kind %s, got %s", kind, rec.Kind))
		assert(t, rec.Meta == meta, fmt.Sprintf("Expected record meta %s, got %s", meta, rec.Meta))
		assert(t, rec.Size == int64(len(d)), fmt.Sprintf("Expected record size %d, got %d", len(d), rec.Size))
		assert(t, rec.Size*2 == rec.SizeInFile, fmt.Sprintf("Expected record size in file %d, got %d", rec.Size*2, rec.SizeInFile))
		data, err := store.ReadRecord(rec)
		assert(t, err == nil, fmt.Sprintf("Failed to read record: %v", err))
		assert(t, bytes.Equal(data, d), fmt.Sprintf("Record data mismatch, expected %s, got %s", d, data))
	}

	// bigger so will create new record because the size is greater than "lala" * 2
	d = []byte("lalalala2")
	store.OverwriteRecord(kind, d, meta)
	{
		rec := getLastRecord(store)
		assert(t, rec.Size == int64(len(d)), fmt.Sprintf("Expected record size %d, got %d", len(d), rec.Size))
		assert(t, rec.Size*2 == rec.SizeInFile, fmt.Sprintf("Expected record size in file %d, got %d", rec.Size*2, rec.SizeInFile))
		data, err := store.ReadRecord(rec)
		assert(t, err == nil, fmt.Sprintf("Failed to read record: %v", err))
		assert(t, bytes.Equal(data, d), fmt.Sprintf("Record data mismatch, expected %s, got %s", d, data))
	}

	// smaller than first record so will overwrite it
	d = []byte("lolahi")
	store.OverwriteRecord(kind, d, meta)
	{
		rec := getLastRecord(store)
		assert(t, rec.Offset == rec1.Offset, fmt.Sprintf("Expected record offset %d, got %d", rec1.Offset, rec.Offset))
		data, err := store.ReadRecord(rec)
		assert(t, err == nil, fmt.Sprintf("Failed to read record: %v", err))
		assert(t, bytes.Equal(data, d), fmt.Sprintf("Record data mismatch, expected %s, got %s", d, data))
	}

}

func assert(t *testing.T, cond bool, msg string) {
	if !cond {
		t.Fatal(msg)
	}
}
