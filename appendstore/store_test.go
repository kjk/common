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
	records := store.allRecords
	return records[len(records)-1]
}

func TestStoreWriteAndRead(t *testing.T) {
	store := createStore(t, "test_")
	// Test with newline in metadata
	err := store.AppendRecord("test_kind", "meta\nwith\nnewlines", []byte("test data"))
	assert(t, err != nil, fmt.Sprintf("Expected AppendRecord to reject metadata with newlines, got error: %v", err))
	// Test kind with spaces
	err = store.AppendRecord("test kind", "meta", []byte("test data"))
	assert(t, err != nil, fmt.Sprintf("Expected AppendRecord to reject kind with spaces, got error: %v", err))
	// Test empty kind
	err = store.AppendRecord("", "meta", []byte("test data"))
	assert(t, err != nil, fmt.Sprintf("Expected AppendRecord to reject empty kind, got error: %v", err))
	// Test kind with newlines
	err = store.AppendRecord("test\nkind", "meta", []byte("test data"))
	assert(t, err != nil, fmt.Sprintf("Expected AppendRecord to reject kind with newlines, got error: %v", err))
	// Verify no records were added
	assert(t, len(store.Records()) == 0, fmt.Sprintf("Expected no records to be added, got %d records", len(store.Records())))

	testRecords := genRandomRecords(1000)
	currOff := int64(0)
	for i, recTest := range testRecords {
		if i%13 == 0 {
			err = store.CloseFiles()
			assert(t, err == nil, fmt.Sprintf("Failed to close store files: %v", err))
		}
		if i%25 == 0 {
			// make sure we're robust against appending non-indexed data
			// this is useful if AppendRecord() fails with partial write, without recording that in the index
			// we still want things to work if this happens
			d := []byte("lalalala\n")
			err = store.appendToDataFile(d)
			assert(t, err == nil, fmt.Sprintf("Failed to append non-indexed data: %v", err))
			currOff += int64(len(d))
		}

		err = store.AppendRecord(recTest.Kind, recTest.Meta, recTest.Data)
		assert(t, err == nil, fmt.Sprintf("Failed to append record: %v", err))
		rec := getLastRecord(store)
		verifyRecord(t, i, rec, recTest)
		if rec.Size > 0 && rec.Offset != currOff {
			t.Fatalf("Record %d: Offset mismatch, expected %d, got %d", i, currOff, rec.Offset)
		}
		currOff += rec.Size
	}

	assert(t, len(store.Records()) == len(testRecords), fmt.Sprintf("Expected %d records, got %d", len(testRecords), len(store.Records())))

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

func openStore(t *testing.T, prefix string) *Store {
	tempDir := "test_data"
	store := &Store{
		DataDir:       tempDir,
		IndexFileName: prefix + "index.txt",
		DataFileName:  prefix + "data.bin",
	}
	err := OpenStore(store)
	assert(t, err == nil, fmt.Sprintf("Failed to open store: %v", err))
	return store
}

func createStore(t *testing.T, prefix string) *Store {
	tempDir := "test_data"
	err := os.MkdirAll(tempDir, 0755)
	assert(t, err == nil, fmt.Sprintf("Failed to create temp dir: %v", err))
	path := filepath.Join(tempDir, prefix+"data.bin")
	os.Remove(path)
	path = filepath.Join(tempDir, prefix+"index.txt")
	os.Remove(path)
	return openStore(t, prefix)
}

func TestRecordOverwrite(t *testing.T) {
	store := createStore(t, "overwrite_")
	store.OverWriteDataExpandPercent = 100
	kind := "file"
	meta := "foo.txt"
	d := []byte("lala")
	store.OverwriteRecord(kind, meta, d)
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
	store.OverwriteRecord(kind, meta, d)
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
	store.OverwriteRecord(kind, meta, d)
	{
		rec := getLastRecord(store)
		assert(t, rec.Offset == rec1.Offset, fmt.Sprintf("Expected record offset %d, got %d", rec1.Offset, rec.Offset))
		data, err := store.ReadRecord(rec)
		assert(t, err == nil, fmt.Sprintf("Failed to read record: %v", err))
		assert(t, bytes.Equal(data, d), fmt.Sprintf("Record data mismatch, expected %s, got %s", d, data))
		assert(t, rec1.Overwritten == true, "Expected record to be marked as overwritten")
	}

	// verify overwritten records recognized when reading all records
	store = openStore(t, "overwrite_")
	recs := store.Records()
	assert(t, len(recs) == 2, fmt.Sprintf("Expected 2 records, got %d", len(recs)))
	assert(t, len(store.allRecords) == 3, fmt.Sprintf("Expected 3 records, got %d", len(store.allRecords)))
}

func assert(t *testing.T, cond bool, msg string) {
	if !cond {
		panic(msg)
	}
}
