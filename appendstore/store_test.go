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
			err = store.appendToDataFile(d, 0)
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
	validateStore(t, store)
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
	d := []byte("lala\n")
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
	d = []byte("lalalalalala\n")
	store.OverwriteRecord(kind, meta, d)
	{
		rec := getLastRecord(store)
		recStr := serializeRecord(rec)
		assert(t, rec.Size == int64(len(d)), fmt.Sprintf("Expected record size %d, got %d\n%s", len(d), rec.Size, recStr))
		assert(t, rec.Size*2 == rec.SizeInFile, fmt.Sprintf("Expected record size in file %d, got %d\n%s", rec.Size*2, rec.SizeInFile, recStr))
		data, err := store.ReadRecord(rec)
		assert(t, err == nil, fmt.Sprintf("Failed to read record '%s', error: %v", recStr, err))
		assert(t, bytes.Equal(data, d), fmt.Sprintf("Record data mismatch, expected %s, got %s\n%s", d, data, recStr))
	}

	// smaller than first record so will overwrite it
	d = []byte("lolahi\n")
	store.OverwriteRecord(kind, meta, d)
	{
		rec := getLastRecord(store)
		assert(t, rec.Offset == rec1.Offset, fmt.Sprintf("Expected record offset %d, got %d", rec1.Offset, rec.Offset))
		data, err := store.ReadRecord(rec)
		assert(t, err == nil, fmt.Sprintf("Failed to read record: %v", err))
		assert(t, bytes.Equal(data, d), fmt.Sprintf("Record data mismatch, expected %s, got %s", d, data))
		assert(t, rec1.Overwritten == true, "Expected record to be marked as overwritten")
	}
	d = []byte("and a big one here boss\n")
	store.OverwriteRecord(kind, meta, d)
	validateStore(t, store)
	store.AppendRecord(kind, meta, d)
	validateStore(t, store)

	// verify overwritten records recognized when reading all records
	store = openStore(t, "overwrite_")
	validateStore(t, store)
	recs := store.Records()
	assert(t, len(recs) == 3, fmt.Sprintf("Expected 3 records, got %d", len(recs)))
	assert(t, len(store.allRecords) == 5, fmt.Sprintf("Expected 5 all records, got %d", len(store.allRecords)))
}

func validateStore(t *testing.T, store *Store) {
	if store == nil {
		t.Fatal("Store is nil")
	}
	dataPath := filepath.Join(store.DataDir, store.DataFileName)
	dataSize := int64(0)
	if st, err := os.Stat(dataPath); os.IsNotExist(err) {
		t.Fatalf("Data file %s does not exist", dataPath)
	} else {
		dataSize = st.Size()
	}
	recs := store.Records()
	for _, rec := range recs {
		recStr := serializeRecord(rec)
		if rec.Offset < 0 || rec.Size < 0 || rec.TimestampMs < 0 {
			t.Fatalf("Invalid record: %+v,\n%s", rec, recStr)
		}
		sz := rec.Size
		if rec.SizeInFile > 0 {
			sz = rec.SizeInFile
		}
		if rec.Offset+sz > dataSize {
			t.Fatalf("Record exceeds data file size: offset %d, size %d, off+size: %d, data size %d\n%s", rec.Offset, sz, rec.Offset+sz, dataSize, recStr)
		}
		if rec.SizeInFile > 0 && rec.SizeInFile < rec.Size {
			t.Fatalf("SizeInFile is less than Size: SizeInFile %d, Size %d\n%s", rec.SizeInFile, rec.Size, recStr)
		}
	}
}

func assert(t *testing.T, cond bool, msg string) {
	if !cond {
		panic(msg)
	}
}

func TestAppendRecordInline(t *testing.T) {
	store := createStore(t, "inline_")

	// Test basic inline record
	kind := "config"
	meta := "settings"
	data := []byte(`{"theme":"dark","fontSize":14}`)
	err := store.AppendRecordInline(kind, meta, data)
	assert(t, err == nil, fmt.Sprintf("Failed to append inline record: %v", err))

	rec := getLastRecord(store)
	assert(t, rec.Kind == kind, fmt.Sprintf("Expected kind %s, got %s", kind, rec.Kind))
	assert(t, rec.Meta == meta, fmt.Sprintf("Expected meta %s, got %s", meta, rec.Meta))
	assert(t, rec.Size == int64(len(data)), fmt.Sprintf("Expected size %d, got %d", len(data), rec.Size))
	assert(t, rec.DataInline == true, "Expected DataInline to be true")

	// Read back the data
	readData, err := store.ReadRecord(rec)
	assert(t, err == nil, fmt.Sprintf("Failed to read inline record: %v", err))
	assert(t, bytes.Equal(readData, data), fmt.Sprintf("Data mismatch, expected %s, got %s", data, readData))

	// Test with empty data
	err = store.AppendRecordInline("empty", "test", nil)
	assert(t, err == nil, fmt.Sprintf("Failed to append empty inline record: %v", err))
	recEmpty := getLastRecord(store)
	assert(t, recEmpty.Size == 0, fmt.Sprintf("Expected size 0, got %d", recEmpty.Size))
	assert(t, recEmpty.DataInline == true, "Expected DataInline to be true for empty record")

	// Test with validation errors
	err = store.AppendRecordInline("", meta, data)
	assert(t, err != nil, "Expected error for empty kind")
	err = store.AppendRecordInline("test kind", meta, data)
	assert(t, err != nil, "Expected error for kind with spaces")
	err = store.AppendRecordInline(kind, "meta\nwith\nnewlines", data)
	assert(t, err != nil, "Expected error for meta with newlines")

	// Mix inline and regular records
	regularData := []byte("regular record data")
	err = store.AppendRecord("regular", "rec1", regularData)
	assert(t, err == nil, fmt.Sprintf("Failed to append regular record: %v", err))
	recRegular := getLastRecord(store)
	assert(t, recRegular.DataInline == false, "Expected DataInline to be false for regular record")

	inlineData2 := []byte("another inline")
	err = store.AppendRecordInline("inline2", "rec2", inlineData2)
	assert(t, err == nil, fmt.Sprintf("Failed to append second inline record: %v", err))

	// Verify all records can be read correctly (config, empty, regular, inline2)
	recs := store.Records()
	assert(t, len(recs) == 4, fmt.Sprintf("Expected 4 records, got %d", len(recs)))

	// Reopen store and verify persistence
	err = store.CloseFiles()
	assert(t, err == nil, fmt.Sprintf("Failed to close store: %v", err))

	store2 := openStore(t, "inline_")
	recs2 := store2.Records()
	assert(t, len(recs2) == 4, fmt.Sprintf("Expected 4 records after reopen, got %d", len(recs2)))

	// Verify inline record data after reopen
	for _, rec := range recs2 {
		if rec.Kind == "config" {
			assert(t, rec.DataInline == true, "Expected DataInline to be true after reopen")
			readData, err := store2.ReadRecord(rec)
			assert(t, err == nil, fmt.Sprintf("Failed to read inline record after reopen: %v", err))
			assert(t, bytes.Equal(readData, data), fmt.Sprintf("Data mismatch after reopen, expected %s, got %s", data, readData))
		}
		if rec.Kind == "regular" {
			assert(t, rec.DataInline == false, "Expected DataInline to be false for regular record after reopen")
			readData, err := store2.ReadRecord(rec)
			assert(t, err == nil, fmt.Sprintf("Failed to read regular record after reopen: %v", err))
			assert(t, bytes.Equal(readData, regularData), fmt.Sprintf("Regular data mismatch, expected %s, got %s", regularData, readData))
		}
	}
}

func TestAppendRecordInlineWithTimestamp(t *testing.T) {
	store := createStore(t, "inline_ts_")

	kind := "log"
	meta := "entry1"
	data := []byte("log message here")
	customTs := int64(1704067200000) // 2024-01-01 00:00:00 UTC

	err := store.AppendRecordInlineWithTimestamp(kind, meta, data, customTs)
	assert(t, err == nil, fmt.Sprintf("Failed to append inline record with timestamp: %v", err))

	rec := getLastRecord(store)
	assert(t, rec.TimestampMs == customTs, fmt.Sprintf("Expected timestamp %d, got %d", customTs, rec.TimestampMs))
	assert(t, rec.DataInline == true, "Expected DataInline to be true")
	assert(t, rec.Kind == kind, fmt.Sprintf("Expected kind %s, got %s", kind, rec.Kind))

	readData, err := store.ReadRecord(rec)
	assert(t, err == nil, fmt.Sprintf("Failed to read record: %v", err))
	assert(t, bytes.Equal(readData, data), fmt.Sprintf("Data mismatch, expected %s, got %s", data, readData))

	// Test with zero timestamp (should use current time)
	err = store.AppendRecordInlineWithTimestamp("log", "entry2", []byte("another log"), 0)
	assert(t, err == nil, fmt.Sprintf("Failed to append inline record with zero timestamp: %v", err))
	rec2 := getLastRecord(store)
	assert(t, rec2.TimestampMs > 0, "Expected non-zero timestamp")
	assert(t, rec2.TimestampMs <= time.Now().UTC().UnixMilli(), "Expected timestamp not in the future")

	// Reopen and verify timestamp persistence
	store.CloseFiles()
	store2 := openStore(t, "inline_ts_")
	recs := store2.Records()
	assert(t, len(recs) == 2, fmt.Sprintf("Expected 2 records, got %d", len(recs)))
	assert(t, recs[0].TimestampMs == customTs, fmt.Sprintf("Expected custom timestamp %d after reopen, got %d", customTs, recs[0].TimestampMs))
}

func TestInlineRecordMultiple(t *testing.T) {
	store := createStore(t, "inline_multi_")

	// Add many inline records to test parsing robustness
	testData := []struct {
		kind string
		meta string
		data []byte
	}{
		{"type1", "meta1", []byte("data one")},
		{"type2", "meta2", []byte("data two with more content")},
		{"type3", "", []byte("no meta")},
		{"type4", "meta4", nil},
		{"type5", "meta with spaces", []byte("short")},
		{"type6", "meta6", []byte("a longer piece of data that spans more bytes")},
	}

	for _, td := range testData {
		err := store.AppendRecordInline(td.kind, td.meta, td.data)
		assert(t, err == nil, fmt.Sprintf("Failed to append inline record %s: %v", td.kind, err))
	}

	// Verify all records
	recs := store.Records()
	assert(t, len(recs) == len(testData), fmt.Sprintf("Expected %d records, got %d", len(testData), len(recs)))

	for i, td := range testData {
		rec := recs[i]
		assert(t, rec.Kind == td.kind, fmt.Sprintf("Record %d: kind mismatch", i))
		assert(t, rec.Meta == td.meta, fmt.Sprintf("Record %d: meta mismatch", i))
		assert(t, rec.DataInline == true, fmt.Sprintf("Record %d: expected DataInline true", i))
		readData, err := store.ReadRecord(rec)
		assert(t, err == nil, fmt.Sprintf("Record %d: failed to read: %v", i, err))
		assert(t, bytes.Equal(readData, td.data), fmt.Sprintf("Record %d: data mismatch", i))
	}

	// Reopen and verify
	store.CloseFiles()
	store2 := openStore(t, "inline_multi_")
	recs2 := store2.Records()
	assert(t, len(recs2) == len(testData), fmt.Sprintf("Expected %d records after reopen, got %d", len(testData), len(recs2)))

	for i, td := range testData {
		rec := recs2[i]
		assert(t, rec.Kind == td.kind, fmt.Sprintf("After reopen, record %d: kind mismatch", i))
		assert(t, rec.Meta == td.meta, fmt.Sprintf("After reopen, record %d: meta mismatch", i))
		assert(t, rec.DataInline == true, fmt.Sprintf("After reopen, record %d: expected DataInline true", i))
		readData, err := store2.ReadRecord(rec)
		assert(t, err == nil, fmt.Sprintf("After reopen, record %d: failed to read: %v", i, err))
		assert(t, bytes.Equal(readData, td.data), fmt.Sprintf("After reopen, record %d: data mismatch", i))
	}
}
