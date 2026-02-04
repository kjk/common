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
	// Test validation errors
	assertValidationErrors(t, func(kind, meta string, data []byte) error {
		return store.AppendData(kind, meta, data)
	})
	// Verify no records were added
	assert(t, len(store.Records()) == 0, fmt.Sprintf("Expected no records to be added, got %d records", len(store.Records())))

	testRecords := genRandomRecords(1000)
	currOff := int64(0)
	var err error
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

		err = store.AppendData(recTest.Kind, recTest.Meta, recTest.Data)
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
		// Skip inline and file records for data file size checks
		if rec.DataInline || rec.FileName != "" {
			continue
		}
		if rec.Offset+rec.Size > dataSize {
			t.Fatalf("Record exceeds data file size: offset %d, size %d, off+size: %d, data size %d\n%s", rec.Offset, rec.Size, rec.Offset+rec.Size, dataSize, recStr)
		}
	}
}

func assert(t *testing.T, cond bool, msg string) {
	if !cond {
		panic(msg)
	}
}

// reopenAndVerify closes the store, reopens it, and verifies the expected record count
func reopenAndVerify(t *testing.T, store *Store, prefix string, expectedCount int) *Store {
	err := store.CloseFiles()
	assert(t, err == nil, fmt.Sprintf("Failed to close store: %v", err))
	store2 := openStore(t, prefix)
	recs := store2.Records()
	assert(t, len(recs) == expectedCount, fmt.Sprintf("Expected %d records after reopen, got %d", expectedCount, len(recs)))
	return store2
}

// readAndVerifyData reads a record and verifies its data matches expected
func readAndVerifyData(t *testing.T, store *Store, rec *Record, expectedData []byte, context string) {
	readData, err := store.ReadRecord(rec)
	assert(t, err == nil, fmt.Sprintf("%s: Failed to read record: %v", context, err))
	assert(t, bytes.Equal(readData, expectedData), fmt.Sprintf("%s: Data mismatch", context))
}

// assertRecordType verifies the record's inline flag and fileName
func assertRecordType(t *testing.T, rec *Record, isInline bool, fileName string, context string) {
	assert(t, rec.DataInline == isInline, fmt.Sprintf("%s: Expected DataInline=%v, got %v", context, isInline, rec.DataInline))
	assert(t, rec.FileName == fileName, fmt.Sprintf("%s: Expected FileName=%q, got %q", context, fileName, rec.FileName))
}

// assertValidationErrors tests that the append function rejects invalid kind/meta values
func assertValidationErrors(t *testing.T, appendFunc func(kind, meta string, data []byte) error) {
	err := appendFunc("", "meta", []byte("data"))
	assert(t, err != nil, "Expected error for empty kind")
	err = appendFunc("test kind", "meta", []byte("data"))
	assert(t, err != nil, "Expected error for kind with spaces")
	err = appendFunc("kind\nnewline", "meta", []byte("data"))
	assert(t, err != nil, "Expected error for kind with newlines")
	err = appendFunc("kind", "meta\nwith\nnewlines", []byte("data"))
	assert(t, err != nil, "Expected error for meta with newlines")
}

func TestAppendRecordInline(t *testing.T) {
	store := createStore(t, "inline_")

	// Test basic inline record
	kind := "config"
	meta := "settings"
	data := []byte(`{"theme":"dark","fontSize":14}`)
	err := store.AppendDataInline(kind, meta, data)
	assert(t, err == nil, fmt.Sprintf("Failed to append inline record: %v", err))

	rec := getLastRecord(store)
	assert(t, rec.Kind == kind, fmt.Sprintf("Expected kind %s, got %s", kind, rec.Kind))
	assert(t, rec.Meta == meta, fmt.Sprintf("Expected meta %s, got %s", meta, rec.Meta))
	assert(t, rec.Size == int64(len(data)), fmt.Sprintf("Expected size %d, got %d", len(data), rec.Size))
	assertRecordType(t, rec, true, "", "config record")

	// Read back the data
	readAndVerifyData(t, store, rec, data, "config record")

	// Test with empty data
	err = store.AppendDataInline("empty", "test", nil)
	assert(t, err == nil, fmt.Sprintf("Failed to append empty inline record: %v", err))
	recEmpty := getLastRecord(store)
	assert(t, recEmpty.Size == 0, fmt.Sprintf("Expected size 0, got %d", recEmpty.Size))
	assertRecordType(t, recEmpty, true, "", "empty record")

	// Test with validation errors
	assertValidationErrors(t, func(kind, meta string, data []byte) error {
		return store.AppendDataInline(kind, meta, data)
	})

	// Mix inline and regular records
	regularData := []byte("regular record data")
	err = store.AppendData("regular", "rec1", regularData)
	assert(t, err == nil, fmt.Sprintf("Failed to append regular record: %v", err))
	recRegular := getLastRecord(store)
	assertRecordType(t, recRegular, false, "", "regular record")

	inlineData2 := []byte("another inline")
	err = store.AppendDataInline("inline2", "rec2", inlineData2)
	assert(t, err == nil, fmt.Sprintf("Failed to append second inline record: %v", err))

	// Verify all records can be read correctly (config, empty, regular, inline2)
	recs := store.Records()
	assert(t, len(recs) == 4, fmt.Sprintf("Expected 4 records, got %d", len(recs)))

	// Reopen store and verify persistence
	store2 := reopenAndVerify(t, store, "inline_", 4)
	recs2 := store2.Records()

	// Verify inline record data after reopen
	for _, rec := range recs2 {
		if rec.Kind == "config" {
			assertRecordType(t, rec, true, "", "config after reopen")
			readAndVerifyData(t, store2, rec, data, "config after reopen")
		}
		if rec.Kind == "regular" {
			assertRecordType(t, rec, false, "", "regular after reopen")
			readAndVerifyData(t, store2, rec, regularData, "regular after reopen")
		}
	}
}

func TestAppendRecordInlineWithTimestamp(t *testing.T) {
	store := createStore(t, "inline_ts_")

	kind := "log"
	meta := "entry1"
	data := []byte("log message here")
	customTs := int64(1704067200000) // 2024-01-01 00:00:00 UTC

	err := store.AppendDataInlineWithTimestamp(kind, meta, data, customTs)
	assert(t, err == nil, fmt.Sprintf("Failed to append inline record with timestamp: %v", err))

	rec := getLastRecord(store)
	assert(t, rec.TimestampMs == customTs, fmt.Sprintf("Expected timestamp %d, got %d", customTs, rec.TimestampMs))
	assertRecordType(t, rec, true, "", "log record")
	assert(t, rec.Kind == kind, fmt.Sprintf("Expected kind %s, got %s", kind, rec.Kind))

	readAndVerifyData(t, store, rec, data, "log record")

	// Test with zero timestamp (should use current time)
	err = store.AppendDataInlineWithTimestamp("log", "entry2", []byte("another log"), 0)
	assert(t, err == nil, fmt.Sprintf("Failed to append inline record with zero timestamp: %v", err))
	rec2 := getLastRecord(store)
	assert(t, rec2.TimestampMs > 0, "Expected non-zero timestamp")
	assert(t, rec2.TimestampMs <= time.Now().UTC().UnixMilli(), "Expected timestamp not in the future")

	// Reopen and verify timestamp persistence
	store2 := reopenAndVerify(t, store, "inline_ts_", 2)
	recs := store2.Records()
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
		err := store.AppendDataInline(td.kind, td.meta, td.data)
		assert(t, err == nil, fmt.Sprintf("Failed to append inline record %s: %v", td.kind, err))
	}

	// Verify all records
	recs := store.Records()
	assert(t, len(recs) == len(testData), fmt.Sprintf("Expected %d records, got %d", len(testData), len(recs)))

	for i, td := range testData {
		rec := recs[i]
		assert(t, rec.Kind == td.kind, fmt.Sprintf("Record %d: kind mismatch", i))
		assert(t, rec.Meta == td.meta, fmt.Sprintf("Record %d: meta mismatch", i))
		ctx := fmt.Sprintf("Record %d", i)
		assertRecordType(t, rec, true, "", ctx)
		readAndVerifyData(t, store, rec, td.data, ctx)
	}

	// Reopen and verify
	store2 := reopenAndVerify(t, store, "inline_multi_", len(testData))
	recs2 := store2.Records()

	for i, td := range testData {
		rec := recs2[i]
		assert(t, rec.Kind == td.kind, fmt.Sprintf("After reopen, record %d: kind mismatch", i))
		assert(t, rec.Meta == td.meta, fmt.Sprintf("After reopen, record %d: meta mismatch", i))
		ctx := fmt.Sprintf("After reopen, record %d", i)
		assertRecordType(t, rec, true, "", ctx)
		readAndVerifyData(t, store2, rec, td.data, ctx)
	}
}

func TestInlineRecordNewlineHandling(t *testing.T) {
	store := createStore(t, "inline_newline_")

	// Test data that ends with newline
	dataWithNewline := []byte("data ending with newline\n")
	err := store.AppendDataInline("with_newline", "meta1", dataWithNewline)
	assert(t, err == nil, fmt.Sprintf("Failed to append inline record with newline: %v", err))

	// Test data that does NOT end with newline
	dataWithoutNewline := []byte("data without newline")
	err = store.AppendDataInline("without_newline", "meta2", dataWithoutNewline)
	assert(t, err == nil, fmt.Sprintf("Failed to append inline record without newline: %v", err))

	// Test another record with newline to ensure parsing continues correctly
	dataWithNewline2 := []byte("another with newline\n")
	err = store.AppendDataInline("with_newline2", "meta3", dataWithNewline2)
	assert(t, err == nil, fmt.Sprintf("Failed to append second inline record with newline: %v", err))

	// Test another record without newline
	dataWithoutNewline2 := []byte("another without newline")
	err = store.AppendDataInline("without_newline2", "meta4", dataWithoutNewline2)
	assert(t, err == nil, fmt.Sprintf("Failed to append second inline record without newline: %v", err))

	// Verify all records before reopen
	recs := store.Records()
	assert(t, len(recs) == 4, fmt.Sprintf("Expected 4 records, got %d", len(recs)))

	// Verify data integrity
	readAndVerifyData(t, store, recs[0], dataWithNewline, "record 0")
	readAndVerifyData(t, store, recs[1], dataWithoutNewline, "record 1")
	readAndVerifyData(t, store, recs[2], dataWithNewline2, "record 2")
	readAndVerifyData(t, store, recs[3], dataWithoutNewline2, "record 3")

	// Reopen and verify persistence
	store2 := reopenAndVerify(t, store, "inline_newline_", 4)
	recs2 := store2.Records()

	// Verify data after reopen
	readAndVerifyData(t, store2, recs2[0], dataWithNewline, "record 0 after reopen")
	readAndVerifyData(t, store2, recs2[1], dataWithoutNewline, "record 1 after reopen")
	readAndVerifyData(t, store2, recs2[2], dataWithNewline2, "record 2 after reopen")
	readAndVerifyData(t, store2, recs2[3], dataWithoutNewline2, "record 3 after reopen")

	// Verify record sizes are correct (should match original data length)
	assert(t, recs2[0].Size == int64(len(dataWithNewline)), fmt.Sprintf("Record 0 size mismatch: expected %d, got %d", len(dataWithNewline), recs2[0].Size))
	assert(t, recs2[1].Size == int64(len(dataWithoutNewline)), fmt.Sprintf("Record 1 size mismatch: expected %d, got %d", len(dataWithoutNewline), recs2[1].Size))
	assert(t, recs2[2].Size == int64(len(dataWithNewline2)), fmt.Sprintf("Record 2 size mismatch: expected %d, got %d", len(dataWithNewline2), recs2[2].Size))
	assert(t, recs2[3].Size == int64(len(dataWithoutNewline2)), fmt.Sprintf("Record 3 size mismatch: expected %d, got %d", len(dataWithoutNewline2), recs2[3].Size))
}

func TestAppendRecordFile(t *testing.T) {
	store := createStore(t, "file_")

	// Test basic file record
	kind := "attachment"
	meta := "document.pdf"
	data := []byte("PDF content here")
	fileName := "doc1.dat"

	err := store.AppendFile(kind, meta, data, fileName)
	assert(t, err == nil, fmt.Sprintf("Failed to append file record: %v", err))

	rec := getLastRecord(store)
	assert(t, rec.Kind == kind, fmt.Sprintf("Expected kind %s, got %s", kind, rec.Kind))
	assert(t, rec.Meta == meta, fmt.Sprintf("Expected meta %s, got %s", meta, rec.Meta))
	assert(t, rec.Size == int64(len(data)), fmt.Sprintf("Expected size %d, got %d", len(data), rec.Size))
	assertRecordType(t, rec, false, fileName, "file record")

	// Read back the data
	readAndVerifyData(t, store, rec, data, "file record")

	// Verify the file exists on disk
	filePath := filepath.Join(store.DataDir, fileName)
	fileData, err := os.ReadFile(filePath)
	assert(t, err == nil, fmt.Sprintf("Failed to read file from disk: %v", err))
	assert(t, bytes.Equal(fileData, data), "File data mismatch")

	// Test error: fileName with space
	err = store.AppendFile("test", "meta", []byte("data"), "file name.dat")
	assert(t, err != nil, "Expected error for fileName with space")

	// Test error: empty fileName
	err = store.AppendFile("test", "meta", []byte("data"), "")
	assert(t, err != nil, "Expected error for empty fileName")

	// Test validation errors (kind, meta)
	err = store.AppendFile("", meta, data, "file2.dat")
	assert(t, err != nil, "Expected error for empty kind")
	err = store.AppendFile("test kind", meta, data, "file3.dat")
	assert(t, err != nil, "Expected error for kind with spaces")

	// Add another file record
	data2 := []byte("Another file content")
	fileName2 := "doc2.dat"
	err = store.AppendFile("attachment", "image.png", data2, fileName2)
	assert(t, err == nil, fmt.Sprintf("Failed to append second file record: %v", err))

	// Mix file and regular records
	regularData := []byte("regular record data")
	err = store.AppendData("regular", "rec1", regularData)
	assert(t, err == nil, fmt.Sprintf("Failed to append regular record: %v", err))

	// Verify all records
	recs := store.Records()
	assert(t, len(recs) == 3, fmt.Sprintf("Expected 3 records, got %d", len(recs)))

	// Reopen store and verify persistence
	store2 := reopenAndVerify(t, store, "file_", 3)
	recs2 := store2.Records()

	// Verify file record data after reopen
	for _, rec := range recs2 {
		if rec.Kind == "attachment" && rec.Meta == "document.pdf" {
			assertRecordType(t, rec, false, fileName, "file record after reopen")
			readAndVerifyData(t, store2, rec, data, "file record after reopen")
		}
		if rec.Kind == "regular" {
			assertRecordType(t, rec, false, "", "regular record after reopen")
			readAndVerifyData(t, store2, rec, regularData, "regular record after reopen")
		}
	}
}

func TestAppendRecordFileWithTimestamp(t *testing.T) {
	store := createStore(t, "file_ts_")

	kind := "backup"
	meta := "snapshot1"
	data := []byte("backup data here")
	fileName := "backup1.dat"
	customTs := int64(1704067200000) // 2024-01-01 00:00:00 UTC

	err := store.AppendFileWithTimestamp(kind, meta, data, fileName, customTs)
	assert(t, err == nil, fmt.Sprintf("Failed to append file record with timestamp: %v", err))

	rec := getLastRecord(store)
	assert(t, rec.TimestampMs == customTs, fmt.Sprintf("Expected timestamp %d, got %d", customTs, rec.TimestampMs))
	assertRecordType(t, rec, false, fileName, "file record with timestamp")

	readAndVerifyData(t, store, rec, data, "file record with timestamp")

	// Reopen and verify timestamp persistence
	store2 := reopenAndVerify(t, store, "file_ts_", 1)
	recs := store2.Records()
	assert(t, recs[0].TimestampMs == customTs, fmt.Sprintf("Expected timestamp %d after reopen, got %d", customTs, recs[0].TimestampMs))
	assertRecordType(t, recs[0], false, fileName, "file record after reopen")
}

func TestMixedRecordTypes(t *testing.T) {
	store := createStore(t, "mixed_")

	// Add different types of records
	regularData := []byte("regular data")
	err := store.AppendData("regular", "meta1", regularData)
	assert(t, err == nil, fmt.Sprintf("Failed to append regular record: %v", err))

	inlineData := []byte("inline data")
	err = store.AppendDataInline("inline", "meta2", inlineData)
	assert(t, err == nil, fmt.Sprintf("Failed to append inline record: %v", err))

	fileData := []byte("file data")
	err = store.AppendFile("file", "meta3", fileData, "mixed.dat")
	assert(t, err == nil, fmt.Sprintf("Failed to append file record: %v", err))

	// Verify all records
	recs := store.Records()
	assert(t, len(recs) == 3, fmt.Sprintf("Expected 3 records, got %d", len(recs)))

	// Verify each record type
	assertRecordType(t, recs[0], false, "", "regular record")
	assertRecordType(t, recs[1], true, "", "inline record")
	assertRecordType(t, recs[2], false, "mixed.dat", "file record")

	// Read all records
	readAndVerifyData(t, store, recs[0], regularData, "regular record")
	readAndVerifyData(t, store, recs[1], inlineData, "inline record")
	readAndVerifyData(t, store, recs[2], fileData, "file record")

	// Reopen and verify
	store2 := reopenAndVerify(t, store, "mixed_", 3)
	recs2 := store2.Records()

	assertRecordType(t, recs2[0], false, "", "regular after reopen")
	assertRecordType(t, recs2[1], true, "", "inline after reopen")
	assertRecordType(t, recs2[2], false, "mixed.dat", "file after reopen")

	readAndVerifyData(t, store2, recs2[0], regularData, "regular after reopen")
	readAndVerifyData(t, store2, recs2[1], inlineData, "inline after reopen")
	readAndVerifyData(t, store2, recs2[2], fileData, "file after reopen")
}
