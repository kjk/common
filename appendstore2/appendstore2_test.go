package appendstore2

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
	a(t, rec.Kind == record.Kind, "Record %d: Kind mismatch, expected %s, got %s", i, record.Kind, rec.Kind)
	a(t, rec.Meta() == record.Meta, "Record %d: Meta mismatch, expected %s, got %s", i, record.Meta, rec.Meta())
	a(t, rec.Size() == int64(len(record.Data)), "Record %d: Length mismatch, expected %d, got %d", i, len(record.Data), rec.Size())
	a(t, rec.TimestampMs <= time.Now().UTC().UnixMilli(), "Record %d: Timestamp is in the future, got %d", i, rec.TimestampMs)
}

func TestParseIndexLine(t *testing.T) {
	var rec Record
	err := ParseIndexLine("123 456 789 test_kind meta data", &rec)
	a(t, err == nil, "ParseIndexLine failed: %v", err)

	a(t, rec.Offset() == 123, "Expected Offset 123, got %d", rec.Offset())
	a(t, rec.Size() == 456, "Expected Size 456, got %d", rec.Size())
	a(t, rec.TimestampMs == 789, "Expected TimestampMs 789, got %d", rec.TimestampMs)
	a(t, rec.Kind == "test_kind", "Expected Kind 'test_kind', got '%s'", rec.Kind)
	a(t, rec.Meta() == "meta data", "Expected Meta 'meta data', got '%s'", rec.Meta())

	// Test with invalid line
	err = ParseIndexLine("invalid line", &rec)
	a(t, err != nil, "Expected error for invalid index line, got nil")
}

func getLastRecord(records []*Record) *Record {
	return records[len(records)-1]
}
func TestStoreWriteAndRead(t *testing.T) {
	var records []*Record
	onRecord := func(rec *Record, _ []byte) {
		records = append(records, rec)
	}
	store := createStore(t, "test_", onRecord)
	// Test with newline in metadata
	err := store.AppendData("test_kind", "meta\nwith\nnewlines", []byte("test data"))
	a(t, err != nil, "Expected AppendRecord to reject metadata with newlines, got error: %v", err)
	// Test kind with spaces
	err = store.AppendData("test kind", "meta", []byte("test data"))
	a(t, err != nil, "Expected AppendRecord to reject kind with spaces, got error: %v", err)
	// Test empty kind
	err = store.AppendData("", "meta", []byte("test data"))
	a(t, err != nil, "Expected AppendRecord to reject empty kind, got error: %v", err)
	// Test kind with newlines
	err = store.AppendData("test\nkind", "meta", []byte("test data"))
	a(t, err != nil, "Expected AppendRecord to reject kind with newlines, got error: %v", err)
	// Verify no records were added
	a(t, len(records) == 0, "Expected no records to be added, got %d records", len(records))

	testRecords := genRandomRecords(1000)
	currOff := int64(0)
	for i, recTest := range testRecords {
		if i%13 == 0 {
			err = store.CloseFiles()
			a(t, err == nil, "Failed to close store files: %v", err)
		}
		if i%25 == 0 {
			// make sure we're robust against appending non-indexed data
			// this is useful if AppendRecord() fails with partial write, without recording that in the index
			// we still want things to work if this happens
			d := []byte("lalalala\n")
			_, _, err = appendToFile(store.dataFilePath, &store.dataFile, d, store.SyncWrite)
			a(t, err == nil, "Failed to append non-indexed data: %v", err)
			currOff += int64(len(d))
		}

		err = store.AppendData(recTest.Kind, recTest.Meta, recTest.Data)
		a(t, err == nil, "Failed to append record: %v", err)
		rec := getLastRecord(records)
		verifyRecord(t, i, rec, recTest)
		if rec.Size() > 0 && rec.Offset() != currOff {
			t.Fatalf("Record %d: Offset mismatch, expected %d, got %d", i, currOff, rec.Offset())
		}
		currOff += rec.Size()
	}

	a(t, len(records) == len(testRecords), "Expected %d records, got %d", len(testRecords), len(records))

	// reopen the store
	err = OpenStore(store)
	a(t, err == nil, "Failed to open store: %v", err)

	recs := records
	for i, recTest := range testRecords {
		rec := recs[i]
		verifyRecord(t, i, rec, recTest)
		data, err := store.ReadRecord(rec)
		a(t, err == nil, "Failed to read record: %v", err)
		a(t, bytes.Equal(data, recTest.Data), "Record %d: Data mismatch, expected %s, got %s", i, recTest.Data, data)
	}
	validateStore(t, store, records)
}

func openStore(t *testing.T, prefix string, onRecord func(*Record, []byte)) *Store {
	tempDir := "test_data"
	store := &Store{
		DataDir:       tempDir,
		IndexFileName: prefix + "index.txt",
		DataFileName:  prefix + "data.bin",
		OnRecord:      onRecord,
	}
	err := OpenStore(store)
	a(t, err == nil, "Failed to open store: %v", err)
	return store
}

func createStore(t *testing.T, prefix string, onRecord func(*Record, []byte)) *Store {
	tempDir := "test_data"
	err := os.MkdirAll(tempDir, 0755)
	a(t, err == nil, "Failed to create temp dir: %v", err)
	path := filepath.Join(tempDir, prefix+"data.bin")
	os.Remove(path)
	path = filepath.Join(tempDir, prefix+"index.txt")
	os.Remove(path)
	return openStore(t, prefix, onRecord)
}

func validateStore(t *testing.T, store *Store, records []*Record) {
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
	recs := records
	for _, rec := range recs {
		recStr := serializeRecord(rec)
		if rec.TimestampMs < 0 {
			t.Fatalf("Invalid record: %+v,\n%s", rec, recStr)
		}
		// Skip inline and file records for data file size checks
		if rec.isInline() || rec.IsFile() {
			continue
		}
		if rec.Offset() < 0 {
			t.Fatalf("Invalid record offset: %+v,\n%s", rec, recStr)
		}
		if rec.Offset()+rec.Size() > dataSize {
			t.Fatalf("Record exceeds data file size: offset %d, size %d, off+size: %d, data size %d\n%s", rec.Offset(), rec.Size(), rec.Offset()+rec.Size(), dataSize, recStr)
		}
	}
}

func a(_ *testing.T, cond bool, format string, args ...any) {
	if !cond {
		msg := format
		if len(args) > 0 {
			msg = fmt.Sprintf(format, args...)
		}
		panic(msg)
	}
}

func TestAppendRecordInline(t *testing.T) {
	var records []*Record
	onRecord := func(rec *Record, _ []byte) {
		records = append(records, rec)
	}

	store := createStore(t, "inline_", onRecord)

	// Test basic inline record
	kind := "config"
	meta := "settings"
	data := []byte(`{"theme":"dark","fontSize":14}`)
	err := store.AppendDataInline(kind, meta, data)
	a(t, err == nil, "Failed to append inline record: %v", err)

	rec := getLastRecord(records)
	a(t, rec.Kind == kind, "Expected kind %s, got %s", kind, rec.Kind)
	a(t, rec.Meta() == meta, "Expected meta %s, got %s", meta, rec.Meta())
	a(t, rec.Size() == int64(len(data)), "Expected size %d, got %d", len(data), rec.Size())
	a(t, rec.isInline() == true, "Expected DataInline to be true")

	// Read back the data
	readData, err := store.ReadRecord(rec)
	a(t, err == nil, "Failed to read inline record: %v", err)
	a(t, bytes.Equal(readData, data), "Data mismatch, expected %s, got %s", data, readData)

	// Test with empty data
	err = store.AppendDataInline("empty", "test", nil)
	a(t, err == nil, "Failed to append empty inline record: %v", err)
	recEmpty := getLastRecord(records)
	a(t, recEmpty.Size() == 0, "Expected size 0, got %d", recEmpty.Size())
	a(t, recEmpty.isInline() == true, "Expected isInline to be true for empty record")

	// Test with validation errors
	err = store.AppendDataInline("", meta, data)
	a(t, err != nil, "Expected error for empty kind")
	err = store.AppendDataInline("test kind", meta, data)
	a(t, err != nil, "Expected error for kind with spaces")
	err = store.AppendDataInline(kind, "meta\nwith\nnewlines", data)
	a(t, err != nil, "Expected error for meta with newlines")

	// Mix inline and regular records
	regularData := []byte("regular record data")
	err = store.AppendData("regular", "rec1", regularData)
	a(t, err == nil, "Failed to append regular record: %v", err)
	recRegular := getLastRecord(records)
	a(t, recRegular.isInline() == false, "Expected DataInline to be false for regular record")

	inlineData2 := []byte("another inline")
	err = store.AppendDataInline("inline2", "rec2", inlineData2)
	a(t, err == nil, "Failed to append second inline record: %v", err)

	// Verify all records can be read correctly (config, empty, regular, inline2)
	recs := records
	a(t, len(recs) == 4, "Expected 4 records, got %d", len(recs))

	// Reopen store and verify persistence
	err = store.CloseFiles()
	a(t, err == nil, "Failed to close store: %v", err)

	var records2 []*Record
	onRecord2 := func(rec *Record, _ []byte) {
		records2 = append(records2, rec)
	}

	store2 := openStore(t, "inline_", onRecord2)
	recs2 := records2
	a(t, len(recs2) == 4, "Expected 4 records after reopen, got %d", len(recs2))

	// Verify inline record data after reopen
	for _, rec := range recs2 {
		if rec.Kind == "config" {
			a(t, rec.isInline() == true, "Expected DataInline to be true after reopen")
			readData, err := store2.ReadRecord(rec)
			a(t, err == nil, "Failed to read inline record after reopen: %v", err)
			a(t, bytes.Equal(readData, data), "Data mismatch after reopen, expected %s, got %s", data, readData)
		}
		if rec.Kind == "regular" {
			a(t, rec.isInline() == false, "Expected DataInline to be false for regular record after reopen")
			readData, err := store2.ReadRecord(rec)
			a(t, err == nil, "Failed to read regular record after reopen: %v", err)
			a(t, bytes.Equal(readData, regularData), "Regular data mismatch, expected %s, got %s", regularData, readData)
		}
	}
}

func TestAppendRecordInlineWithTimestamp(t *testing.T) {
	var records []*Record
	onRecord := func(rec *Record, _ []byte) {
		records = append(records, rec)
	}
	store := createStore(t, "inline_ts_", onRecord)

	kind := "log"
	meta := "entry1"
	data := []byte("log message here")
	customTs := int64(1704067200000) // 2024-01-01 00:00:00 UTC

	err := store.AppendDataInlineWithTimestamp(kind, meta, data, customTs)
	a(t, err == nil, "Failed to append inline record with timestamp: %v", err)

	rec := getLastRecord(records)
	a(t, rec.TimestampMs == customTs, "Expected timestamp %d, got %d", customTs, rec.TimestampMs)
	a(t, rec.isInline() == true, "Expected DataInline to be true")
	a(t, rec.Kind == kind, "Expected kind %s, got %s", kind, rec.Kind)

	readData, err := store.ReadRecord(rec)
	a(t, err == nil, "Failed to read record: %v", err)
	a(t, bytes.Equal(readData, data), "Data mismatch, expected %s, got %s", data, readData)

	// Test with zero timestamp (should use current time)
	err = store.AppendDataInlineWithTimestamp("log", "entry2", []byte("another log"), 0)
	a(t, err == nil, "Failed to append inline record with zero timestamp: %v", err)
	rec2 := getLastRecord(records)
	a(t, rec2.TimestampMs > 0, "Expected non-zero timestamp")
	a(t, rec2.TimestampMs <= time.Now().UTC().UnixMilli(), "Expected timestamp not in the future")

	// Reopen and verify timestamp persistence
	store.CloseFiles()
	var records2 []*Record
	onRecord2 := func(rec *Record, _ []byte) {
		records2 = append(records2, rec)
	}

	openStore(t, "inline_ts_", onRecord2)
	recs := records2
	a(t, len(recs) == 2, "Expected 2 records, got %d", len(recs))
	a(t, recs[0].TimestampMs == customTs, "Expected custom timestamp %d after reopen, got %d", customTs, recs[0].TimestampMs)
}

func TestInlineRecordMultiple(t *testing.T) {
	var records []*Record
	onRecord := func(rec *Record, _ []byte) {
		records = append(records, rec)
	}

	store := createStore(t, "inline_multi_", onRecord)

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
		a(t, err == nil, "Failed to append inline record %s: %v", td.kind, err)
	}

	// Verify all records
	recs := records
	a(t, len(recs) == len(testData), "Expected %d records, got %d", len(testData), len(recs))

	for i, td := range testData {
		rec := recs[i]
		a(t, rec.Kind == td.kind, "Record %d: kind mismatch", i)
		a(t, rec.Meta() == td.meta, "Record %d: meta mismatch", i)
		// Note: zero-length inline records can't be distinguished since -0 == 0
		if len(td.data) > 0 {
			a(t, rec.isInline() == true, "Record %d: expected inline true", i)
		}
		readData, err := store.ReadRecord(rec)
		a(t, err == nil, "Record %d: failed to read: %v", i, err)
		a(t, bytes.Equal(readData, td.data), "Record %d: data mismatch", i)
	}

	// Reopen and verify
	store.CloseFiles()
	var records2 []*Record
	onRecord2 := func(rec *Record, _ []byte) {
		records2 = append(records2, rec)
	}
	store2 := openStore(t, "inline_multi_", onRecord2)
	recs2 := records2
	a(t, len(recs2) == len(testData), "Expected %d records after reopen, got %d", len(testData), len(recs2))

	for i, td := range testData {
		rec := recs2[i]
		a(t, rec.Kind == td.kind, "After reopen, record %d: kind mismatch", i)
		a(t, rec.Meta() == td.meta, "After reopen, record %d: meta mismatch", i)
		// Note: zero-length inline records can't be distinguished since -0 == 0
		if len(td.data) > 0 {
			a(t, rec.isInline() == true, "After reopen, record %d: expected inline true", i)
		}
		readData, err := store2.ReadRecord(rec)
		a(t, err == nil, "After reopen, record %d: failed to read: %v", i, err)
		a(t, bytes.Equal(readData, td.data), "After reopen, record %d: data mismatch", i)
	}
}

func TestInlineRecordNewlineHandling(t *testing.T) {
	var records []*Record
	onRecord := func(rec *Record, _ []byte) {
		records = append(records, rec)
	}
	store := createStore(t, "inline_newline_", onRecord)

	// Test data that ends with newline
	dataWithNewline := []byte("data ending with newline\n")
	err := store.AppendDataInline("with_newline", "meta1", dataWithNewline)
	a(t, err == nil, "Failed to append inline record with newline: %v", err)

	// Test data that does NOT end with newline
	dataWithoutNewline := []byte("data without newline")
	err = store.AppendDataInline("without_newline", "meta2", dataWithoutNewline)
	a(t, err == nil, "Failed to append inline record without newline: %v", err)

	// Test another record with newline to ensure parsing continues correctly
	dataWithNewline2 := []byte("another with newline\n")
	err = store.AppendDataInline("with_newline2", "meta3", dataWithNewline2)
	a(t, err == nil, "Failed to append second inline record with newline: %v", err)

	// Test another record without newline
	dataWithoutNewline2 := []byte("another without newline")
	err = store.AppendDataInline("without_newline2", "meta4", dataWithoutNewline2)
	a(t, err == nil, "Failed to append second inline record without newline: %v", err)

	// Verify all records before reopen
	recs := records
	a(t, len(recs) == 4, "Expected 4 records, got %d", len(recs))

	// Verify data integrity
	d1, _ := store.ReadRecord(recs[0])
	a(t, bytes.Equal(d1, dataWithNewline), "Data with newline mismatch")
	d2, _ := store.ReadRecord(recs[1])
	a(t, bytes.Equal(d2, dataWithoutNewline), "Data without newline mismatch")
	d3, _ := store.ReadRecord(recs[2])
	a(t, bytes.Equal(d3, dataWithNewline2), "Second data with newline mismatch")
	d4, _ := store.ReadRecord(recs[3])
	a(t, bytes.Equal(d4, dataWithoutNewline2), "Second data without newline mismatch")

	// Reopen and verify persistence
	store.CloseFiles()

	var records2 []*Record
	onRecord2 := func(rec *Record, _ []byte) {
		records2 = append(records2, rec)
	}
	store2 := openStore(t, "inline_newline_", onRecord2)
	recs2 := records2
	a(t, len(recs2) == 4, "Expected 4 records after reopen, got %d", len(recs2))

	// Verify data after reopen
	d1, err = store2.ReadRecord(recs2[0])
	a(t, err == nil, "Failed to read record 0 after reopen: %v", err)
	a(t, bytes.Equal(d1, dataWithNewline), "Data with newline mismatch after reopen")

	d2, err = store2.ReadRecord(recs2[1])
	a(t, err == nil, "Failed to read record 1 after reopen: %v", err)
	a(t, bytes.Equal(d2, dataWithoutNewline), "Data without newline mismatch after reopen")

	d3, err = store2.ReadRecord(recs2[2])
	a(t, err == nil, "Failed to read record 2 after reopen: %v", err)
	a(t, bytes.Equal(d3, dataWithNewline2), "Second data with newline mismatch after reopen")

	d4, err = store2.ReadRecord(recs2[3])
	a(t, err == nil, "Failed to read record 3 after reopen: %v", err)
	a(t, bytes.Equal(d4, dataWithoutNewline2), "Second data without newline mismatch after reopen")

	// Verify record sizes are correct (should match original data length)
	a(t, recs2[0].Size() == int64(len(dataWithNewline)), "Record 0 size mismatch: expected %d, got %d", len(dataWithNewline), recs2[0].Size())
	a(t, recs2[1].Size() == int64(len(dataWithoutNewline)), "Record 1 size mismatch: expected %d, got %d", len(dataWithoutNewline), recs2[1].Size())
	a(t, recs2[2].Size() == int64(len(dataWithNewline2)), "Record 2 size mismatch: expected %d, got %d", len(dataWithNewline2), recs2[2].Size())
	a(t, recs2[3].Size() == int64(len(dataWithoutNewline2)), "Record 3 size mismatch: expected %d, got %d", len(dataWithoutNewline2), recs2[3].Size())
}

func TestAppendRecordFile(t *testing.T) {
	var records []*Record
	onRecord := func(rec *Record, _ []byte) {
		records = append(records, rec)
	}
	store := createStore(t, "file_", onRecord)

	// Test basic file record
	kind := "attachment"
	data := []byte("PDF content here")
	meta := []byte("this is meta")
	fileName := "doc1.dat"

	err := store.AppendFile(kind, fileName, data, meta)
	a(t, err == nil, "Failed to append file record: %v", err)

	rec := getLastRecord(records)
	a(t, rec.Kind == kind, "Expected kind %s, got %s", kind, rec.Kind)
	a(t, rec.Meta() == "", "Expected empty meta for file record, got %s", rec.Meta())
	a(t, rec.Size() == int64(len(meta)), "Expected size %d, got %d", len(data), rec.Size())
	fsize, err := store.FileSize(rec)
	a(t, err == nil, "Failed to get file size: %v", err)
	a(t, fsize == int64(len(data)), "Expected size %d, got %d", len(data), rec.Size())
	a(t, rec.FileName() == fileName, "Expected fileName %s, got %s", fileName, rec.FileName())
	a(t, rec.IsFile() == true, "Expected IsFile to be true")
	a(t, rec.isInline() == true, "Expected isInline to be false")

	// Read back the data
	readData, err := store.ReadRecord(rec)
	a(t, err == nil, "Failed to read file record: %v", err)
	a(t, bytes.Equal(readData, meta), "Data mismatch, expected '%s', got '%s'", meta, readData)
	fileData, err := store.ReadFile(rec)
	a(t, err == nil, "Failed to read file record: %v", err)
	a(t, bytes.Equal(fileData, data), "Data mismatch, expected %s, got %s", data, readData)

	// Verify the file exists on disk
	filePath := filepath.Join(store.DataDir, fileName)
	fileData, err = os.ReadFile(filePath)
	a(t, err == nil, "Failed to read file from disk: %v", err)
	a(t, bytes.Equal(fileData, data), "File data mismatch")

	// Test error: empty fileName
	err = store.AppendFile("test", "", []byte("data"), nil)
	a(t, err != nil, "Expected error for empty fileName")

	// Test validation errors (kind)
	err = store.AppendFile("", "file2.dat", data, nil)
	a(t, err != nil, "Expected error for empty kind")
	err = store.AppendFile("test kind", "file3.dat", data, nil)
	a(t, err != nil, "Expected error for kind with spaces")

	// Add another file record
	data2 := []byte("Another file content")
	fileName2 := "doc2.dat"
	err = store.AppendFile("attachment", fileName2, data2, nil)
	a(t, err == nil, "Failed to append second file record: %v", err)

	// Mix file and regular records
	regularData := []byte("regular record data")
	err = store.AppendData("regular", "rec1", regularData)
	a(t, err == nil, "Failed to append regular record: %v", err)

	// Verify all records
	recs := records
	a(t, len(recs) == 3, "Expected 3 records, got %d", len(recs))

	// Reopen store and verify persistence
	err = store.CloseFiles()
	a(t, err == nil, "Failed to close store: %v", err)

	var records2 []*Record
	onRecord2 := func(rec *Record, _ []byte) {
		records2 = append(records2, rec)
	}

	store2 := openStore(t, "file_", onRecord2)
	recs2 := records2
	a(t, len(recs2) == 3, "Expected 3 records after reopen, got %d", len(recs2))

	// Verify file record data after reopen
	for _, rec := range recs2 {
		if rec.Kind == "attachment" && rec.FileName() == fileName {
			a(t, rec.IsFile() == true, "Expected IsFile to be true after reopen")
			readData, err := store2.ReadFile(rec)
			a(t, err == nil, "Failed to read file record after reopen: %v", err)
			a(t, bytes.Equal(readData, data), "Data mismatch after reopen")
		}
		if rec.Kind == "regular" {
			a(t, rec.FileName() == "", "Expected empty fileName for regular record")
			a(t, rec.IsFile() == false, "Expected IsFile to be false for regular record")
			readData, err := store2.ReadRecord(rec)
			a(t, err == nil, "Failed to read regular record after reopen: %v", err)
			a(t, bytes.Equal(readData, regularData), "Regular data mismatch")
		}
	}
}

func TestAppendRecordFileWithTimestamp(t *testing.T) {
	var records []*Record
	onRecord := func(rec *Record, _ []byte) {
		records = append(records, rec)
	}
	store := createStore(t, "file_ts_", onRecord)

	kind := "backup"
	data := []byte("backup data here")
	fileName := "backup1.dat"
	customTs := int64(1704067200000) // 2024-01-01 00:00:00 UTC

	meta := []byte("meta")
	err := store.AppendFileWithTimestamp(kind, fileName, data, meta, customTs)
	a(t, err == nil, "Failed to append file record with timestamp: %v", err)

	rec := getLastRecord(records)
	a(t, rec.TimestampMs == customTs, "Expected timestamp %d, got %d", customTs, rec.TimestampMs)
	a(t, rec.FileName() == fileName, "Expected fileName %s, got %s", fileName, rec.FileName())
	a(t, rec.IsFile() == true, "Expected IsFile to be true")

	readMeta, err := store.ReadRecord(rec)
	a(t, err == nil, "Failed to read record: %v", err)
	a(t, bytes.Equal(readMeta, meta), "metadata mismatch")
	fileData, err := store.ReadFile(rec)
	a(t, err == nil, "store.ReadFile() failed with %v", err)
	a(t, bytes.Equal(fileData, data), "Data mismatch")

	// Reopen and verify timestamp persistence
	store.CloseFiles()
	var records2 []*Record
	onRecord2 := func(rec *Record, _ []byte) {
		records2 = append(records2, rec)
	}
	openStore(t, "file_ts_", onRecord2)
	recs := records2
	a(t, len(recs) == 1, "Expected 1 record, got %d", len(recs))
	a(t, recs[0].TimestampMs == customTs, "Expected timestamp %d after reopen, got %d", customTs, recs[0].TimestampMs)
	a(t, recs[0].FileName() == fileName, "Expected fileName %s after reopen, got %s", fileName, recs[0].FileName())
	a(t, recs[0].IsFile() == true, "Expected IsFile to be true after reopen")
}

func TestMixedRecordTypes(t *testing.T) {
	var records []*Record
	onRecord := func(rec *Record, _ []byte) {
		records = append(records, rec)
	}
	store := createStore(t, "mixed_", onRecord)

	// Add different types of records
	regularData := []byte("regular data")
	err := store.AppendData("regular", "meta1", regularData)
	a(t, err == nil, "Failed to append regular record: %v", err)

	inlineData := []byte("inline data")
	err = store.AppendDataInline("inline", "meta2", inlineData)
	a(t, err == nil, "Failed to append inline record: %v", err)

	fileData := []byte("file data")
	err = store.AppendFile("file", "mixed.dat", fileData, nil)
	a(t, err == nil, "Failed to append file record: %v", err)

	// Verify all records
	recs := records
	a(t, len(recs) == 3, "Expected 3 records, got %d", len(recs))

	// Verify each record type
	a(t, recs[0].isInline() == false && !recs[0].IsFile(), "Record 0 should be regular")
	a(t, recs[1].isInline() == true && !recs[1].IsFile(), "Record 1 should be inline")
	a(t, recs[2].IsFile() == true && recs[2].FileName() == "mixed.dat", "Record 2 should be file")

	// Read all records
	d1, _ := store.ReadRecord(recs[0])
	a(t, bytes.Equal(d1, regularData), "Regular data mismatch")

	d2, _ := store.ReadRecord(recs[1])
	a(t, bytes.Equal(d2, inlineData), "Inline data mismatch")

	d3, _ := store.ReadFile(recs[2])
	a(t, bytes.Equal(d3, fileData), "File data mismatch")

	// Reopen and verify
	store.CloseFiles()
	var records2 []*Record
	onRecord2 := func(rec *Record, _ []byte) {
		records2 = append(records2, rec)
	}

	store2 := openStore(t, "mixed_", onRecord2)
	recs2 := records2
	a(t, len(recs2) == 3, "Expected 3 records after reopen, got %d", len(recs2))

	a(t, recs2[0].isInline() == false && !recs2[0].IsFile(), "After reopen: record 0 should be regular")
	a(t, recs2[1].isInline() == true && !recs2[1].IsFile(), "After reopen: record 1 should be inline")
	a(t, recs2[2].IsFile() == true && recs2[2].FileName() == "mixed.dat", "After reopen: record 2 should be file")

	d1, _ = store2.ReadRecord(recs2[0])
	a(t, bytes.Equal(d1, regularData), "After reopen: regular data mismatch")

	d2, _ = store2.ReadRecord(recs2[1])
	a(t, bytes.Equal(d2, inlineData), "After reopen: inline data mismatch")

	d3, _ = store2.ReadFile(recs2[2])
	a(t, bytes.Equal(d3, fileData), "After reopen: file data mismatch")
}

func TestFileWithSpacesInName(t *testing.T) {
	var records []*Record
	onRecord := func(rec *Record, _ []byte) {
		records = append(records, rec)
	}
	store := createStore(t, "file_spaces_", onRecord)

	// Test file with spaces in name
	fileName := "my document file.txt"
	data := []byte("content of file with spaces in name")

	err := store.AppendFile("doc", fileName, data, nil)
	a(t, err == nil, "Failed to append file with spaces: %v", err)

	rec := getLastRecord(records)
	a(t, rec.FileName() == fileName, "Expected fileName '%s', got '%s'", fileName, rec.FileName())
	a(t, rec.IsFile() == true, "Expected IsFile to be true")

	// Read back the data
	readData, err := store.ReadFile(rec)
	a(t, err == nil, "Failed to read file record: %v", err)
	a(t, bytes.Equal(readData, data), "Data mismatch")

	// Add another file with multiple spaces
	fileName2 := "another file   with   multiple spaces.dat"
	data2 := []byte("more content here")
	err = store.AppendFile("doc", fileName2, data2, nil)
	a(t, err == nil, "Failed to append second file with spaces: %v", err)

	// Reopen and verify persistence
	store.CloseFiles()
	var records2 []*Record
	onRecord2 := func(rec *Record, _ []byte) {
		records2 = append(records2, rec)
	}
	store2 := openStore(t, "file_spaces_", onRecord2)

	a(t, len(records2) == 2, "Expected 2 records after reopen, got %d", len(records2))

	// Verify first file
	a(t, records2[0].FileName() == fileName, "After reopen: expected fileName '%s', got '%s'", fileName, records2[0].FileName())
	d1, err := store2.ReadFile(records2[0])
	a(t, err == nil, "Failed to read first file after reopen: %v", err)
	a(t, bytes.Equal(d1, data), "First file data mismatch after reopen")

	// Verify second file
	a(t, records2[1].FileName() == fileName2, "After reopen: expected fileName '%s', got '%s'", fileName2, records2[1].FileName())
	d2, err := store2.ReadFile(records2[1])
	a(t, err == nil, "Failed to read second file after reopen: %v", err)
	a(t, bytes.Equal(d2, data2), "Second file data mismatch after reopen")
}
