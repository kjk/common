package appendstore

import (
	"bytes"
	"math/rand"
	"os"
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
			Meta: "meta_" + string(rune('a'+rng.Intn(26))),
		}
		if i%33 == 0 {
			records[i].Meta = ""
		}
	}
	return records
}

func verifyRecord(t *testing.T, i int, rec *Record, record testRecord) {
	if rec.Kind != record.Kind {
		t.Errorf("Record %d: Kind mismatch, expected %s, got %s", i, record.Kind, rec.Kind)
	}
	if rec.Meta != record.Meta {
		t.Errorf("Record %d: Meta mismatch, expected %s, got %s", i, record.Meta, rec.Meta)
	}
	if rec.Length != int64(len(record.Data)) {
		t.Errorf("Record %d: Length mismatch, expected %d, got %d", i, len(record.Data), rec.Length)
	}
	if rec.Timestamp > time.Now().UTC().Unix() {
		t.Errorf("Record %d: Timestamp is in the future, got %d", i, rec.Timestamp)
	}
}

func TestStoreWriteAndRead(t *testing.T) {
	tempDir := "store_test_data"
	os.RemoveAll(tempDir)
	err := os.MkdirAll(tempDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	testRecords := genRandomRecords(1000)

	store := &Store{
		DataDir:       tempDir,
		IndexFileName: "index.txt",
		DataFileName:  "data.bin",
	}

	if err := OpenStore(store); err != nil {
		t.Fatalf("Failed to open store: %v", err)
	}

	// Test with newline in metadata
	_, err = store.AppendRecord("test_kind", []byte("test data"), "meta\nwith\nnewlines")
	if err == nil {
		t.Fatalf("Expected AppendRecord to reject metadata with newlines, got error: %v", err)
	}
	// Test kind with spaces
	_, err = store.AppendRecord("test kind", []byte("test data"), "meta")
	if err == nil {
		t.Fatalf("Expected AppendRecord to reject kind with spaces, got error: %v", err)
	}
	// Test empty kind
	_, err = store.AppendRecord("", []byte("test data"), "meta")
	if err == nil {
		t.Fatalf("Expected AppendRecord to reject empty kind, got error: %v", err)
	}
	// Test kind with newlines
	_, err = store.AppendRecord("test\nkind", []byte("test data"), "meta")
	if err == nil {
		t.Fatalf("Expected AppendRecord to reject kind with newlines, got error: %v", err)
	}
	// Verify no records were added
	if len(store.Records) != 0 {
		t.Fatalf("Expected no records to be added, got %d records", len(store.Records))
	}

	currOff := int64(0)
	for i, recTest := range testRecords {
		if i%25 == 0 {
			// make sure we're robust against appending non-indexed data
			// this is useful if AppendRecord() fails with partial write, without recording that in the index
			// we still want things to work if this happens
			d := []byte("lalalala\n")
			appendToFileRobust(store.dataFilePath, d)
			currOff += int64(len(d))
		}

		rec, err := store.AppendRecord(recTest.Kind, recTest.Data, recTest.Meta)
		if err != nil {
			t.Fatalf("Failed to append record: %v", err)
		}
		verifyRecord(t, i, rec, recTest)
		if rec.Length > 0 && rec.Offset != currOff {
			t.Fatalf("Record %d: Offset mismatch, expected %d, got %d", i, currOff, rec.Offset)
		}
		currOff += rec.Length
	}

	if len(store.Records) != 1000 {
		t.Fatalf("Expected 1000 records, got %d", len(store.Records))
	}

	// reopen the store
	if err := OpenStore(store); err != nil {
		t.Fatalf("Failed to open store: %v", err)
	}

	for i, recTest := range testRecords {
		rec := &store.Records[i]
		verifyRecord(t, i, rec, recTest)
		data, err := store.ReadRecord(rec)
		if err != nil {
			t.Fatalf("Failed to read record: %v", err)
		}
		if !bytes.Equal(data, recTest.Data) {
			t.Errorf("Record %d: Data mismatch, expected %s, got %s", i, recTest.Data, data)
		}
	}
}
