package appendstore

import (
	"os"
	"path/filepath"
	"testing"
)

func openTestStore(t *testing.T, dir string) (*Store, *[]*Record, map[*Record][]byte) {
	s := &Store{DataDir: dir}
	if err := OpenStore(s); err != nil {
		t.Fatalf("OpenStore failed: %v", err)
	}
	records := s.Records()
	datas := map[*Record][]byte{}
	for _, rec := range records {
		d, err := s.ReadRecord(rec)
		if err != nil {
			t.Fatalf("ReadRecord failed: %v", err)
		}
		datas[rec] = d
	}
	return s, &records, datas
}

// simulate a crash during append by appending an incomplete record to
// index file and check that the store recovers on open
func TestRecoverFromIncompleteRecord(t *testing.T) {
	torn := []string{
		"_ 100 1700000000000 kind meta\nonly part of inline data", // truncated inline data
		"_ 5 1700000000000 ki", // partial index line
		"0 5 17000",            // partial index line
	}
	for _, tail := range torn {
		dir := t.TempDir()
		s, _, _ := openTestStore(t, dir)
		if err := s.AppendDataInline("k1", "m1", []byte("inline1")); err != nil {
			t.Fatal(err)
		}
		if err := s.AppendData("k2", "m2", []byte("data2")); err != nil {
			t.Fatal(err)
		}
		s.CloseFiles()

		indexPath := filepath.Join(dir, "index.txt")
		goodSize := fileSize(t, indexPath)
		f, err := os.OpenFile(indexPath, os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = f.WriteString(tail)
		f.Close()

		s, records, _ := openTestStore(t, dir)
		if len(*records) != 2 {
			t.Fatalf("tail %q: expected 2 records, got %d", tail, len(*records))
		}
		if got := fileSize(t, indexPath); got != goodSize {
			t.Fatalf("tail %q: expected index truncated to %d, got %d", tail, goodSize, got)
		}
		// new appends must work after recovery
		if err := s.AppendDataInline("k3", "m3", []byte("inline3")); err != nil {
			t.Fatal(err)
		}
		s.CloseFiles()

		s, records, datas := openTestStore(t, dir)
		if len(*records) != 3 {
			t.Fatalf("tail %q: expected 3 records after reopen, got %d", tail, len(*records))
		}
		last := (*records)[2]
		if last.Kind != "k3" || string(datas[last]) != "inline3" {
			t.Fatalf("tail %q: bad last record %+v, data %q", tail, last, datas[last])
		}
		d, err := s.ReadRecord((*records)[1])
		if err != nil || string(d) != "data2" {
			t.Fatalf("tail %q: ReadRecord: %q, %v", tail, d, err)
		}
		s.CloseFiles()
	}
}

// corruption in the middle of the index is still an error
func TestCorruptionInMiddleIsError(t *testing.T) {
	dir := t.TempDir()
	indexPath := filepath.Join(dir, "index.txt")
	err := os.WriteFile(indexPath, []byte("garbage line\n_ 3 1700000000000 k m\nabc\n"), 0644)
	if err != nil {
		t.Fatal(err)
	}
	s := &Store{DataDir: dir}
	if err := OpenStore(s); err == nil {
		t.Fatal("expected error for corrupted index")
	}
}

func fileSize(t *testing.T, path string) int64 {
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return st.Size()
}
