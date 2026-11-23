package atomicfile

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func writeAtomic(t *testing.T, path string, s string) {
	f, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.RemoveIfNotClosed()
	if _, err = f.Write([]byte(s)); err != nil {
		t.Fatal(err)
	}
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestPermissions(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("unix permissions not supported on windows")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "new.txt")
	writeAtomic(t, path, "hello")
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if st.Mode().Perm() != defaultPerm {
		t.Fatalf("expected %v, got %v", defaultPerm, st.Mode().Perm())
	}

	// existing file keeps its permissions
	if err = os.Chmod(path, 0640); err != nil {
		t.Fatal(err)
	}
	writeAtomic(t, path, "hello again")
	st, err = os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if st.Mode().Perm() != 0640 {
		t.Fatalf("expected 0640, got %v", st.Mode().Perm())
	}
}

func TestSetWriteDeadlineDoesntCancel(t *testing.T) {
	path := filepath.Join(t.TempDir(), "f.txt")
	f, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	_ = f.SetWriteDeadline(time.Now().Add(time.Second))
	if _, err = f.Write([]byte("data")); err != nil {
		t.Fatal(err)
	}
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
	d, err := os.ReadFile(path)
	if err != nil || string(d) != "data" {
		t.Fatalf("got %q, %v", d, err)
	}
}
