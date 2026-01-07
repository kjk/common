package atomicfile

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func assertFileExists(t *testing.T, path string) {
	st, err := os.Stat(path)
	if err != nil {
		t.Fatalf("file '%s' doesn't exist, os.Stat() failed with '%s'", path, err)
	}
	if !st.Mode().IsRegular() {
		t.Fatalf("Path '%s' exists but is not a file (mode: %d)", path, int(st.Mode()))
	}
}

func assertFileNotExists(t *testing.T, path string) {
	_, err := os.Stat(path)
	if err == nil {
		t.Fatalf("file '%s' exist, expected to not exist", path)
	}
}

func assertNoError(t *testing.T, err error) {
	if err != nil {
		t.Fatalf("error: %s", err)
	}
}

func assertError(t *testing.T, err error) {
	if err == nil {
		t.Fatal("expected to get an error")
	}
}

func assertFileSizeEqual(t *testing.T, path string, n int64) {
	st, err := os.Stat(path)
	if err != nil {
		t.Fatalf("os.Stat('%s') failed with '%s'", path, err)
	}
	if st.Size() != n {
		t.Fatalf("path: '%s', expected size: %d, got: %d", path, n, st.Size())
	}
}

func assertIntEqual(t *testing.T, exp int, got int) {
	if exp != got {
		t.Fatalf("expected: %d, got: %d", exp, got)
	}
}

func TestSimulateError(t *testing.T) {
	// test cleanup after write
	dst := "atomic_file.go.copy2"
	_ = os.Remove(dst)
	f, err := New(dst)
	assertNoError(t, err)
	assertFileExists(t, f.tmpPath)
	_, err = f.Write([]byte("foo"))
	assertNoError(t, err)
	// simulate an error
	errSimulated := errors.New("simiulated")
	f.err = errSimulated
	err = f.Close()
	if err != errSimulated {
		t.Fatalf("got unexpected error")
	}
	assertFileNotExists(t, f.tmpPath)
	assertFileNotExists(t, dst)
	// on second Close() should get the same error
	err = f.Close()
	if err != errSimulated {
		t.Fatalf("got unexpected error")
	}
}

func writeWithPanicClose(t *testing.T, f *File) {
	defer f.Close()

	_, err := f.Write([]byte("foo"))
	assertNoError(t, err)
	panic("simulating a crash")
}

func recoverWritePanic(t *testing.T, f *File) {
	defer func() {
		err := recover()
		if err == nil {
			t.Fatalf("expected to panic")
		}
	}()

	writeWithPanicClose(t, f)
}

func TestWriteWithPanic(t *testing.T) {
	dst := "atomic_file.go.copy"
	_ = os.Remove(dst)
	f, err := New(dst)
	assertNoError(t, err)
	assertFileExists(t, f.tmpPath)
	recoverWritePanic(t, f)
	assertFileExists(t, dst)
}

func writeWithPanicCancel(t *testing.T, f *File) {
	defer f.RemoveIfNotClosed()

	_, err := f.Write([]byte("foo"))
	assertNoError(t, err)
	panic("simulating a crash")
}

func recoverCancelPanic(t *testing.T, f *File) {
	defer func() {
		err := recover()
		if err == nil {
			t.Fatalf("expected to panic")
		}
	}()

	writeWithPanicCancel(t, f)
}

func TestCancel(t *testing.T) {
	dst := "atomic_file.go.copy"
	_ = os.Remove(dst)
	f, err := New(dst)
	assertNoError(t, err)
	assertFileExists(t, f.tmpPath)
	recoverCancelPanic(t, f)
	assertFileNotExists(t, f.tmpPath)
}

func TestWrite(t *testing.T) {
	dst := "atomic_file.go.copy"
	_ = os.Remove(dst)
	{
		f, err := New(dst)
		assertNoError(t, err)
		assertFileExists(t, f.tmpPath)
		_ = f.Close()
		assertFileExists(t, dst)
		assertFileSizeEqual(t, dst, 0)
		assertFileNotExists(t, f.tmpPath)
	}

	d, err := os.ReadFile("atomic_file.go")
	assertNoError(t, err)

	{
		f, err := New(dst)
		assertNoError(t, err)
		assertFileExists(t, f.tmpPath)
		n, err := f.Write(d)
		assertNoError(t, err)
		assertIntEqual(t, n, len(d))
		assertFileExists(t, f.tmpPath)
		err = f.Close()
		assertNoError(t, err)
		assertFileNotExists(t, f.tmpPath)
		assertFileSizeEqual(t, dst, int64(len(d)))
		// calling Close twice is a no-op
		err = f.Close()
		assertNoError(t, err)
	}
	_ = os.Remove(dst)

	{
		// check that Cancel sets an error state
		f, err := New(dst)
		assertNoError(t, err)
		f.RemoveIfNotClosed()
		_, err = f.Write(d)
		if err != ErrCancelled {
			t.Fatalf("expected err to be %v, got %v", ErrCancelled, err)
		}
		err = f.Close()
		if err != ErrCancelled {
			t.Fatalf("expected err to be %v, got %v", ErrCancelled, err)
		}
		err = f.Close()
		if err != ErrCancelled {
			t.Fatalf("expected err to be %v, got %v", ErrCancelled, err)
		}
		os.Remove(dst)
	}

	// we can't create files in directories that don't exist
	// so verify we do an early check (no point writing to a file
	// if it couldn't be created at the end)
	dst = filepath.Join("foo", "bar.txt")
	{
		f, err := New(dst)
		assertError(t, err)
		if f != nil {
			t.Fatalf("expected w to be nil, got %v", f)
		}
	}
}
