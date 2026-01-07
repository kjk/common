# atomicfile

A Go library to help create files atomically.

The file is created only if all writes succeeded.

To learn how to use it:

- tutorial: https://blog.kowalczyk.info/article/90f01e0e24924f6c8ed428f065c0016a/atomicfile-robustly-writing-to-a-file-in-go.html
- api docs: ttps://pkg.go.dev/github.com/kjk/common/atomicfile

## example usage

```go
package main

import (
	"fmt"
	"log"
	"os"

	"github.com/kjk/atomicfile"
)

func writeToFileAtomically(filePath string, data []byte) error {
	f, err := atomicfile.New(filePath)
	if err != nil {
		return err
	}

	// ensure that if there's a panic or early exit due to
	// error, the file will not be created
	// RemoveIfNotClosed() after Close() is a no-op
	defer f.RemoveIfNotClosed()

	_, err = f.Write(data)
	if err != nil {
		return err
	}
	return f.Close()
}

func main() {
	fileName := "foo.txt"
	data := []byte("hello\n")
	err := writeToFileAtomically(fileName, data)
	if err != nil {
		fmt.Printf("writeToFileAtomically failed with '%s'\n", err)
		return
	}
	st, err := os.Stat(fileName)
	if err != nil {
		log.Fatalf("os.Stat('%s') failed with '%s'\n", fileName, err)
	}
	fmt.Printf("Wrote to file '%s' atomically. Size of file: %d bytes\n", fileName, st.Size())
	_ = os.Remove(fileName)
}
```
