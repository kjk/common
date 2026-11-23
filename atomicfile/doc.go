/*
To write to files in a robust way we should:

- handle error returned by `Close()`

- handle error returned by `Write()`

- remove partially written file if `Write()` or `Close()` returned an error

This logic is non-trivial.

Package atomicfile makes it easy to get this logic right:

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

To learn more see https://blog.kowalczyk.info/article/90f01e0e24924f6c8ed428f065c0016a/atomicfile-robustly-writing-to-a-file-in-go.html
*/
package atomicfile
