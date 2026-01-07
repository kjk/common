/*
To write to files in a robust way we should:

- handle error returned by `Close()`

- handle error returned by `Write()`

- remove partially written file if `Write()` or `Close()` returned an error

This logic is non-trivial.

Package atomicfile makes it easy to get this logic right:

	func writeToFileAtomically(filePath string, data []byte) error {
		w, err := atomicfile.New(filePath)
		if err != nil {
			return err
		}
		// calling Close() twice is a no-op
		defer w.Close()

		_, err = w.Write(data)
		if err != nil {
			return err
		}
		return w.Close()
	}


To learn more see https://presstige.io/p/atomicfile-22143bf788b542fda2262ca7aee57ae4
*/
package atomicfile
