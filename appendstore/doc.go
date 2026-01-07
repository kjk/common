// Package appendstore provides a simple append-only storage system with
// an index file and a data file.
//
// The store is designed for scenarios where data needs to be persisted
// efficiently with metadata tracking.
//
// # Store Structure
//
// A Store consists of two files:
//   - An index file (default: "index.txt") containing record metadata
//   - A data file (default: "data.bin") containing the actual record data
//
// Each record has an offset, size, timestamp, kind, and optional metadata.
//
// # Basic Usage
//
//	s := &appendstore.Store{
//	    DataDir: "./data",
//	    IndexFileName: "index.txt",
//	    DataFileName: "data.bin",
//	}
//	err := appendstore.OpenStore(s)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer s.CloseFiles()
//
//	// Append a record
//	err = s.AppendRecord("kind", "meta string", []byte("value1"))
//
//	// Read records
//	for _, rec := range s.Records() {
//	    data, err := s.ReadRecord(rec)
//	    // ...
//	}
//
// # Key-Value Serialization
//
// The package also provides [KeyValueMarshal] and [KeyValueUnmarshal] functions
// for serializing key-value pairs into a simple single-line format suitable
// for storing in record metadata or data.
//
//	line, err := appendstore.KeyValueMarshal("name", "John Doe", "age", "30")
//	// line: `name:John age:30`
//
//	pairs, err := appendstore.KeyValueUnmarshal(line)
//	// pairs: ["name", "John Doe", "age", "30"]
//
// # Thread Safety
//
// The Store is safe for concurrent use. All public methods that access
// or modify records are protected by a mutex.
package appendstore
