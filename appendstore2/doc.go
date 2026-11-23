// Package appendstore2 provides a simple append-only storage system with
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
// The store doesn't keep records in memory. OnRecord is called for every
// record read in OpenStore and for every appended record, so you can build
// your in-memory state.
//
//	var records []*appendstore2.Record
//	s := &appendstore2.Store{
//	    DataDir:       "./data",
//	    IndexFileName: "index.txt",
//	    DataFileName:  "data.bin",
//	    OnRecord: func(rec *appendstore2.Record, data []byte) {
//	        records = append(records, rec)
//	    },
//	}
//	err := appendstore2.OpenStore(s)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer s.CloseFiles()
//
//	// Append a record
//	err = s.AppendData("kind", "meta string", []byte("value1"))
//
//	// Read records
//	for _, rec := range records {
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
//	line, err := appendstore2.KeyValueMarshal("name", "John Doe", "age", "30")
//	// line: `name:"John Doe" age:30`
//
//	pairs, err := appendstore2.KeyValueUnmarshal(line)
//	// pairs: ["name", "John Doe", "age", "30"]
//
// # Thread Safety
//
// The Store is not safe for concurrent use. If you access it from multiple
// goroutines, protect it with your own mutex.
package appendstore2
