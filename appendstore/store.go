package appendstore

import (
	"bufio"
	"fmt"
	"io"
	"iter"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Record struct {
	// offset in data file (or index file if DataInline is true), 0 means no data
	Offset int64
	// size of the data in bytes, 0 means no data
	Size int64
	// for over-written records we record the size taken in the file
	// which is larger than Size
	SizeInFile int64
	// time in utc unix milliseconds (milliseconds since January 1, 1970, 00:00:00 UTC)
	TimestampMs int64
	// kind of the record, e.g., "data", "metadata"
	// can't contain spaces or newlines
	Kind string
	// optional metadata, can't contain newlines
	Meta string
	// true if this record was over-written which means there's a newer version
	// with the same kind and meta after it
	Overwritten bool
	// true if data is stored inline in the index file instead of the data file
	DataInline bool
	// if not empty, data is stored in this file (relative to DataDir) instead of the data file
	FileName string
}

type Store struct {
	DataDir       string
	IndexFileName string
	DataFileName  string

	// when over-writing a record, we expand the data by this much to minimize
	// the amount written to file.
	// 0 means no expansion.
	// 40 means we expand the data by 40%
	// 100 means we expand the data by 100%
	OverWriteDataExpandPercent int

	// if true, will call file.Sync() after every write
	// this makes things super slow (5 secs vs 0.03 secs for 1000 records)
	SyncWrite bool

	indexFile *os.File
	dataFile  *os.File

	indexFilePath  string
	dataFilePath   string
	allRecords     []*Record
	nonOverwritten []*Record

	// internedKinds stores unique Kind strings to reduce memory usage
	// when many records share the same Kind
	internedKinds []string

	mu             sync.Mutex
	currDataOffset int64
}

func (s *Store) calcNonOverwritten() {
	s.nonOverwritten = make([]*Record, 0, len(s.allRecords))
	for _, rec := range s.allRecords {
		if !rec.Overwritten {
			s.nonOverwritten = append(s.nonOverwritten, rec)
		}
	}
}

// internKind returns an interned version of the kind string.
// If the kind already exists in internedKinds, returns the existing string.
// Otherwise, adds it to internedKinds and returns it.
func (s *Store) internKind(kind string) string {
	for _, k := range s.internedKinds {
		if k == kind {
			return k
		}
	}
	s.internedKinds = append(s.internedKinds, kind)
	return kind
}

// Records returns all records
func (s *Store) Records() []*Record {
	// no direct access to records to ensure thread safety
	s.mu.Lock()
	res := append([]*Record{}, s.nonOverwritten...)
	s.mu.Unlock()
	return res
}

// RecordsIter returns an iterator over all records
// Usage: for rec := range store.RecordsIter() { ... }
func (s *Store) RecordsIter() iter.Seq[*Record] {
	return func(yield func(*Record) bool) {
		s.mu.Lock()
		defer s.mu.Unlock()
		for _, rec := range s.nonOverwritten {
			if !yield(rec) {
				return
			}
		}
	}
}

// for debugging
func (s *Store) AllRecords() []*Record {
	// no direct access to records to ensure thread safety
	s.mu.Lock()
	res := append([]*Record{}, s.allRecords...)
	s.mu.Unlock()
	return res
}

func openFileSeekToEnd(path string, fp **os.File) (int64, error) {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return 0, err
	}
	off, err := file.Seek(0, io.SeekEnd) // move to the end of the file
	if err != nil {
		file.Close()
		return 0, err
	}
	*fp = file
	return off, nil
}

func (s *Store) reopenFiles() error {
	if s.indexFile == nil {
		_, err := openFileSeekToEnd(s.indexFilePath, &s.indexFile)
		if err != nil {
			return fmt.Errorf("failed to open index file for appending: %w", err)
		}
	}
	if s.dataFile == nil {
		off, err := openFileSeekToEnd(s.dataFilePath, &s.dataFile)
		if err != nil {
			s.closeFiles()
			return err
		}
		s.currDataOffset = off
	}
	return nil
}

// CloseFiles closes the index and data files.
func (s *Store) CloseFiles() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closeFiles()
}

func (s *Store) closeFiles() error {
	var err1, err2 error
	if s.indexFile != nil {
		err1 = s.indexFile.Close()
		s.indexFile = nil
	}
	if s.dataFile != nil {
		err2 = s.dataFile.Close()
		s.dataFile = nil
	}
	if err1 != nil {
		return err1
	}
	return err2
}

func appendToFile(file *os.File, data []byte, additionalBytes int, sync bool) (int64, error) {
	if additionalBytes > 0 {
		d2 := make([]byte, len(data)+additionalBytes)
		copy(d2, data)
		for i := len(data); i < len(d2); i++ {
			d2[i] = 32 // fill with spaces
		}
		data = d2
	}
	nWritten, err := file.Write(data)
	if err != nil {
		return 0, err
	}
	if nWritten < len(data) {
		return 0, fmt.Errorf("failed to write all data to file: wrote %d bytes, expected %d bytes", nWritten, len(data))
	}
	if sync {
		err = file.Sync()
		if err != nil {
			return 0, err
		}
	}
	return int64(nWritten), nil
}

func writeToFileAtOffset(file *os.File, offset int64, data []byte, sync bool) error {
	_, err := file.Seek(offset, io.SeekStart)
	if err != nil {
		return err
	}
	_, err = file.Write(data)
	if err != nil {
		return err
	}
	if sync {
		err = file.Sync()
	}
	return err
}

// format of the index line:
// <offset> <length>:[<length in file>] <timestamp> <kind> [<meta>]
// for inline data, offset is "_" and data follows immediately after the newline
// for file data, offset is ":<fileName>"
func serializeRecord(rec *Record) string {
	sz := ""
	if rec.SizeInFile > 0 {
		sz = fmt.Sprintf("%d:%d", rec.Size, rec.SizeInFile)
	} else {
		sz = fmt.Sprintf("%d", rec.Size)
	}
	if rec.TimestampMs == 0 {
		rec.TimestampMs = time.Now().UTC().UnixMilli()
	}
	t := rec.TimestampMs
	offsetStr := fmt.Sprintf("%d", rec.Offset)
	if rec.DataInline {
		offsetStr = "_"
	} else if rec.FileName != "" {
		offsetStr = ":" + rec.FileName
	}
	if rec.Meta == "" {
		return fmt.Sprintf("%s %s %d %s\n", offsetStr, sz, t, rec.Kind)
	}
	return fmt.Sprintf("%s %s %d %s %s\n", offsetStr, sz, t, rec.Kind, rec.Meta)
}

func validateKindAndMeta(kind, meta string) error {
	// kind cannot be empty or contain spaces or newlines
	if kind == "" {
		return fmt.Errorf("kind is empty")
	}
	if strings.Contains(kind, " ") {
		return fmt.Errorf("kind cannot contain spaces")
	}
	if strings.Contains(kind, "\n") {
		return fmt.Errorf("kind cannot contain newlines")
	}
	if strings.Contains(meta, "\n") {
		return fmt.Errorf("metadata cannot contain newlines")
	}
	return nil
}

func (s *Store) appendRecord(kind string, meta string, data []byte, additionalBytes int, timestampMs int64) error {
	if err := validateKindAndMeta(kind, meta); err != nil {
		return err
	}

	err := s.reopenFiles()
	if err != nil {
		return err
	}

	size := int64(len(data))
	rec := &Record{
		Size:        size,
		Kind:        s.internKind(kind),
		Meta:        meta,
		TimestampMs: timestampMs,
	}
	if size > 0 {
		rec.Offset = s.currDataOffset
		err := s.appendToDataFile(data, additionalBytes)
		if err != nil {
			return err
		}
	}
	if additionalBytes > 0 {
		rec.SizeInFile = rec.Size + int64(additionalBytes)
	}

	indexLine := serializeRecord(rec)
	_, err = appendToFile(s.indexFile, []byte(indexLine), 0, s.SyncWrite)
	if err != nil {
		return err
	}
	s.allRecords = append(s.allRecords, rec)
	s.nonOverwritten = append(s.nonOverwritten, rec)
	return nil
}

func (s *Store) appendToDataFile(data []byte, additionalBytes int) error {
	if err := s.reopenFiles(); err != nil {
		return err
	}
	_, err := s.dataFile.Seek(0, io.SeekEnd) // move to the end of the file
	if err != nil {
		return err
	}

	if nWritten, err := appendToFile(s.dataFile, data, additionalBytes, s.SyncWrite); err != nil {
		return err
	} else {
		s.currDataOffset += nWritten
	}
	return nil
}

// AppendRecord appends a new record to the store.
func (s *Store) AppendRecord(kind string, meta string, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.appendRecord(kind, meta, data, 0, 0)
}

// AppendRecordWithTimestamp appends a new record to the store with the specified timestamp in milliseconds.
// If timestampMs is 0, the current time will be used.
// Note: timestampMs should be in UTC (time.Now().UTC().UnixMilli())
// Timestamps is meant to record the creation time of the data being stored.
// Explicitly setting timestamps can be useful when importing data from other sources
func (s *Store) AppendRecordWithTimestamp(kind string, meta string, data []byte, timestampMs int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.appendRecord(kind, meta, data, 0, timestampMs)
}

// AppendRecordInline appends a new record with data stored inline in the index file.
// This is useful for small data that doesn't warrant a separate entry in the data file.
// The data is stored immediately after the index line in the index file.
func (s *Store) AppendRecordInline(kind string, meta string, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.appendRecordInline(kind, meta, data, 0)
}

// AppendRecordInlineWithTimestamp appends a new record with data stored inline in the index file
// with the specified timestamp in milliseconds.
func (s *Store) AppendRecordInlineWithTimestamp(kind string, meta string, data []byte, timestampMs int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.appendRecordInline(kind, meta, data, timestampMs)
}

func (s *Store) appendRecordInline(kind string, meta string, data []byte, timestampMs int64) error {
	if err := validateKindAndMeta(kind, meta); err != nil {
		return err
	}

	err := s.reopenFiles()
	if err != nil {
		return err
	}

	rec := &Record{
		Size:        int64(len(data)),
		Kind:        s.internKind(kind),
		Meta:        meta,
		TimestampMs: timestampMs,
		DataInline:  true,
	}

	indexLine := serializeRecord(rec)

	// Get current position in index file to calculate where inline data will be
	currentPos, err := s.indexFile.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	// The inline data starts right after the index line
	rec.Offset = currentPos + int64(len(indexLine))

	// Write the index line
	_, err = s.indexFile.WriteString(indexLine)
	if err != nil {
		return err
	}

	// Write the inline data immediately after
	if len(data) > 0 {
		_, err = s.indexFile.Write(data)
		if err != nil {
			return err
		}
	}

	if s.SyncWrite {
		err = s.indexFile.Sync()
		if err != nil {
			return err
		}
	}

	s.allRecords = append(s.allRecords, rec)
	s.nonOverwritten = append(s.nonOverwritten, rec)
	return nil
}

// AppendRecordFile appends a new record with data stored in a separate file.
// The file is created in DataDir with the given fileName.
// Returns error if fileName contains a space.
func (s *Store) AppendRecordFile(kind string, meta string, data []byte, fileName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.appendRecordFile(kind, meta, data, fileName, 0)
}

// AppendRecordFileWithTimestamp appends a new record with data stored in a separate file
// with the specified timestamp in milliseconds.
func (s *Store) AppendRecordFileWithTimestamp(kind string, meta string, data []byte, fileName string, timestampMs int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.appendRecordFile(kind, meta, data, fileName, timestampMs)
}

func (s *Store) appendRecordFile(kind string, meta string, data []byte, fileName string, timestampMs int64) error {
	if err := validateKindAndMeta(kind, meta); err != nil {
		return err
	}
	if strings.Contains(fileName, " ") {
		return fmt.Errorf("fileName cannot contain spaces")
	}
	if fileName == "" {
		return fmt.Errorf("fileName cannot be empty")
	}

	err := s.reopenFiles()
	if err != nil {
		return err
	}

	// Write data to the separate file
	filePath := filepath.Join(s.DataDir, fileName)
	err = os.WriteFile(filePath, data, 0644)
	if err != nil {
		return fmt.Errorf("failed to write data to file %s: %w", filePath, err)
	}

	rec := &Record{
		Size:        int64(len(data)),
		Kind:        s.internKind(kind),
		Meta:        meta,
		TimestampMs: timestampMs,
		FileName:    fileName,
	}

	indexLine := serializeRecord(rec)
	_, err = appendToFile(s.indexFile, []byte(indexLine), 0, s.SyncWrite)
	if err != nil {
		return err
	}

	s.allRecords = append(s.allRecords, rec)
	s.nonOverwritten = append(s.nonOverwritten, rec)
	return nil
}

func (s *Store) overwriteRecord(kind string, meta string, data []byte, timestampMs int64) error {
	if len(data) == 0 {
		return s.AppendRecordWithTimestamp(kind, meta, nil, timestampMs)
	}

	// find a record that we can overwrite
	recToOverwriteIdx := -1
	neededSize := int64(len(data))
	for i, rec := range s.allRecords {
		if rec.Kind == kind && rec.Meta == meta && rec.SizeInFile >= neededSize {
			recToOverwriteIdx = i
			break
		}
	}
	if recToOverwriteIdx == -1 {
		// no record to overwrite, append a new one with potentially padding
		// for future overwrites
		additionalBytes := (neededSize * int64(s.OverWriteDataExpandPercent)) / 100
		return s.appendRecord(kind, meta, data, int(additionalBytes), timestampMs)
	}

	err := s.reopenFiles()
	if err != nil {
		return err
	}
	recOverwritten := s.allRecords[recToOverwriteIdx]
	offset := recOverwritten.Offset
	recOverwritten.Overwritten = true
	writeToFileAtOffset(s.dataFile, offset, data, s.SyncWrite)

	rec := &Record{
		Offset:      offset,
		Size:        int64(len(data)),
		SizeInFile:  0,
		Kind:        s.internKind(kind),
		Meta:        meta,
		TimestampMs: timestampMs,
	}
	indexLine := serializeRecord(rec)
	_, err = appendToFile(s.indexFile, []byte(indexLine), 0, s.SyncWrite)
	if err != nil {
		return err
	}
	s.allRecords = append(s.allRecords, rec)
	s.calcNonOverwritten()
	return nil
}

// OverwriteRecordWithTimestamp overwrites an existing record with the same kind and meta if possible.
// If no such record exists or the new data is larger than the existing record's allocated size,
// a new record is appended.
// Timestamps is meant to record the creation time of the data being stored.
// Explicitly setting timestamps can be useful when importing data from other sources
func (s *Store) OverwriteRecordWithTimestamp(kind string, meta string, data []byte, timestampMs int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.overwriteRecord(kind, meta, data, timestampMs)
}

// OverwriteRecord overwrites an existing record with the same kind and meta if possible.
// If no such record exists or the new data is larger than the existing record's allocated size,
// a new record is appended.
// This is meant for scenarios where data with the same kind and meta is updated frequently
// and you don't care about preserving previous versions.
func (s *Store) OverwriteRecord(kind string, meta string, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.overwriteRecord(kind, meta, data, 0)
}

// ParseIndexLine parses a single line from the index file into a Record.
// rec is passed in to allow re-using Record for perf. can be nil
// For inline records (offset="_"), the caller must set rec.Offset to the
// actual byte position in the index file where the data starts.
func ParseIndexLine(line string, rec *Record) error {
	parts := strings.SplitN(line, " ", 5)
	if len(parts) < 4 {
		return fmt.Errorf("invalid index line: %s", line)
	}

	var err error
	rec.DataInline = false // reset in case of reuse
	rec.FileName = ""      // reset in case of reuse
	if parts[0] == "_" {
		// Inline data - offset will be set by caller based on file position
		rec.DataInline = true
		rec.Offset = 0 // will be set by caller
	} else if strings.HasPrefix(parts[0], ":") {
		// File data - data is stored in a separate file
		rec.FileName = parts[0][1:]
		rec.Offset = 0
	} else {
		rec.Offset, err = strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			return fmt.Errorf("invalid offset '%s' in index line: %s", parts[0], line)
		}
		if rec.Offset < 0 {
			return fmt.Errorf("invalid offset '%s' in index line: %s", parts[0], line)
		}
	}

	rec.SizeInFile = 0 // possibly reusing rec so needs to reset
	sizeParts := strings.SplitN(parts[1], ":", 2)
	rec.Size, err = strconv.ParseInt(sizeParts[0], 10, 64)
	if err != nil {
		return fmt.Errorf("invalid size '%s' in index line: %s", parts[1], line)
	}
	if rec.Size < 0 {
		return fmt.Errorf("invalid size '%s' in index line: %s", parts[1], line)
	}

	if len(sizeParts) > 1 {
		rec.SizeInFile, err = strconv.ParseInt(sizeParts[1], 10, 64)
		if err != nil {
			return fmt.Errorf("invalid size '%s' in index line: %s", parts[1], line)
		}
	}

	rec.TimestampMs, err = strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		return fmt.Errorf("invalid timestamp '%s' in index line: %s", parts[2], line)
	}
	if rec.TimestampMs < 0 {
		return fmt.Errorf("invalid timestamp '%s' in index line: %s", parts[2], line)
	}

	rec.Kind = parts[3]
	rec.Meta = "" // possibly reusing rec so needs to reset
	if len(parts) > 4 {
		rec.Meta = parts[4]
	}
	return nil
}

// ParseIndexFromFile parses index lines from a file into a slice of Records.
// This function properly handles inline data by tracking file positions.
// If internKind is not nil, it will be called to intern the Kind string.
func ParseIndexFromFile(path string, internKind func(string) string) ([]*Record, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	var records []*Record
	var currentOffset int64 = 0

	for {
		line, err := reader.ReadString('\n')
		if err == io.EOF {
			if line == "" {
				break
			}
			// process last line without newline
		} else if err != nil {
			return nil, fmt.Errorf("error reading index file: %w", err)
		}

		lineLen := int64(len(line))
		line = strings.TrimSuffix(line, "\n")
		if line == "" {
			currentOffset += lineLen
			continue
		}

		rec := &Record{}
		err = ParseIndexLine(line, rec)
		if err != nil {
			return nil, err
		}

		if internKind != nil {
			rec.Kind = internKind(rec.Kind)
		}

		if rec.DataInline {
			// Data follows immediately after this line
			rec.Offset = currentOffset + lineLen
			currentOffset += lineLen + rec.Size
			// Skip past the inline data in the buffer
			if rec.Size > 0 {
				_, err = io.CopyN(io.Discard, reader, rec.Size)
				if err != nil {
					return nil, fmt.Errorf("error skipping inline data: %w", err)
				}
			}
		} else {
			currentOffset += lineLen
		}

		records = append(records, rec)
	}
	return records, nil
}

// ParseIndexFromData parses multiple index lines from a byte slice into a slice of Records.
func ParseIndexFromData(d []byte) ([]*Record, error) {
	scanner := bufio.NewScanner(strings.NewReader(string(d)))
	return ParseIndexFromScanner(scanner)
}

// ParseIndexFromScanner parses multiple index lines from a bufio.Scanner into a slice of Records.
// It's meant to efficiently parse index file.
func ParseIndexFromScanner(scanner *bufio.Scanner) ([]*Record, error) {
	var records []*Record
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		rec := &Record{}
		err := ParseIndexLine(line, rec)
		if err != nil {
			return nil, err
		}
		records = append(records, rec)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading from scanner: %w", err)
	}
	return records, nil
}

// readFilePart efficiently reads a specific portion of a file
func readFilePart(path string, offset int64, len int64) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// Seek to the specified offset
	_, err = file.Seek(offset, io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("failed to seek to offset %d: %w", offset, err)
	}

	// Read exactly len bytes
	buf := make([]byte, len)
	n, err := io.ReadFull(file, buf)
	if err != nil {
		if err == io.EOF {
			return nil, fmt.Errorf("reached end of file after reading %d bytes, expected %d", n, len)
		}
		return nil, fmt.Errorf("failed to read %d bytes: %w", len, err)
	}

	return buf, nil
}

// ReadRecord reads the data for a given record.
// For inline records (DataInline=true), reads from the index file.
// For file records (FileName!=""), reads from the specified file in DataDir.
// For regular records, reads from the data file.
func (s *Store) ReadRecord(r *Record) ([]byte, error) {
	if r.Size == 0 {
		return nil, nil
	}
	// TODO: not sure if this is needed
	s.mu.Lock()
	defer s.mu.Unlock()

	if r.DataInline {
		return readFilePart(s.indexFilePath, r.Offset, r.Size)
	}
	if r.FileName != "" {
		filePath := filepath.Join(s.DataDir, r.FileName)
		return os.ReadFile(filePath)
	}
	if r.Offset < 0 {
		return nil, nil
	}
	return readFilePart(s.dataFilePath, r.Offset, r.Size)
}

func readAllRecords(path string, internKind func(string) string) ([]*Record, error) {
	return ParseIndexFromFile(path, internKind)
}

// OpenStore initializes the Store by loading existing records from the index file.
func OpenStore(s *Store) error {
	if s.DataDir == "" {
		return fmt.Errorf("data directory is not set. For current directory, use '.'")
	}
	if s.IndexFileName == "" {
		s.IndexFileName = "index.txt"
	}
	if s.DataFileName == "" {
		s.DataFileName = "data.bin"
	}

	var err error
	s.indexFilePath = filepath.Join(s.DataDir, s.IndexFileName)
	s.indexFilePath, err = filepath.Abs(s.indexFilePath)
	if err != nil {
		return fmt.Errorf("failed to get absolute path for index file: %w", err)
	}
	s.dataFilePath = filepath.Join(s.DataDir, s.DataFileName)
	s.dataFilePath, err = filepath.Abs(s.dataFilePath)
	if err != nil {
		return fmt.Errorf("failed to get absolute path for data file: %w", err)
	}

	err = os.MkdirAll(s.DataDir, 0755)
	if err != nil {
		return err
	}
	if _, err := os.Stat(s.indexFilePath); os.IsNotExist(err) {
		file, err := os.Create(s.indexFilePath)
		if err != nil {
			return err
		}
		file.Close()
	}

	s.allRecords, err = readAllRecords(s.indexFilePath, s.internKind)
	if err != nil {
		return fmt.Errorf("failed to read records from index file: %w", err)
	}

	// mark overwritten records
	m := make(map[int64]*Record)
	for _, rec := range s.allRecords {
		if rec.Size == 0 {
			continue
		}
		// Only check for overwrites on regular records (not inline or file records)
		// Inline records don't overwrite each other (they each have unique positions in index file)
		// File records don't overwrite each other (they're separate files)
		if rec.DataInline || rec.FileName != "" {
			continue
		}
		// this record has the same offset as previous one which means
		// previous one was overwritten by this one
		if rec := m[rec.Offset]; rec != nil {
			rec.Overwritten = true
			continue
		}
		m[rec.Offset] = rec
	}
	s.calcNonOverwritten()
	return nil
}
