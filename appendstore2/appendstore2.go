package appendstore2

import (
	"bufio"
	"fmt"
	"io"
	"iter"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	// kOffsetFile indicates that the data is stored in a separate file and metada is 0
	kOffsetFileMeatDataZero int64 = math.MinInt64
)

type Record struct {
	// offset in data file (or index file if inline), 0 means no data
	// offset < 0 means data is stored in a separate file named in metaOrFileName
	offset int64
	// size of the data in bytes
	// negative or zero means inline data (stored in index file). Also applies to file records with metadata.
	// use Size() to get absolute value, isInline() to check if inline, IsFile() to see if stored in separate file
	// for files, it's the size of metadata. For size of the file use store.FileSize(*Record))
	size int64
	// time in utc unix milliseconds (milliseconds since January 1, 1970, 00:00:00 UTC)
	TimestampMs int64
	// kind of the record, e.g., "data", "metadata"
	// can't contain spaces or newlines
	Kind string
	// for file records (Offset == kOffsetFile): the file name
	// for other records: optional metadata, can't contain newlines
	metaOrFileName string
}

// Size returns the absolute size of the data in bytes
func (r *Record) Size() int64 {
	if r.size < 0 {
		return -r.size
	}
	return r.size
}

func (r *Record) Offset() int64 {
	if r.offset < 0 {
		return -r.offset
	}
	return r.offset
}

// isInline returns true if the data is stored inline in the index file
func (r *Record) isInline() bool {
	return r.size <= 0
}

// IsFile returns true if the data is stored in a separate file
func (r *Record) IsFile() bool {
	return r.offset < 0
}

// FileSize() returns the size of the file for file records.
// Size() returns size of inline metadata.
func (s *Store) FileSize(r *Record) (int64, error) {
	if !r.IsFile() {
		return 0, fmt.Errorf("not a file record")
	}
	path := filepath.Join(s.DataDir, r.FileName())
	st, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return st.Size(), nil
}

// Meta returns the metadata for this record.
// For file records, returns empty string.
func (r *Record) Meta() string {
	if r.IsFile() {
		return ""
	}
	return r.metaOrFileName
}

// FileName returns the file name for file records.
// For non-file records, returns empty string.
func (r *Record) FileName() string {
	if r.IsFile() {
		return r.metaOrFileName
	}
	return ""
}

type Store struct {
	DataDir       string
	IndexFileName string
	DataFileName  string

	// if true, will call file.Sync() after every write
	// this makes things super slow (5 secs vs 0.03 secs for 1000 records)
	SyncWrite bool

	OnRecord  func(*Record, []byte)
	indexFile *os.File
	dataFile  *os.File

	indexFilePath string
	dataFilePath  string

	// internedKinds stores unique Kind strings to reduce memory usage
	// when many records share the same Kind
	internedKinds []string
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

// CloseFiles closes the index and data files.
func (s *Store) CloseFiles() error {
	if s == nil {
		return nil
	}
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

// writeWithOptionalNewline writes data to the file followed by a newline if data is not empty and doesn't end with one.
// this is for readability of log files: \n is a separator between records
func writeWithOptionalNewline(file *os.File, data []byte, sync bool) (int, error) {
	if len(data) == 0 {
		return 0, nil
	}
	nWritten, err := file.Write(data)
	if err != nil {
		return nWritten, err
	}
	if data[len(data)-1] == '\n' {
		return nWritten, nil
	}
	_, err = file.WriteString("\n")
	if err != nil {
		return 0, err
	}
	if sync {
		err = file.Sync()
		if err != nil {
			return 0, err
		}
	}
	return nWritten, nil
}

func appendToFile(path string, filePtr **os.File, data []byte, sync bool) (int64, int64, error) {
	var err error
	var off int64

	file := *filePtr
	defer func() {
		if err != nil && file != nil {
			file.Close()
			*filePtr = nil
		}
	}()

	if file == nil {
		file, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			return 0, 0, err
		}
		*filePtr = file
	}
	off, err = file.Seek(0, io.SeekEnd) // move to the end of the file
	if err != nil {
		return 0, 0, err
	}

	nWritten, err := writeWithOptionalNewline(file, data, sync)
	if err != nil {
		return 0, 0, err
	}
	return off, int64(nWritten), nil
}

// format of the index line:
// <offset> <length> <timestamp> <kind> [<meta>]
// for inline data, offset is "_" and data follows immediately after the newline
// for file data, offset is "f" and meta contains the fileName
func serializeRecord(rec *Record) string {
	if rec.TimestampMs == 0 {
		rec.TimestampMs = time.Now().UTC().UnixMilli()
	}
	t := rec.TimestampMs
	var offsetStr string
	if rec.IsFile() {
		offsetStr = "f"
	} else if rec.isInline() {
		offsetStr = "_"
	} else {
		offsetStr = fmt.Sprintf("%d", rec.Offset())
	}
	size := rec.Size()
	if rec.metaOrFileName == "" {
		return fmt.Sprintf("%s %d %d %s\n", offsetStr, size, t, rec.Kind)
	}
	return fmt.Sprintf("%s %d %d %s %s\n", offsetStr, size, t, rec.Kind, rec.metaOrFileName)
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

func (s *Store) appendRecord(kind string, meta string, data []byte, timestampMs int64) error {
	if err := validateKindAndMeta(kind, meta); err != nil {
		return err
	}

	size := int64(len(data))
	rec := &Record{
		size:           size,
		Kind:           s.internKind(kind),
		metaOrFileName: meta,
		TimestampMs:    timestampMs,
	}
	if size > 0 {
		off, _, err := appendToFile(s.dataFilePath, &s.dataFile, data, s.SyncWrite)
		rec.offset = off
		if err != nil {
			return err
		}
	}

	indexLine := serializeRecord(rec)
	if _, _, err := appendToFile(s.indexFilePath, &s.indexFile, []byte(indexLine), s.SyncWrite); err != nil {
		return err
	}
	if s.OnRecord != nil {
		s.OnRecord(rec, data)
	}
	return nil
}

// AppendData appends a new record to the store.
func (s *Store) AppendData(kind string, meta string, data []byte) error {
	return s.appendRecord(kind, meta, data, 0)
}

// AppendDataWithTimestamp appends a new record to the store with the specified timestamp in milliseconds.
// If timestampMs is 0, the current time will be used.
// Note: timestampMs should be in UTC (time.Now().UTC().UnixMilli())
// Timestamps is meant to record the creation time of the data being stored.
// Explicitly setting timestamps can be useful when importing data from other sources
func (s *Store) AppendDataWithTimestamp(kind string, meta string, data []byte, timestampMs int64) error {
	return s.appendRecord(kind, meta, data, timestampMs)
}

// AppendDataInline appends a new record with data stored inline in the index file.
// This is useful for small data that doesn't warrant a separate entry in the data file.
// The data is stored immediately after the index line in the index file.
func (s *Store) AppendDataInline(kind string, meta string, data []byte) error {
	return s.appendRecordInline(kind, meta, data, 0)
}

// AppendDataInlineWithTimestamp appends a new record with data stored inline in the index file
// with the specified timestamp in milliseconds.
func (s *Store) AppendDataInlineWithTimestamp(kind string, meta string, data []byte, timestampMs int64) error {
	return s.appendRecordInline(kind, meta, data, timestampMs)
}

func (s *Store) appendRecordInline(kind string, meta string, data []byte, timestampMs int64) error {
	if err := validateKindAndMeta(kind, meta); err != nil {
		return err
	}

	rec := &Record{
		size:           -int64(len(data)), // negative size indicates inline
		Kind:           s.internKind(kind),
		metaOrFileName: meta,
		TimestampMs:    timestampMs,
	}

	indexLine := serializeRecord(rec)
	sync := s.SyncWrite && len(data) > 0
	off, _, err := appendToFile(s.indexFilePath, &s.indexFile, []byte(indexLine), sync)
	if err != nil {
		return err
	}
	// set the right Offset for OnRecord
	rec.offset = off + int64(len(indexLine))
	if _, err = writeWithOptionalNewline(s.indexFile, data, s.SyncWrite); err != nil {
		return err
	}
	s.OnRecord(rec, data)
	return nil
}

// AppendFile appends a new record with data stored in a separate file.
// The file is created in DataDir with the given fileName.
// Returns error if fileName contains a space.
func (s *Store) AppendFile(kind string, fileName string, data []byte, metaData []byte) error {
	return s.appendRecordFile(kind, fileName, data, metaData, 0)
}

// AppendFileWithTimestamp appends a new record with data stored in a separate file
// with the specified timestamp in milliseconds.
func (s *Store) AppendFileWithTimestamp(kind string, fileName string, data []byte, metaData []byte, timestampMs int64) error {
	return s.appendRecordFile(kind, fileName, data, metaData, timestampMs)
}

func (s *Store) appendRecordFile(kind string, fileName string, data []byte, metaData []byte, timestampMs int64) error {
	if kind == "" {
		return fmt.Errorf("kind is empty")
	}
	if strings.Contains(kind, " ") {
		return fmt.Errorf("kind cannot contain spaces")
	}
	if strings.Contains(kind, "\n") {
		return fmt.Errorf("kind cannot contain newlines")
	}
	if strings.Contains(fileName, "\n") {
		return fmt.Errorf("fileName cannot contain newlines")
	}
	if fileName == "" {
		return fmt.Errorf("fileName cannot be empty")
	}

	// Write data to the separate file
	filePath := filepath.Join(s.DataDir, fileName)
	err := os.WriteFile(filePath, data, 0644)
	if err != nil {
		return fmt.Errorf("failed to write data to file %s: %w", filePath, err)
	}

	rec := &Record{
		offset:         kOffsetFileMeatDataZero,
		size:           -int64(len(metaData)),
		Kind:           s.internKind(kind),
		metaOrFileName: fileName,
		TimestampMs:    timestampMs,
	}

	sync := s.SyncWrite && len(metaData) > 0
	indexLine := serializeRecord(rec)
	off, _, err := appendToFile(s.indexFilePath, &s.indexFile, []byte(indexLine), sync)

	if err != nil {
		return err
	}
	// set the right Offset for OnRecord
	rec.offset = -(off + int64(len(indexLine)))
	if _, err = writeWithOptionalNewline(s.indexFile, metaData, s.SyncWrite); err != nil {
		return err
	}

	s.OnRecord(rec, metaData)
	return nil
}

// splitFields splits a string into up to len(parts) space-separated fields.
// The last field captures the remainder of the string.
// Returns the number of fields found.
func splitFields(s string, parts *[5]string) int {
	n := 0
	start := 0
	for i := 0; i < len(s) && n < len(parts)-1; i++ {
		if s[i] == ' ' {
			if i > start {
				parts[n] = s[start:i]
				n++
			}
			start = i + 1
		}
	}
	// Last field gets the remainder
	if start < len(s) {
		parts[n] = s[start:]
		n++
	}
	return n
}

// ParseIndexLine parses a single line from the index file into a Record.
// rec is passed in to allow re-using Record for perf. can be nil
// For inline records (offset="_"), the caller must set rec.Offset to the
// actual byte position in the index file where the data starts.
func ParseIndexLine(line string, rec *Record) error {
	var parts [5]string
	n := splitFields(line, &parts)
	if n < 4 {
		return fmt.Errorf("invalid index line: %s", line)
	}

	var err error
	var isInline bool
	var isFile bool
	switch parts[0] {
	case "_":
		// Inline data - offset will be set by caller based on file position
		isInline = true
		rec.offset = 0 // will be set by caller
	case "f":
		// File data - data is stored in a separate file, fileName is in meta field
		isFile = true
		rec.offset = kOffsetFileMeatDataZero
	default:
		rec.offset, err = strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			return fmt.Errorf("invalid offset '%s' in index line: %s", parts[0], line)
		}
		if rec.offset < 0 {
			return fmt.Errorf("invalid offset '%s' in index line: %s", parts[0], line)
		}
	}

	size, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return fmt.Errorf("invalid size '%s' in index line: %s", parts[1], line)
	}
	if size < 0 {
		return fmt.Errorf("invalid size '%s' in index line: %s", parts[1], line)
	}
	if isInline || isFile {
		rec.size = -size // negative size indicates inline
	} else {
		rec.size = size
	}

	rec.TimestampMs, err = strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		return fmt.Errorf("invalid timestamp '%s' in index line: %s", parts[2], line)
	}
	if rec.TimestampMs < 0 {
		return fmt.Errorf("invalid timestamp '%s' in index line: %s", parts[2], line)
	}

	rec.Kind = parts[3]
	rec.metaOrFileName = "" // possibly reusing rec so needs to reset
	if n > 4 {
		rec.metaOrFileName = parts[4]
	}
	// For file records, meta field must contain fileName
	if isFile && rec.metaOrFileName == "" {
		return fmt.Errorf("file record missing fileName in index line: %s", line)
	}
	return nil
}

type RecordData struct {
	Rec  *Record
	Data []byte
}

// ParseIndexFromFile returns an iterator over records parsed from an index file.
// For inline records, Data contains the inline data. For other records, Data is nil.
// If internKind is not nil, it will be called to intern the Kind string.
// Call the returned error function after iteration to check for parse errors.
func ParseIndexFromFile(path string, internKind func(string) string) (iter.Seq[RecordData], func() error) {
	var iterErr error

	seq := func(yield func(RecordData) bool) {
		file, err := os.Open(path)
		if err != nil {
			iterErr = err
			return
		}
		defer file.Close()

		reader := bufio.NewReader(file)
		var currentOffset int64 = 0

		for {
			line, err := reader.ReadString('\n')
			if err == io.EOF {
				if line == "" {
					break
				}
			} else if err != nil {
				iterErr = fmt.Errorf("error reading index file: %w", err)
				return
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
				iterErr = err
				return
			}

			reader.Size()
			if internKind != nil {
				rec.Kind = internKind(rec.Kind)
			}

			var data []byte
			if rec.isInline() {
				offset := currentOffset + lineLen
				if rec.IsFile() {
					rec.offset = -offset
				} else {
					rec.offset = offset
				}
				size := rec.Size()
				currentOffset += lineLen + size
				if size > 0 {
					data = make([]byte, size)
					_, err = io.ReadFull(reader, data)
					if err != nil {
						iterErr = fmt.Errorf("error reading inline data: %w", err)
						return
					}
				}
				// Skip trailing newline added for readability (if present)
				nextByte, err := reader.Peek(1)
				if err == nil && len(nextByte) > 0 && nextByte[0] == '\n' {
					reader.ReadByte()
					currentOffset++
				}
			} else {
				currentOffset += lineLen
			}

			if !yield(RecordData{Rec: rec, Data: data}) {
				return
			}
		}
	}

	return seq, func() error { return iterErr }
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

// ReadFile reads the data for a given record without locking the store.
func (s *Store) ReadFile(r *Record) ([]byte, error) {
	if !r.IsFile() {
		return nil, fmt.Errorf("not a file record")
	}
	filePath := filepath.Join(s.DataDir, r.FileName())
	return os.ReadFile(filePath)
}

// ReadRecord reads the data for a given record.
// For inline records (isInline()=true), reads from the index file.
// For file records (IsFile()=true), reads from the specified file in DataDir.
// For regular records, reads from the data file.
func (s *Store) ReadRecord(r *Record) ([]byte, error) {
	size := r.Size()
	if size == 0 {
		return nil, nil
	}
	if r.isInline() {
		return readFilePart(s.indexFilePath, r.Offset(), size)
	}
	if r.offset < 0 {
		return nil, nil
	}
	return readFilePart(s.dataFilePath, r.Offset(), size)
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

	records, errFn := ParseIndexFromFile(s.indexFilePath, s.internKind)
	for rd := range records {
		if s.OnRecord != nil {
			d := rd.Data
			s.OnRecord(rd.Rec, d)
		}
	}
	if err := errFn(); err != nil {
		return fmt.Errorf("failed to read records from index file: %w", err)
	}

	return nil
}
