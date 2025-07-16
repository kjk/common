package appendstore

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Record struct {
	// offset in data file, 0 means no data
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
}

type Store struct {
	DataDir       string
	IndexFileName string
	DataFileName  string

	indexFilePath string
	dataFilePath  string
	records       []*Record

	// when over-writing a record, we expand the data by this much to minimize
	// the amount written to file.
	// 0 means no expansion.
	// 40 means we expand the data by 40%
	// 100 means we expand the data by 100%
	OverWriteDataExpandPercent int
	mu                         sync.Mutex
}

// no direct access to records to ensure thread safety
func (s *Store) Records() []*Record {
	s.mu.Lock()
	res := append([]*Record{}, s.records...)
	s.mu.Unlock()
	return res
}

// returns offset at which the data was written
// we write len(data) bytes
func appendToFileRobust(path string, data []byte, additinalBytes int) (int64, error) {
	// get file size
	info, err := os.Stat(path)
	if err != nil && !os.IsNotExist(err) {
		return 0, err
	}
	var offset int64 = 0 // if file does not exist, offset is 0
	if info != nil {
		offset = info.Size()
	}

	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return 0, err
	}
	_, err = file.Write(data)
	if err != nil {
		file.Close()
		return 0, err
	}
	if additinalBytes > 0 {
		d := make([]byte, additinalBytes)
		for i := 0; i < additinalBytes; i++ {
			d[i] = 32 // fill with spaces
		}
		_, err = file.Write(d)
		if err != nil {
			file.Close()
			return 0, err
		}
	}
	err = file.Sync()
	if err != nil {
		file.Close()
		return 0, err
	}
	err = file.Close()
	if err != nil {
		return 0, err
	}
	return offset, nil
}

func writeToFilAtOffset(path string, offset int64, data []byte) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.Seek(offset, io.SeekStart)
	if err != nil {
		return err
	}
	_, err = file.Write(data)
	return err
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

func (s *Store) OverwriteRecord(kind string, data []byte, meta string) error {
	if err := validateKindAndMeta(kind, meta); err != nil {
		return err
	}
	if len(data) == 0 {
		return s.AppendRecord(kind, nil, meta)
	}

	// find a record that we can overwrite
	recToOverwrite := -1
	neededSize := int64(len(data))
	for i, rec := range s.records {
		if rec.Kind == kind && rec.Meta == meta && rec.SizeInFile >= neededSize {
			recToOverwrite = i
			break
		}
	}
	if recToOverwrite == -1 {
		// no record to overwrite, append a new one with potentially padding
		// for future overwrites
		additionalBytes := (neededSize * int64(s.OverWriteDataExpandPercent)) / 100
		return s.appendRecord(kind, data, meta, int(additionalBytes))
	}

	offset := s.records[recToOverwrite].Offset
	writeToFilAtOffset(s.dataFilePath, offset, data)

	rec := &Record{
		Offset:     offset,
		Size:       int64(len(data)),
		SizeInFile: 0,
		Kind:       kind,
		Meta:       meta,
	}
	indexLine := serializeRecord(rec)
	_, err := appendToFileRobust(s.indexFilePath, []byte(indexLine), 0)
	if err != nil {
		return err
	}
	s.records = append(s.records, rec)
	return nil
}

// format of the index line:
// <offset> <length>:[<length in file>] <timestamp> <kind> [<meta>]
func serializeRecord(rec *Record) string {
	sz := ""
	if rec.SizeInFile > 0 {
		sz = fmt.Sprintf("%d:%d", rec.Size, rec.SizeInFile)
	} else {
		sz = fmt.Sprintf("%d", rec.Size)
	}
	rec.TimestampMs = time.Now().UTC().UnixMilli()
	t := rec.TimestampMs
	if rec.Meta == "" {
		return fmt.Sprintf("%d %s %d %s\n", rec.Offset, sz, t, rec.Kind)
	}
	return fmt.Sprintf("%d %s %d %s %s\n", rec.Offset, sz, t, rec.Kind, rec.Meta)
}

func (s *Store) appendRecord(kind string, data []byte, meta string, additionalBytes int) error {
	if err := validateKindAndMeta(kind, meta); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	size := int64(len(data))
	rec := &Record{
		Size: size,
		Kind: kind,
		Meta: meta,
	}
	var err error
	if size > 0 {
		rec.Offset, err = appendToFileRobust(s.dataFilePath, data, additionalBytes)
		if err != nil {
			return err
		}
	}
	if additionalBytes > 0 {
		rec.SizeInFile = rec.Size + int64(additionalBytes)
	}

	indexLine := serializeRecord(rec)
	_, err = appendToFileRobust(s.indexFilePath, []byte(indexLine), 0)
	if err != nil {
		return err
	}
	s.records = append(s.records, rec)
	return nil
}

func (s *Store) AppendRecord(kind string, data []byte, meta string) error {
	return s.appendRecord(kind, data, meta, 0)
}

// perf: allow re-using Record
func ParseIndexLine(line string, rec *Record) error {
	parts := strings.SplitN(line, " ", 5)
	if len(parts) < 4 {
		return fmt.Errorf("invalid index line: %s", line)
	}

	var err error
	rec.Offset, err = strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return fmt.Errorf("invalid offset '%s' in index line: %s", parts[0], line)
	}
	if rec.Offset < 0 {
		return fmt.Errorf("invalid offset '%s' in index line: %s", parts[0], line)
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

func ParseIndexFromData(d []byte) ([]*Record, error) {
	scanner := bufio.NewScanner(strings.NewReader(string(d)))
	return ParseIndexFromScanner(scanner)
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

func (s *Store) ReadRecord(r *Record) ([]byte, error) {
	if r.Offset < 0 || r.Size == 0 {
		return nil, nil
	}
	// TODO: not sure if this is needed
	s.mu.Lock()
	defer s.mu.Unlock()
	return readFilePart(s.dataFilePath, r.Offset, r.Size)
}

func readAllRecords(path string) ([]*Record, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	return ParseIndexFromScanner(scanner)
}

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

	s.records, err = readAllRecords(s.indexFilePath)
	if err != nil {
		return fmt.Errorf("failed to read records from index file: %w", err)
	}
	return nil
}
