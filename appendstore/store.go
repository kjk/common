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

func (s *Store) Records() []*Record {
	// no direct access to records to ensure thread safety
	s.mu.Lock()
	res := append([]*Record{}, s.nonOverwritten...)
	s.mu.Unlock()
	return res
}

// for debugging
func (s *Store) AllRecords() []*Record {
	// no direct access to records to ensure thread safety
	s.mu.Lock()
	res := append([]*Record{}, s.nonOverwritten...)
	s.mu.Unlock()
	return res
}

func openFileForAppend(path string, fp **os.File) (int64, error) {
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
		_, err := openFileForAppend(s.indexFilePath, &s.indexFile)
		if err != nil {
			return fmt.Errorf("failed to open index file for appending: %w", err)
		}
	}
	if s.dataFile == nil {
		off, err := openFileForAppend(s.dataFilePath, &s.dataFile)
		if err != nil {
			s.CloseFiles()
			return err
		}
		s.currDataOffset = off
	}
	return nil
}

func (s *Store) CloseFiles() error {
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
	_, err := file.Write(data)
	if err != nil {
		return 0, err
	}
	if additionalBytes > 0 {
		d := make([]byte, additionalBytes)
		for i := 0; i < additionalBytes; i++ {
			d[i] = 32 // fill with spaces
		}
		_, err = file.Write(d)
		if err != nil {
			return 0, err
		}
	}
	if sync {
		err = file.Sync()
		if err != nil {
			return 0, err
		}
	}
	return int64(len(data) + additionalBytes), nil
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

func (s *Store) OverwriteRecord(kind string, meta string, data []byte) error {
	if len(data) == 0 {
		return s.AppendRecord(kind, meta, nil)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

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
		return s.appendRecord(kind, meta, data, int(additionalBytes))
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
		Offset:     offset,
		Size:       int64(len(data)),
		SizeInFile: 0,
		Kind:       kind,
		Meta:       meta,
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

func (s *Store) appendRecord(kind string, meta string, data []byte, additionalBytes int) error {
	if err := validateKindAndMeta(kind, meta); err != nil {
		return err
	}

	err := s.reopenFiles()
	if err != nil {
		return err
	}

	size := int64(len(data))
	rec := &Record{
		Size: size,
		Kind: kind,
		Meta: meta,
	}
	if size > 0 {
		rec.Offset = s.currDataOffset
		nWritten, err := appendToFile(s.dataFile, data, additionalBytes, s.SyncWrite)
		s.currDataOffset += nWritten
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

func (s *Store) appendToDataFile(data []byte) error {

	if err := s.reopenFiles(); err != nil {
		return err
	}

	if nWritten, err := appendToFile(s.dataFile, data, 0, s.SyncWrite); err != nil {
		return err
	} else {
		s.currDataOffset += nWritten
	}
	return nil
}

func (s *Store) AppendRecord(kind string, meta string, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.appendRecord(kind, meta, data, 0)
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

	s.allRecords, err = readAllRecords(s.indexFilePath)
	if err != nil {
		return fmt.Errorf("failed to read records from index file: %w", err)
	}

	// mark overwritten records
	m := make(map[int64]*Record)
	for _, rec := range s.allRecords {
		if rec.Size == 0 {
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
