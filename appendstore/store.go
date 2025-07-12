package appendstore

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type Record struct {
	Offset    int64  // Offset in the data file where the record starts, -1 means no data for this record, just Kind / Meta
	Length    int64  // Length of the record in bytes
	Timestamp int64  // in utc unix time format, seconds since January 1, 1970, 00:00:00 UTC)
	Kind      string // Kind of the record (e.g., "data", "metadata"). Can use to identify the type of data stored.
	Meta      string // Optional metadata associated with the record, cannot contain newlines
}

type Store struct {
	DataDir       string
	IndexFileName string
	DataFileName  string

	indexFilePath string
	dataFilePath  string
	records       []*Record // In-memory cache of records, can be used for quick access
	mu            sync.Mutex
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
func appendToFileRobust(path string, data []byte) (int64, error) {
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

func (s *Store) AppendRecord(kind string, data []byte, meta string) (*Record, error) {
	// kind cannot be empty or ontain spaces
	if kind == "" {
		return nil, fmt.Errorf("kind is empty")
	}
	if strings.Contains(kind, " ") {
		return nil, fmt.Errorf("kind cannot contain spaces")
	}
	if strings.Contains(kind, "\n") {
		return nil, fmt.Errorf("kind cannot contain newlines")
	}
	if strings.Contains(meta, "\n") {
		return nil, fmt.Errorf("metadata cannot contain newlines")
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	rec := &Record{}
	var err error
	if len(data) > 0 {
		rec.Offset, err = appendToFileRobust(s.dataFilePath, data)
		if err != nil {
			return nil, err
		}
	} else {
		rec.Offset = -1 // No data for this record
	}
	rec.Length = int64(len(data))
	rec.Timestamp = time.Now().UTC().Unix()

	// format of the index line:
	// <offset> <length> <timestamp> <kind> <meta>
	rec.Meta = meta
	rec.Kind = kind
	var indexLine string
	if rec.Meta == "" {
		indexLine = fmt.Sprintf("%d %d %d %s\n", rec.Offset, rec.Length, rec.Timestamp, rec.Kind)
	} else {
		indexLine = fmt.Sprintf("%d %d %d %s %s\n", rec.Offset, rec.Length, rec.Timestamp, rec.Kind, rec.Meta)
	}
	_, err = appendToFileRobust(s.indexFilePath, []byte(indexLine))
	if err != nil {
		return nil, err
	}
	s.records = append(s.records, rec)
	return rec, nil
}

// perf: re-using Record
func parseIndexLine(line string, res *Record) error {
	n, err := fmt.Sscanf(line, "%d %d %d %s %s", &res.Offset, &res.Length, &res.Timestamp, &res.Kind, &res.Meta)
	if err != nil {
		if n == 4 {
			res.Meta = ""
		} else {
			fmt.Fprintf(os.Stderr, "failed to parse index line: '%s', error: %v\n", line, err)
			return err
		}
	}
	if res.Offset < -1 || res.Length < 0 || res.Timestamp < 0 {
		return fmt.Errorf("invalid index line: %s", line)
	}
	return nil
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
	if r.Offset == -1 {
		return nil, nil
	}
	// TODO: not sure if this is needed
	s.mu.Lock()
	defer s.mu.Unlock()
	return readFilePart(s.dataFilePath, r.Offset, r.Length)
}

func readAllRecords(path string) ([]*Record, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var line string
	var records []*Record
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		rec := &Record{}
		line = scanner.Text()
		err = parseIndexLine(line, rec)
		if err != nil {
			return nil, fmt.Errorf("failed to parse index line: %s, error: %w", line, err)
		}
		records = append(records, rec)
	}

	// Check for scanning errors (e.g., corrupted file)
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading file: %w", err)
	}

	return records, nil
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
