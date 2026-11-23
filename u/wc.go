package u

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
)

// LineCount describes line count for a file
type LineCount struct {
	Name      string
	Ext       string
	LineCount int
}

// LineStats gathers line count info for files
type LineStats struct {
	FileToCount map[string]*LineCount
}

// NewLineStats returns new LineStats
func NewLineStats() *LineStats {
	return &LineStats{
		FileToCount: map[string]*LineCount{},
	}
}

func LineStatsPerExt(fileToCount map[string]*LineCount) []*LineCount {
	extToCount := map[string]*LineCount{}
	for _, wc := range fileToCount {
		ext := wc.Ext
		extWc := extToCount[ext]
		if extWc == nil {
			extWc = &LineCount{
				Ext: ext,
			}
			extToCount[ext] = extWc
		}
		extWc.LineCount += wc.LineCount
	}
	var res []*LineCount
	for _, wc := range extToCount {
		res = append(res, wc)
	}
	sort.Slice(res, func(i, j int) bool {
		wc1 := res[i]
		wc2 := res[j]
		return wc1.LineCount < wc2.LineCount
	})
	return res
}

type FilterFunc func(string) bool

func MakeAllowedFileFilterForExts(exts ...string) FilterFunc {
	// don't modify caller's slice
	exts = slices.Clone(exts)
	for i, ext := range exts {
		exts[i] = strings.ToLower(ext)
	}

	return func(path string) bool {
		fext := strings.ToLower(filepath.Ext(path))
		return slices.Contains(exts, fext)
	}
}

func MakeExcludeDirsFilter(dirs ...string) FilterFunc {
	return func(path string) bool {
		// path starts as a file path
		// we only compare directory names
		path = filepath.Dir(path)
		for len(path) > 0 {
			if path == "." {
				return true
			}
			name := filepath.Base(path)
			if slices.Contains(dirs, name) {
				return false
			}
			parent := filepath.Dir(path)
			if parent == path {
				// reached root of absolute path e.g. "/" or "C:\"
				break
			}
			path = parent
		}
		return true
	}
}

func MakeFilterOr(filters ...FilterFunc) FilterFunc {
	return func(name string) bool {
		for _, f := range filters {
			if f(name) {
				return true
			}
		}
		return false
	}
}

func MakeFilterAnd(filters ...FilterFunc) FilterFunc {
	return func(name string) bool {
		for _, f := range filters {
			if !f(name) {
				return false
			}
		}
		return true
	}
}

// FileLineCount returns number of lines in a file.
// Handles LF, CRLF and CR line endings. A final line without a newline
// is counted, a trailing newline doesn't start a new line
// i.e. "a\nb" and "a\nb\n" both have 2 lines.
func FileLineCount(path string) (int, error) {
	d, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	return dataLineCount(d), nil
}

func dataLineCount(d []byte) int {
	n := len(d)
	nLines := 0
	for i := 0; i < n; i++ {
		switch d[i] {
		case '\n':
			nLines++
		case '\r':
			nLines++
			// CRLF is a single line ending
			if i+1 < n && d[i+1] == '\n' {
				i++
			}
		}
	}
	// last line without newline at the end
	if n > 0 && d[n-1] != '\n' && d[n-1] != '\r' {
		nLines++
	}
	return nLines
}

func (s *LineStats) CalcInDir(dir string, allowedFileFilter func(name string) bool, recur bool) error {
	files, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, fi := range files {
		name := fi.Name()
		path := filepath.Join(dir, name)
		if fi.IsDir() {
			if recur {
				if err := s.CalcInDir(path, allowedFileFilter, recur); err != nil {
					return err
				}
			}
			continue
		}
		if !fi.Type().IsRegular() {
			continue
		}
		if !allowedFileFilter(path) {
			continue
		}
		lineCount, err := FileLineCount(path)
		if err != nil {
			return err
		}
		s.FileToCount[path] = &LineCount{
			Name:      fi.Name(),
			Ext:       strings.ToLower(filepath.Ext(name)),
			LineCount: lineCount,
		}
	}
	return nil
}

func PrintLineStats(stats *LineStats) {
	var files []string
	for k := range stats.FileToCount {
		files = append(files, k)
	}
	sort.Strings(files)
	total := 0
	for _, f := range files {
		wc := stats.FileToCount[f]
		fmt.Printf("% 6d %s\n", wc.LineCount, f)
		total += wc.LineCount
	}
	fmt.Printf("\nPer extension:\n")
	wcPerExt := LineStatsPerExt(stats.FileToCount)
	for _, wc := range wcPerExt {
		fmt.Printf("%d %s\n", wc.LineCount, wc.Ext)
	}
	fmt.Printf("\ntotal: %d\n", total)
}
