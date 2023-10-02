package u

import (
	"os"
	"path/filepath"
	"strings"
)

// NormalizeNewlinesInPlace changes CRLF (Windows) and
// CR (Mac) to LF (Unix)
// Optimized for speed, modifies data in place
func NormalizeNewlinesInPlace(d []byte) []byte {
	wi := 0
	n := len(d)
	for i := 0; i < n; i++ {
		c := d[i]
		// 13 is CR
		if c != 13 {
			d[wi] = c
			wi++
			continue
		}
		// replace CR (mac / win) with LF (unix)
		d[wi] = 10
		wi++
		if i < n-1 && d[i+1] == 10 {
			// this was CRLF, so skip the LF
			i++
		}

	}
	return d[:wi]
}

// NormalizeNewlines is like NormalizeNewlinesInPlace but
// slower because it makes a copy of data
func NormalizeNewlines(d []byte) []byte {
	d = append([]byte{}, d...)
	return NormalizeNewlinesInPlace(d)
}

// Capitalize does foo => Foo, BAR => Bar etc.
func Capitalize(s string) string {
	if len(s) == 0 {
		return s
	}
	s = strings.ToLower(s)
	return strings.ToUpper(s[0:1]) + s[1:]
}

// TrimPrefix is like strings.TrimPrefix but also returns a bool
// indicating that the string was trimmed
func TrimPrefix(s string, prefix string) (string, bool) {
	s2 := strings.TrimPrefix(s, prefix)
	return s2, len(s) != len(s2)
}

func ToTrimmedLines(d []byte) []string {
	lines := strings.Split(string(d), "\n")
	i := 0
	for _, l := range lines {
		l = strings.TrimSpace(l)
		// remove empty lines
		if len(l) > 0 {
			lines[i] = l
			i++
		}
	}
	return lines[:i]
}

// TrimExt removes extension from s
func TrimExt(s string) string {
	idx := strings.LastIndex(s, ".")
	if idx == -1 {
		return s
	}
	return s[:idx]
}

// ExtEqualFold returns true if s ends with extension (e.g. ".html")
// case-insensitive
func ExtEqualFold(s string, ext string) bool {
	e := filepath.Ext(s)
	return strings.EqualFold(e, ext)
}

func AppendNewline(s *string) string {
	if strings.HasSuffix(*s, "\n") {
		return *s
	}
	*s = *s + "\n"
	return *s
}

func CollapseMultipleNewlines(s string) string {
	s = strings.ReplaceAll(s, "\r\n", "\n") // CRLF => CR
	prev := ""
	for prev != s {
		prev = s
		s = strings.ReplaceAll(s, "\n\n\n", "\n\n")
	}
	return s
}

func AppendOrReplaceInText(orig string, toAppend string, delim string) string {
	AppendNewline(&toAppend)
	AppendNewline(&delim)
	content := "\n\n" + delim + toAppend + delim
	if strings.Contains(orig, content) {
		return CollapseMultipleNewlines(orig)
	}
	start := strings.Index(orig, delim)
	if start < 0 {
		return CollapseMultipleNewlines(orig + content)
	}
	end := strings.Index(orig[start+1:], delim)
	PanicIf(end == -1, "didn't find end delim")
	end += start + 1
	orig = orig[:start] + "\n\n" + orig[end+len(delim):]
	res := AppendNewline(&orig) + content
	return CollapseMultipleNewlines(res)
}

func AppendOrReplaceInFileMust(path string, toAppend string, delim string) bool {
	st, err := os.Lstat(path)
	must(err)
	perm := st.Mode().Perm()
	orig, err := os.ReadFile(path)
	must(err)
	newContent := AppendOrReplaceInText(string(orig), toAppend, delim)
	if newContent == string(orig) {
		return false
	}
	err = os.WriteFile(path, []byte(newContent), perm)
	must(err)
	return true
}

func ExpandTildeInPath(s string) string {
	if strings.HasPrefix(s, "~") {
		dir, err := os.UserHomeDir()
		must(err)
		return dir + s[1:]
	}
	return s
}

func ParseEnvMust(d []byte) map[string]string {
	d = NormalizeNewlines(d)
	s := string(d)
	lines := strings.Split(s, "\n")
	m := make(map[string]string)
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		PanicIf(len(parts) != 2, "invalid line '%s' in .env\n", line)
		key := strings.TrimSpace(parts[0])
		val := strings.TrimSpace(parts[1])
		m[key] = val
	}
	return m
}
