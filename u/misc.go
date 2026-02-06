package u

import (
	"fmt"
	"mime"
	"net/http"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

// from https://gist.github.com/hyg/9c4afcd91fe24316cbf0
func OpenBrowser(url string) error {
	var err error

	switch runtime.GOOS {
	case "linux":
		err = exec.Command("xdg-open", url).Start()
	case "windows":
		err = exec.Command("rundll32", "url.dll,FileProtocolHandler", url).Start()
	case "darwin":
		err = exec.Command("open", url).Start()
	default:
		err = fmt.Errorf("unsupported platform")
	}
	return err
}

// WaitForServerReady waits up to 10 secs for a given url to return
func WaitForServerReady(uri string) error {
	c := *http.DefaultClient
	c.Timeout = time.Second * 2
	var err error
	for range 10 {
		var resp *http.Response
		resp, err = c.Get(uri)
		if err == nil && resp.StatusCode == http.StatusOK {
			resp.Body.Close()
			return nil
		}
		time.Sleep(time.Second * 1)
	}
	return nil
}

// FormatSize formats a number in a human-readable form e.g. 1.24 kB
func FormatSize(n int64) string {
	sizes := []int64{1024 * 1024 * 1024, 1024 * 1024, 1024}
	suffixes := []string{"GB", "MB", "kB"}
	for i, size := range sizes {
		if n >= size {
			s := fmt.Sprintf("%.2f", float64(n)/float64(size))
			return strings.TrimSuffix(s, ".00") + " " + suffixes[i]
		}
	}
	return fmt.Sprintf("%d bytes", n)
}

// Percent returns how many percent of total is sub
// 100% means total == sub, 50% means sub = total / 2
func Percent(total, sub int64) float64 {
	return float64(sub) * 100 / float64(total)
}

// time.Duration with a better string representation
type FormattedDuration time.Duration

func (d FormattedDuration) String() string {
	return FormatDuration(time.Duration(d))
}

// Formats duration in a more human friendly way
// than time.Duration.String()
func FormatDuration(d time.Duration) string {
	s := d.String()
	if strings.HasSuffix(s, "µs") {
		// for µs we don't want fractions
		parts := strings.Split(s, ".")
		if len(parts) > 1 {
			return parts[0] + " µs"
		}
		return strings.ReplaceAll(s, "µs", " µs")
	} else if strings.HasSuffix(s, "ms") {
		// for ms we only want 2 digit fractions
		parts := strings.Split(s, ".")
		//fmt.Printf("fmtDur: '%s' => %#v\n", s, parts)
		if len(parts) > 1 {
			s2 := parts[1]
			if len(s2) > 4 {
				// 2 for "ms" and 2+ for fraction
				res := parts[0] + "." + s2[:2] + " ms"
				//fmt.Printf("fmtDur: s2: '%s', res: '%s'\n", s2, res)
				return res
			}
		}
		return strings.ReplaceAll(s, "ms", " ms")
	}
	return s
}

var mimeTypes = map[string]string{
	// not present in mime.TypeByExtension()
	".txt": "text/plain",
	".exe": "application/octet-stream",

	// a copy from mime.TypeByExtension()
	// this is because on Windows Go uses registry first
	// and registry can have bad content type
	// (e.g. on Win 10 I got text/plain for .js)
	".avif":        "image/avif",
	".css":         "text/css; charset=utf-8",
	".gif":         "image/gif",
	".htm":         "text/html; charset=utf-8",
	".html":        "text/html; charset=utf-8",
	".jpeg":        "image/jpeg",
	".jpg":         "image/jpeg",
	".js":          "text/javascript; charset=utf-8",
	".json":        "application/json",
	".mjs":         "text/javascript; charset=utf-8",
	".pdf":         "application/pdf",
	".png":         "image/png",
	".svg":         "image/svg+xml",
	".wasm":        "application/wasm",
	".webp":        "image/webp",
	".xml":         "text/xml; charset=utf-8",
	".webmanifest": "application/manifest+json",
}

func MimeTypeFromFileName(path string) string {
	ext := strings.ToLower(filepath.Ext(path))
	ct := mimeTypes[ext]
	if ct == "" {
		ct = mime.TypeByExtension(ext)
	}
	if ct == "" {
		// if all else fails
		ct = "application/octet-stream"
	}
	return ct
}

// Slug generates safe url from string by removing hazardous characters
func Slug(s string) string {
	return slug(s, true)
}

func SlugNoLowerCase(s string) string {
	return slug(s, false)
}

func slug(s string, lowerCase bool) string {
	// whitelisted characters valid in url
	isAlpha := func(c rune) bool {
		if c >= 'a' && c <= 'z' {
			return true
		}
		if c >= 'A' && c <= 'Z' {
			return true
		}
		if c >= '0' && c <= '9' {
			return true
		}
		return false
	}
	toValid := func(c rune) byte {
		if isAlpha(c) {
			return byte(c)
		}
		if c == '-' || c == '_' || c == '.' {
			return byte(c)
		}
		if c == ' ' {
			return '-'
		}
		return 0
	}

	charCanRepeat := func(c byte) bool {
		return isAlpha(rune(c))
	}

	s = strings.TrimSpace(s)
	if lowerCase {
		s = strings.ToLower(s)
	}
	var res []byte
	for _, r := range s {
		c := toValid(r)
		if c == 0 {
			continue
		}
		// eliminate duplicate consecutive characters
		var prev byte
		if len(res) > 0 {
			prev = res[len(res)-1]
		}
		if c == prev && !charCanRepeat(c) {
			continue
		}
		res = append(res, c)
	}
	s = string(res)
	if len(s) > 128 {
		s = s[:128]
	}
	return s
}

// get date and hash of current git checkin
func GetGitHashDateMust() (string, string) {
	// git log --pretty=format:"%h %ad %s" --date=short -1
	cmd := exec.Command("git", "log", "-1", `--pretty=format:%h %ad %s`, "--date=short")
	out, err := cmd.Output()
	PanicIf(err != nil, "git log failed")
	s := strings.TrimSpace(string(out))
	//logf("exec out: '%s'\n", s)
	parts := strings.SplitN(s, " ", 3)
	PanicIf(len(parts) != 3, "expected 3 parts in '%s'", s)
	return parts[0], parts[1] // hashShort, date
}
