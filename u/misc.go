package u

import (
	"context"
	"fmt"
	"log"
	"mime"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"
)

// from https://gist.github.com/hyg/9c4afcd91fe24316cbf0
func OpenBrowser(url string) {
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
	if err != nil {
		log.Fatal(err)
	}
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
	".avif": "image/avif",
	".css":  "text/css; charset=utf-8",
	".gif":  "image/gif",
	".htm":  "text/html; charset=utf-8",
	".html": "text/html; charset=utf-8",
	".jpeg": "image/jpeg",
	".jpg":  "image/jpeg",
	".js":   "text/javascript; charset=utf-8",
	".json": "application/json",
	".mjs":  "text/javascript; charset=utf-8",
	".pdf":  "application/pdf",
	".png":  "image/png",
	".svg":  "image/svg+xml",
	".wasm": "application/wasm",
	".webp": "image/webp",
	".xml":  "text/xml; charset=utf-8",
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

func UpdateGoDeps(dir string, noProxy bool) error {
	{
		cmd := exec.Command("go", "get", "-u", ".")
		cmd.Dir = dir
		if noProxy {
			cmd.Env = append(os.Environ(), "GOPROXY=direct")
		}
		fmt.Printf("running: %s in dir '%s'\n", cmd.String(), cmd.Dir)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		err := cmd.Run()
		if err != nil {
			return err
		}
	}
	{
		cmd := exec.Command("go", "mod", "tidy")
		cmd.Dir = dir
		fmt.Printf("running: %s in dir '%s'\n", cmd.String(), cmd.Dir)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		err := cmd.Run()
		return err
	}
}

func WaitForSigIntOrKill() {
	// Ctrl-C sends SIGINT
	ctx := context.Background()
	sctx, stop := signal.NotifyContext(ctx, os.Interrupt /*SIGINT*/, os.Kill /* SIGKILL */, syscall.SIGTERM)
	defer stop()
	<-sctx.Done()
}
