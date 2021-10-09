package httplogger

import (
	"fmt"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/kjk/common/filerotate"
	"github.com/kjk/common/siser"
)

type Logger struct {
	rec   siser.Record // re-usable for performance
	siser *siser.Writer
	file  *filerotate.File
	mu    sync.Mutex

	dir string
}

func New(dir string, didRotateFn func(path string)) (*Logger, error) {
	absDir, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}

	res := &Logger{
		dir: absDir,
	}

	didRotateInternal := func(path string, didRotate bool) {
		if didRotate && didRotateFn != nil {
			didRotateFn(path)
		}
	}

	newLogHourly := func(dir string, didClose func(path string, didRotate bool)) (*filerotate.File, error) {
		hourly := func(creationTime time.Time, now time.Time) string {
			if filerotate.IsSameHour(creationTime, now) {
				return ""
			}
			name := "httplog-" + now.Format("2006-01-02_15") + ".txt"
			path := filepath.Join(dir, name)
			// logf(ctx(), "NewLogHourly: '%s'\n", path)
			return path
		}
		config := filerotate.Config{
			DidClose:           didClose,
			PathIfShouldRotate: hourly,
		}
		return filerotate.New(&config)
	}

	res.file, err = newLogHourly(absDir, didRotateInternal)
	if err != nil {
		return nil, err
	}

	res.siser = siser.NewWriter(res.file)
	return res, nil
}

func (l *Logger) Close() error {
	err := l.file.Close()
	l.file = nil
	return err
}

// some headers and not worth logging
var (
	hdrsToNotLog = []string{
		"Connection",
		"Sec-Ch-Ua-Mobile",
		"Sec-Fetch-Dest",
		"Sec-Ch-Ua-Platform",
		"Dnt",
		"Upgrade-Insecure-Requests",
		"Sec-Fetch-Site",
		"Sec-Fetch-Mode",
		"Sec-Fetch-User",
		"If-Modified-Since",
		"Accept-Language",
		"Cf-Ray",
		"CF-Visitor",
		"X-Request-Start",
		"Cdn-Loop",
		"X-Forwarded-Proto",
	}
	hdrsToNotLogMap map[string]bool
)

func shouldLogHeader(s string) bool {
	if hdrsToNotLogMap == nil {
		hdrsToNotLogMap = map[string]bool{}
		for _, h := range hdrsToNotLog {
			h = strings.ToLower(h)
			hdrsToNotLogMap[h] = true
		}
	}
	s = strings.ToLower(s)
	return !hdrsToNotLogMap[s]
}

func (l *Logger) LogReq(r *http.Request, code int, size int64, dur time.Duration) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.siser == nil {
		return nil
	}

	rec := &l.rec
	rec.Reset()
	rec.Write("req", fmt.Sprintf("%s %s %d", r.Method, r.RequestURI, code))
	rec.WriteNonEmpty("host", r.Host)
	rec.WriteNonEmpty("ipaddr", requestGetRemoteAddress(r))
	rec.Write("size", strconv.FormatInt(size, 10))
	durMicro := int64(dur / time.Microsecond)
	rec.Write("durmicro", strconv.FormatInt(durMicro, 10))

	// to minimize logging, we don't log headers if this is self-referal
	skipLoggingHeaders := func() bool {
		ref := r.Header.Get("Referer")
		if ref == "" {
			return false
		}
		return strings.Contains(ref, r.Host)
	}

	if !skipLoggingHeaders() {
		for k, v := range r.Header {
			if shouldLogHeader(k) && len(v) > 0 {
				rec.WriteNonEmpty(k, v[0])
			}
		}
	}

	_, err := l.siser.WriteRecord(rec)
	return err
}

// requestGetRemoteAddress returns ip address of the client making the request,
// taking into account http proxies
func requestGetRemoteAddress(r *http.Request) string {
	hdr := r.Header
	hdrRealIP := hdr.Get("x-real-ip")
	hdrForwardedFor := hdr.Get("x-forwarded-for")
	// Request.RemoteAddress contains port, which we want to remove i.e.:
	// "[::1]:58292" => "[::1]"
	ipAddrFromRemoteAddr := func(s string) string {
		idx := strings.LastIndex(s, ":")
		if idx == -1 {
			return s
		}
		return s[:idx]
	}
	if hdrRealIP == "" && hdrForwardedFor == "" {
		return ipAddrFromRemoteAddr(r.RemoteAddr)
	}
	if hdrForwardedFor != "" {
		// X-Forwarded-For is potentially a list of addresses separated with ","
		parts := strings.Split(hdrForwardedFor, ",")
		for i, p := range parts {
			parts[i] = strings.TrimSpace(p)
		}
		// TODO: should return first non-local address
		return parts[0]
	}
	return hdrRealIP
}

// <dir>/httplog-2021-10-06_01.txt.br
// =>
//apps/cheatsheet/httplog/2021/10-06/2021-10-06_01.txt.br
// return "" if <path> is in unexpected format
func RemotePathFromFilePath(app, path string) string {
	name := filepath.Base(path)
	parts := strings.Split(name, "_")
	if len(parts) != 2 {
		return ""
	}
	// parts[1]: 01.txt.br
	hr := strings.Split(parts[1], ".")[0]
	if len(hr) != 2 {
		return ""
	}
	// parts[0]: httplog-2021-10-06
	parts = strings.Split(parts[0], "-")
	if len(parts) != 4 {
		return ""
	}
	year := parts[1]
	month := parts[2]
	day := parts[3]
	name = fmt.Sprintf("%s/%s-%s/%s-%s-%s_%s.txt.br", year, month, day, year, month, day, hr)
	return fmt.Sprintf("apps/%s/httplog/%s", app, name)
}
