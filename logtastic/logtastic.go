package logtastic

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/carlmjohnson/requests"
	"github.com/kjk/common/filerotate"
	"github.com/kjk/common/httputil"
	"github.com/kjk/common/siserlogger"
)

type op struct {
	uri  string
	mime string
	d    []byte
}

const (
	// how long to wait before we resume sending logs to the server
	// after a failure. doesn't affect logging to files
	throttleTimeout = time.Second * 15

	kPleaseStop = "please-stop"
)

var (
	Server         = ""
	ApiKey         = ""
	LogDir         = ""
	FileLogs       *filerotate.File
	FileErrors     *siserlogger.File
	FileEvents     *siserlogger.File
	FileHits       *siserlogger.File
	throttleUntil  time.Time
	ch             = make(chan op, 1000)
	startLogWorker sync.Once
)

func ctx() context.Context {
	return context.Background()
}

func logf(s string, args ...interface{}) {
	if len(args) > 0 {
		s = fmt.Sprintf(s, args...)
	}
	fmt.Print(s)
}

func logtasticWorker() {
	logf("logtasticWorker started\n")
	for op := range ch {
		// logfLocal("logtasticPOST %s\n", op.uri)
		uri := op.uri
		if uri == kPleaseStop {
			break
		}
		d := op.d
		mime := op.mime
		r := requests.
			URL(uri).
			BodyBytes(d).
			ContentType(mime)
		if ApiKey != "" {
			r = r.Header("X-Api-Key", ApiKey)
		}
		ctx, cancel := context.WithTimeout(ctx(), time.Second*10)
		err := r.Fetch(ctx)
		cancel()
		if err != nil {
			logf("logtasticPOST %s failed: %v, will throttle for %s\n", uri, err, throttleTimeout)
			throttleUntil = time.Now().Add(throttleTimeout)
		}
	}
	logf("logtasticWorker stopped\n")
}

func Stop() {
	Server = ""
	ch <- op{uri: kPleaseStop}
	if FileLogs != nil {
		FileLogs.Close()
	}
	if FileErrors != nil {
		FileErrors.Close()
	}
	if FileEvents != nil {
		FileEvents.Close()
	}
	if FileHits != nil {
		FileHits.Close()
	}
}

func logtasticPOST(uriPath string, d []byte, mime string) {
	if Server == "" {
		return
	}
	startLogWorker.Do(func() {
		go logtasticWorker()
	})

	throttleLeft := time.Until(throttleUntil)
	if throttleLeft > 0 {
		logf(" skipping because throttling for %s\n", throttleLeft)
		return
	}

	uri := "http://" + Server + uriPath
	// logfLocal("logtasticPOST %s\n", uri)
	op := op{
		uri:  uri,
		mime: mime,
		d:    d,
	}

	select {
	case ch <- op:
	default:
		logf("logtasticPOST %s failed: channel full\n", uri)
	}
}

const (
	mimeJSON      = "application/json"
	mimePlainText = "text/plain"
)

func writeLog(d []byte) {
	if LogDir == "" {
		return
	}
	if FileLogs == nil {
		var err error
		FileLogs, err = filerotate.NewDaily(LogDir, "log", nil)
		if err != nil {
			logf("failed to open log file logs: %v\n", err)
			return
		}
	}
	FileLogs.Write2(d, true)
}

func writeSiserLog(name string, lPtr **siserlogger.File, d []byte) {
	if LogDir == "" {
		return
	}
	if *lPtr == nil {
		l, err := siserlogger.NewDaily(LogDir, name, nil)
		if err != nil {
			logf("failed to open log file %s: %v\n", name, err)
			return
		}
		*lPtr = l
	}
	// logf("writeSiserLog %s: %s\n", name, limitString(string(d), 100))
	(*lPtr).Write(d)
}

func Log(s string) {
	d := []byte(s)
	writeLog(d)
	logtasticPOST("/api/v1/log", d, mimePlainText)
}

func LogHit(r *http.Request, code int, size int64, dur time.Duration) {
	m := map[string]interface{}{}
	httputil.GetRequestInfo(r, m)
	if dur > 0 {
		m["dur_ms"] = float64(dur) / float64(time.Millisecond)
	}
	if code >= 400 {
		m["status"] = code
	}
	if size > 0 {
		m["size"] = size
	}

	d, _ := json.Marshal(m)
	writeSiserLog("hit", &FileHits, d)

	logtasticPOST("/api/v1/hit", d, mimeJSON)
}

func LogEvent(r *http.Request, m map[string]interface{}) {
	if r != nil {
		httputil.GetRequestInfo(r, m)
	}

	d, _ := json.Marshal(m)
	writeSiserLog("event", &FileEvents, d)

	logtasticPOST("/api/v1/event", d, mimeJSON)
}

func HandleEvent(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	d, err := io.ReadAll(r.Body)
	if err != nil {
		logf("failed to read body: %v\n", err)
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}
	// we validate it's json and agument it with ip of the user's browser
	var m map[string]interface{}
	err = json.Unmarshal(d, &m)
	if err != nil {
		logf("HandleEvent: json.Unmarshal() failed with '%s'\nbody:\n%s\n", err, limitString(string(d), 100))
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}
	LogEvent(r, m)
}

// TODO: send callstack as a separate field
// TODO: send server build hash so we can auto-link callstack lines
// to	source code on github
func LogError(r *http.Request, s string) {
	writeSiserLog("errors", &FileErrors, []byte(s))

	m := map[string]interface{}{}
	if r != nil {
		httputil.GetRequestInfo(r, m)
	}
	m["msg"] = s
	d, _ := json.Marshal(m)
	logtasticPOST("/api/v1/error", d, mimeJSON)
}

func limitString(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}
