package logtastic

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/carlmjohnson/requests"
	"github.com/kjk/common/filerotate"
	"github.com/kjk/common/httputil"
	"github.com/kjk/common/siserlogger"
	"github.com/kjk/common/u"
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
	kMaxURLLen  = 1024
)

var (
	Server           = ""
	ApiKey           = ""
	LogDir           = ""
	BuildHash        = ""
	FileLogs         *filerotate.File
	FileErrors       *siserlogger.File
	FileEvents       *siserlogger.File
	FileHits         *siserlogger.File
	throttleUntil    time.Time
	lastThrottleLog  time.Time
	logWorkerCh      = make(chan op, 1000)
	startLogWorker   sync.Once
	logWorkerStopped sync.WaitGroup
	isShuttingDown   atomic.Bool
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
	logWorkerStopped.Add(1)
	for op := range logWorkerCh {
		// logfLocal("logtasticPOST %s\n", op.uri)
		writeLog(op.d)

		uri := op.uri
		if uri == kPleaseStop {
			break
		}
		throttleLeft := time.Until(throttleUntil)
		if throttleLeft > 0 {
			if time.Since(lastThrottleLog) > time.Second*10 {
				logf(" skipping because throttling for %s\n", throttleLeft)
				lastThrottleLog = time.Now()
			}
			continue
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
	close(logWorkerCh)
	logWorkerStopped.Done()
	logf("logtasticWorker stopped\n")
}

func Stop() {
	isShuttingDown.Store(true)
	Server = ""
	logWorkerCh <- op{uri: kPleaseStop}
	logf("Stop: waiting for logWorkerStopped\n")
	logWorkerStopped.Wait()
	logf("Stop: logWorkerDidStop\n")
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

func fullURL(server string, uriPath string) string {
	httpScheme := "https://"
	if strings.Contains(server, "localhost") || strings.Contains(server, "127.0.0.01") {
		httpScheme = "http://"
	}

	return httpScheme + server + uriPath
}

func logtasticPOST(uriPath string, d []byte, mime string) {
	if Server == "" {
		return
	}
	startLogWorker.Do(func() {
		go logtasticWorker()
	})

	uri := fullURL(Server, uriPath)
	// logfLocal("logtasticPOST %s\n", uri)
	op := op{
		uri:  uri,
		mime: mime,
		d:    d,
	}

	select {
	case logWorkerCh <- op:
	default:
		logf("logtasticPOST %s failed: channel full or closed\n", uri)
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
		FileLogs, err = filerotate.NewDaily(LogDir, "log.txt", nil)
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
	if isShuttingDown.Load() {
		return
	}
	d := []byte(s)
	logtasticPOST("/api/v1/log", d, mimePlainText)
}

var userAgentBlacklist = []string{"OhDear.app", "(StatusCake)", "GoogleOther", "AhrefsBot/", "YandexBot/", "DotBot/", "Twitterbot/", "ClaudeBot/", "bingbot/", "Bytespider", "CensysInspect/", "Googlebot/", "coccocbot-web/", "PetalBot"}

// we don't want to log noise, just real user requests
func skipRemoteLog(r *http.Request) bool {
	ua := r.UserAgent()
	for _, s := range userAgentBlacklist {
		if strings.Contains(ua, s) {
			return true
		}
	}
	return false
}

func LogHit(r *http.Request, code int, size int64, dur time.Duration) {
	if isShuttingDown.Load() {
		return
	}
	m := map[string]interface{}{}
	httputil.GetRequestInfo(r, m, "")
	uri := m["url"].(string)
	if len(uri) > kMaxURLLen {
		m["url"] = uri[:kMaxURLLen]
	}
	m["dur_ms"] = float64(dur) / float64(time.Millisecond)
	m["status"] = code
	m["size"] = size
	if BuildHash != "" {
		m["build_hash"] = BuildHash
	}

	d, _ := json.Marshal(m)
	writeSiserLog("hit.txt", &FileHits, d)

	if skipRemoteLog(r) {
		return
	}
	logtasticPOST("/api/v1/hit", d, mimeJSON)
}

func LogEvent(r *http.Request, m map[string]interface{}) {
	if isShuttingDown.Load() {
		return
	}
	httputil.GetRequestInfo(r, m, "http")
	if BuildHash != "" {
		m["build_hash"] = BuildHash
	}

	d, _ := json.Marshal(m)
	writeSiserLog("event.txt", &FileEvents, d)

	logtasticPOST("/api/v1/event", d, mimeJSON)
}

func HandleEvent(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if isShuttingDown.Load() {
		http.Error(w, "Server shutting down", http.StatusMethodNotAllowed)
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
	if isShuttingDown.Load() {
		return
	}
	writeSiserLog("errors.txt", &FileErrors, []byte(s))

	m := map[string]interface{}{}
	httputil.GetRequestInfo(r, m, "http")
	m["error"] = s
	if BuildHash != "" {
		m["build_hash"] = BuildHash
	}
	m["callstack"] = u.GetCallstack(1)
	d, _ := json.Marshal(m)
	logtasticPOST("/api/v1/error", d, mimeJSON)
}

func limitString(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}

func CheckServerAlive(server string) error {
	if isShuttingDown.Load() {
		return errors.New("server shutting down")
	}
	if server == "" {
		return errors.New("server is empty string")
	}
	uri := fullURL(server, "/ping")
	_, err := http.Get(uri)
	return err
}
