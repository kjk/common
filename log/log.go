package log

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/kjk/common/siser"

	"github.com/toon-format/toon-go"
)

var (
	log       *WriteDaily
	httpLog   *WriteDaily
	errorsLog *WriteDaily
	eventsLog *WriteDaily

	// if true, Verbosef() will log messages
	Verbose bool
)

type WriteDaily struct {
	Dir         string
	currentDate int // YYYYMMDD format
	file        *os.File
	mu          sync.Mutex
}

func NewWriteDaily(dir string) *WriteDaily {
	return &WriteDaily{
		Dir: dir,
	}
}

// WriteString writes a string to the daily log file
// it's safe to call on nil receiver
func (w *WriteDaily) WriteString(s string) error {
	return w.Write([]byte(s))
}

// dayFromTime converts a time.Time to YYYYMMDD inte	ger format
func dayFromTime(t time.Time) int {
	return t.Year()*10000 + int(t.Month())*100 + t.Day()
}

// Writer returns an io.Writer for today's log file
// it creates a new file if needed
// it's safe to call on nil receiver
func (w *WriteDaily) Writer() (io.Writer, error) {
	if w == nil {
		return nil, fmt.Errorf("w is nil")
	}
	w.mu.Lock()
	defer w.mu.Unlock()

	now := time.Now().UTC()
	today := dayFromTime(now)

	if w.file != nil && w.currentDate != today {
		if err := w.close(); err != nil {
			return nil, err
		}
	}

	if w.file == nil {
		dateStr := now.Format("2006-01-02")
		filename := filepath.Join(w.Dir, dateStr+".txt")
		if err := os.MkdirAll(w.Dir, 0755); err != nil {
			return nil, err
		}
		f, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return nil, err
		}
		w.file = f
		w.currentDate = today
	}
	return w.file, nil
}

// Write writes data to the daily log file
// it's safe to call on nil receiver
func (w *WriteDaily) Write(d []byte) error {
	if w == nil {
		return nil
	}
	if wr, err := w.Writer(); err != nil {
		return err
	} else {
		_, err := wr.Write(d)
		return err
	}
}

func (w *WriteDaily) close() error {
	if w.file == nil {
		return nil
	}

	err := w.file.Close()
	w.file = nil
	w.currentDate = 0
	return err
}

// Close closes the daily log file
// it's safe to call on nil receiver
func (w *WriteDaily) Close() error {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.close()
}

// Sync flushes the daily log file to disk
// it's safe to call on nil receiver
func (w *WriteDaily) Sync() error {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file != nil {
		return w.file.Sync()
	}
	return nil
}

type Config struct {
	// directory where log files are stored
	// each log type (regular, error, event, http) has its own subdirectory
	Dir string
	// called for every Logf() call
	// allows sending logs to other places (e.g. logtail)
	OnLog func(s string)
}

// Init initializes the logging system
// log files are stored in config.Dir
func Init(config *Config) {
	dir := config.Dir
	log = NewWriteDaily(filepath.Join(dir, "log"))
	errorsLog = NewWriteDaily(filepath.Join(dir, "errors"))
	// this doesn't create log files so if app doesn't
	// log http requests of events, it's a no-op
	httpLog = NewWriteDaily(filepath.Join(dir, "http"))
	eventsLog = NewWriteDaily(filepath.Join(dir, "events"))
}

// CloseWriteDaily closes the WriteDaily and sets its pointer to nil
// it's safe to call with nil pointer
func CloseWriteDaily(wd **WriteDaily) {
	if *wd == nil {
		return
	}
	(*wd).Sync()
	(*wd).Close()
	*wd = nil
}

// Close
func Close() {
	CloseWriteDaily(&log)
	CloseWriteDaily(&httpLog)
	CloseWriteDaily(&errorsLog)
	CloseWriteDaily(&eventsLog)
}

func Logf(s string, args ...any) {
	if len(args) > 0 {
		s = fmt.Sprintf(s, args...)
	}
	fmt.Print(s)
	log.WriteString(s)
	// logToChannels(s)
}

func GetCallstackFrames(skip int) []string {
	var callers [32]uintptr
	n := runtime.Callers(skip+1, callers[:])
	frames := runtime.CallersFrames(callers[:n])
	var cs []string
	for {
		frame, more := frames.Next()
		if !more {
			break
		}
		s := frame.File + ":" + strconv.Itoa(frame.Line)
		cs = append(cs, s)
	}
	return cs
}

func GetCallstack(skip int) string {
	frames := GetCallstackFrames(skip + 1)
	return strings.Join(frames, "\n")
}

func Verbosef(format string, args ...any) {
	if !Verbose {
		return
	}
	Logf(format, args...)
}

// Errorf logs an error message along with the callstack
func Errorf(s string, args ...any) {
	if len(args) > 0 {
		s = fmt.Sprintf(s, args...)
	}
	cs := GetCallstack(1)
	Logf("%s\n%s\n", s, cs)
}

// if err != nil, log and return true
// IfErrf(err) => logs err.Error()
// IfErrf(err, "error is: %v", err) => logs message formatted
func IfErrf(err error, a ...any) bool {
	if err == nil {
		return false
	}
	if len(a) == 0 {
		Errorf(err.Error())
		return true
	}
	s, ok := a[0].(string)
	if !ok {
		// shouldn't happen but just in case
		s = fmt.Sprintf("%s", a[0])
	}
	if len(a) > 1 {
		s = fmt.Sprintf(s, a[1:]...)
	}
	Errorf(s)
	return true
}

func panicIf(cond bool) {
	if cond {
		panic("condition is true")
	}
}

func pickFirst(s string) string {
	parts := strings.Split(s, ",")
	return strings.TrimSpace(parts[0])
}

// BestRemoteAddress picks the most accurate IP address from client request
// needed because of proxies
func BestRemoteAddress(r *http.Request) string {
	h := r.Header
	val := h.Get("CF-Connecting-IP")
	if len(val) > 0 {
		return pickFirst(val)
	}
	val = h.Get("X-Real-Ip")
	if len(val) > 0 {
		return pickFirst(val)
	}
	val = h.Get("X-Forwarded-For")
	if len(val) > 0 {
		return pickFirst(val)
	}
	val = r.RemoteAddr
	if len(val) > 0 {
		return pickFirst(val)
	}
	return ""
}

func appendRequestValues(r *http.Request, vals []any) []any {
	if r == nil {
		return vals
	}
	ip := BestRemoteAddress(r)
	vals = append(vals, "ip", ip)
	userID := r.Header.Get("X-User")
	if userID != "" {
		vals = append(vals, "user", userID)
	}
	return vals
}

// simpleTypeToStr converts simple types to string
// panics if v is of complex type
func simpleTypeToStr(v any) string {
	rt := reflect.TypeOf(v)
	kind := rt.Kind()
	switch kind {
	// TODO: what to do about pointers?
	// TODO: maybe whitelist better than blacklist?
	case reflect.Array, reflect.Slice, reflect.Struct, reflect.Map, reflect.Chan, reflect.Interface, reflect.Pointer:
		panic(fmt.Sprintf("toStr: value is of kind %v", kind))
	case reflect.String:
		return v.(string)
	}
	return fmt.Sprintf("%s", v)
}

// Event logs event in toon format with
func Event(name string, vals ...any) {
	n := len(vals)
	panicIf(n%2 != 0)
	var d []byte
	if n > 0 {
		m := map[string]any{}
		for i := 0; i < n; i += 2 {
			k := simpleTypeToStr(vals[i])
			m[k] = vals[i+1]
		}
		d, _ = toon.Marshal(m)
	}
	t := time.Now().UTC()
	d2 := siser.MarshalLine(name, t, d, nil)
	eventsLog.Write(d2)
}

// GET /api/le?name=<name>&key1=val1&key2=val2...
func HandleEvent(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	var vals []any
	name := ""
	for k, va := range r.Form {
		if len(va) == 0 {
			continue
		}
		if k == "name" {
			name = va[0]
			continue
		}
		vals = append(vals, k, va[0])
	}
	// should always have a name
	if name == "" {
		v := map[string]any{
			"Error": "<name> is required",
		}
		serveJSONStatus(w, v, http.StatusBadRequest)
		return
	}
	Logf("HandleEvent: '%s', vals: %#v\n", name, vals)
	EventFromRequest(r, name, vals)
	v := map[string]any{
		"Message": "ok",
	}
	serveJSONStatus(w, v, http.StatusOK)
}

func EventMap(m map[string]any) error {
	name := "_js"
	if v, ok := m["name"]; ok {
		if s, ok := v.(string); ok {
			name = s
		}
	}
	dt, err := toon.Marshal(m)
	if err != nil {
		return err
	}
	t := time.Now().UTC()
	d2 := siser.MarshalLine(name, t, dt, nil)
	eventsLog.Write(d2)
	return nil
}

// EventJSON logs event we presume is JSON encoded
// it converts it to toon and saves as siser record
// if name is present, it uses it, otherwise uses "_js"
func EventJSON(d []byte) error {
	var m map[string]any
	if err := json.Unmarshal(d, &m); err != nil {
		return err
	}
	return EventMap(m)
}

// POST /api/lejson
func HandleEventJSON(w http.ResponseWriter, r *http.Request) {
	d, err := io.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	EventJSON(d)
}

func EventWithDuration(name string, dur time.Duration, vals ...any) {
	vals = append(vals, "durmicro", dur.Microseconds())
	Event(name, vals...)
}

func EventFromRequest(r *http.Request, name string, vals ...any) {
	vals = appendRequestValues(r, vals)
	Event(name, vals...)
}

func ErrorEventFromRequest(r *http.Request, err error, name string, vals ...any) {
	vals = appendRequestValues(r, vals)
	vals = append(vals, "error", err.Error())
	Event(name, vals...)
}

func serveJSONStatus(w http.ResponseWriter, v any, statusCode int) {
	d, _ := json.Marshal(v)
	w.Header().Set("Content-Type", "text/javascript; charset=utf-8")
	w.WriteHeader(statusCode)
	w.Write(d)
}

func HTTPRequestToWriteDaily(w *WriteDaily, r *http.Request, code int, nWritten int64, dur time.Duration) error {
	ip := BestRemoteAddress(r)

	rawQuery := r.URL.RawQuery
	if len(rawQuery) > 128 {
		rawQuery = rawQuery[:128]
	}

	entry := map[string]any{
		"ts":     time.Now().UTC().Unix(),
		"method": r.Method,
		"url":    r.URL.Path,
		"query":  rawQuery,
		"host":   r.Host,
		"ip":     ip,
		"code":   code,
		"size":   nWritten,
		"dur":    float64(dur.Microseconds()) / 1000.0, // milliseconds with decimal precision
	}

	if referer := r.Header.Get("Referer"); referer != "" {
		entry["referer"] = referer
	}
	if ua := r.Header.Get("User-Agent"); ua != "" {
		entry["ua"] = ua
	}
	if contentType := r.Header.Get("Content-Type"); contentType != "" {
		entry["content_type"] = contentType
	}

	buf := &strings.Builder{}
	encoder := json.NewEncoder(buf)
	// avoid unnecessary escaping
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(entry); err != nil {
		return err
	}

	// write to log (Encode adds a newline)
	return w.Write([]byte(buf.String()))
}

func HTTPRequest(r *http.Request, code int, nWritten int64, dur time.Duration) error {
	return HTTPRequestToWriteDaily(httpLog, r, code, nWritten, dur)
}
