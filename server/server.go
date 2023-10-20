package server

import (
	"archive/zip"
	"bytes"
	"compress/flate"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/kjk/common/httputil"
	"github.com/kjk/common/u"
)

var (
	must = u.Must
)

// Server represents all files known to the server
type Server struct {
	Port     int
	Handlers []Handler
	// if true supports clean urls i.e. /foo will match /foo.html URL
	CleanURLS bool
	// if true forces clean urls i.e. /foo.html will redirect to /foo
	ForceCleanURLS bool
}

type HandlerFunc = func(w http.ResponseWriter, r *http.Request)
type GetHandlerFunc = func(string) func(w http.ResponseWriter, r *http.Request)

// Handler represents one or more urls and their content
type Handler interface {
	// returns a handler for this url
	// if nil, doesn't handle this url
	Get(url string) HandlerFunc
	// get all urls handled by this Handler
	// useful for e.g. saving a static copy to disk
	URLS() []string
}

func panicIfAbsoluteURL(uri string) {
	u.PanicIf(strings.Contains(uri, "://"), "got absolute url '%s'", uri)
}

// FileWriter implements http.ResponseWriter interface for writing to a io.Writer
type FileWriter struct {
	w      io.Writer
	header http.Header
}

func (w *FileWriter) Header() http.Header {
	if w.header == nil {
		w.header = http.Header{}
	}
	return w.header
}

func (w *FileWriter) Write(p []byte) (int, error) {
	return w.w.Write(p)
}

func (w *FileWriter) WriteHeader(statusCode int) {
	// no-op
}

// d can be nil, in which case no caching
func serve404FileCached(w http.ResponseWriter, r *http.Request, path string, cached *[]byte) {
	var d []byte
	if cached != nil && len(*cached) > 0 {
		d = *cached
	} else {
		var err error
		d, err = os.ReadFile(path)
		must(err)
		if cached != nil {
			*cached = d
		}
	}

	ctype := u.MimeTypeFromFileName(path)
	if ctype != "" {
		w.Header().Set("Content-Type", ctype)
	}

	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusNotFound)
	w.Write(d)
}

func makeServeFile(path string, tryServeCompressed bool) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(path, "404.html") {
			serve404FileCached(w, r, path, nil)
			return
		}
		dir := filepath.Dir(path)
		fsys := os.DirFS(dir)
		name := filepath.Base(path)
		opts := httputil.ServeFileOptions{
			FS:              fsys,
			ServeCompressed: tryServeCompressed,
		}
		if !httputil.TryServeFileFromURL(w, r, name, &opts) {
			http.ServeFile(w, r, path)
		}
	}
}

// uri is only used to guess content type
func serveContent(w http.ResponseWriter, r *http.Request, uri string, d []byte, code int, modTime time.Time) {
	if r == nil {
		_, err := w.Write(d)
		must(err)
		return
	}
	content := bytes.NewReader(d)
	if code != http.StatusOK {
		w.WriteHeader(code)
		return
	}
	http.ServeContent(w, r, uri, modTime, content)
}

func MakeServeContent(uri string, d []byte, code int, modTime time.Time) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		serveContent(w, r, uri, d, code, modTime)
	}
}

type FilesHandler struct {
	files              map[string]string // maps url to a path on disk
	TryServeCompressed bool
}

func (h *FilesHandler) AddFile(uri, path string) {
	panicIfAbsoluteURL(uri)
	u.PanicIf(!u.FileExists(path), "file '%s' doesn't exist", path)
	h.files[uri] = path
}

func (h *FilesHandler) AddFilesInDir(dir string, uriPrefix string, files []string) {
	for _, f := range files {
		uri := uriPrefix + f
		path := filepath.Join(dir, f)
		h.AddFile(uri, path)
	}
}

func (h *FilesHandler) Get(url string) func(w http.ResponseWriter, r *http.Request) {
	for uri, path := range h.files {
		// we consider URLs case-insensitive
		if strings.EqualFold(uri, url) {
			return makeServeFile(path, h.TryServeCompressed)
		}
	}
	return nil
}

func (h *FilesHandler) URLS() []string {
	urls := []string{}
	for uri := range h.files {
		urls = append(urls, uri)
	}
	return urls
}

// files is: uri1, path1, uri2, path2, ...
func NewFilesHandler(files ...string) *FilesHandler {
	u.PanicIf(len(files)%2 == 1)
	n := len(files)
	h := &FilesHandler{
		files: map[string]string{},
	}
	for i := 0; i < n; i += 2 {
		h.AddFile(files[i], files[i+1])
	}
	return h
}

type DirHandler struct {
	Dir                string
	URLPrefix          string
	TryServeCompressed bool

	URL   []string
	paths []string // same order as URL
}

func (h *DirHandler) Get(url string) func(w http.ResponseWriter, r *http.Request) {
	for i, u := range h.URL {
		// urls are case-insensitive
		if strings.EqualFold(u, url) {
			return makeServeFile(h.paths[i], h.TryServeCompressed)
		}
	}
	return nil
}

func (h *DirHandler) URLS() []string {
	return h.URL
}

func getURLSForFiles(startDir string, urlPrefix string, acceptFile func(string) bool) (urls []string, paths []string) {
	filepath.WalkDir(startDir, func(filePath string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if !d.Type().IsRegular() {
			return nil
		}
		if acceptFile != nil && !acceptFile(filePath) {
			return nil
		}
		dir := strings.TrimPrefix(filePath, startDir)
		dir = filepath.ToSlash(dir)
		dir = strings.TrimPrefix(dir, "/")
		uri := path.Join(urlPrefix, dir)
		//logf(ctx(), "getURLSForFiles: dir: '%s'\n", dir)
		urls = append(urls, uri)
		paths = append(paths, filePath)
		return nil
	})
	return
}

func NewDirHandler(dir string, urlPrefix string, acceptFile func(string) bool) *DirHandler {
	urls, paths := getURLSForFiles(dir, urlPrefix, acceptFile)
	return &DirHandler{
		Dir:       dir,
		URLPrefix: urlPrefix,
		URL:       urls,
		paths:     paths,
	}
}

type DynamicHandler struct {
	get  GetHandlerFunc
	urls func() []string
}

func (h *DynamicHandler) Get(uri string) func(http.ResponseWriter, *http.Request) {
	return h.get(uri)
}

func (h *DynamicHandler) URLS() []string {
	return h.urls()
}

func NewDynamicHandler(get GetHandlerFunc, urls func() []string) *DynamicHandler {
	return &DynamicHandler{
		get:  get,
		urls: urls,
	}
}

type InMemoryFilesHandler struct {
	files   map[string][]byte
	modTime time.Time
}

func (h *InMemoryFilesHandler) Get(uri string) func(http.ResponseWriter, *http.Request) {
	for path, d := range h.files {
		if strings.EqualFold(path, uri) {
			code := http.StatusOK
			if strings.HasSuffix(uri, "/404.html") {
				code = http.StatusNotFound
			}
			return MakeServeContent(uri, d, code, h.modTime)
		}
	}
	return nil
}

func (h *InMemoryFilesHandler) URLS() []string {
	var urls []string
	for path := range h.files {
		urls = append(urls, path)
	}
	return urls
}

func (h *InMemoryFilesHandler) Add(uri string, body []byte) {
	panicIfAbsoluteURL(uri)
	// in case uri is a windows path, convert to unix path
	uri = strings.Replace(uri, "\\", "/", -1)
	u.PanicIf(!strings.HasPrefix(uri, "/"))
	h.files[uri] = body
}

func NewInMemoryFilesHandler(uri string, d []byte) *InMemoryFilesHandler {
	h := &InMemoryFilesHandler{
		files:   map[string][]byte{},
		modTime: time.Now(),
	}
	h.Add(uri, d)
	return h
}

// IterContent calls a function for every url and (optionally) its content
func IterURLS(handlers []Handler, withContent bool, fn func(uri string, d []byte)) {
	var buf bytes.Buffer
	for _, h := range handlers {
		urls := h.URLS()
		for _, uri := range urls {
			if !withContent {
				fn(uri, nil)
				continue
			}
			buf.Reset()
			fw := &FileWriter{
				w: &buf,
			}
			serve := h.Get(uri)
			u.PanicIf(serve == nil, "must have a handler for '%s'", uri)
			serve(fw, nil)
			fn(uri, buf.Bytes())
		}
	}
}

// IterContent calls a function for every url and its content
func IterContent(handlers []Handler, fn func(uri string, d []byte)) {
	IterURLS(handlers, true, fn)
}

func (s *Server) FindHandlerExact(uri string) HandlerFunc {
	for _, h := range s.Handlers {
		if send := h.Get(uri); send != nil {
			return send
		}
	}
	return nil
}

func commonExt(uri string) bool {
	ext := strings.ToLower(filepath.Ext(uri))
	switch ext {
	case ".html", ".js", ".css", ".txt", ".xml":
		return true
	}
	return false
}

const (
	html404 = "/404.html"
)

func Gen404Candidates(uri string) []string {
	idx := strings.LastIndex(uri, "/")
	if idx == -1 || idx == 0 {
		return []string{html404}
	}

	var res []string
	rest := uri
	for idx >= 0 {
		last := rest[idx:]
		if last != "/" && !commonExt(last) {
			res = append(res, path.Join(rest, html404))
		}
		rest = rest[:idx]
		idx = strings.LastIndex(rest, "/")
	}
	res = append(res, html404)
	return res
}

func makePermRedirect(uri string) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		httputil.SmartPermanentRedirect(w, r, uri)
	}
}

func (s *Server) FindHandler(uri string) (h HandlerFunc, is404 bool) {
	if strings.HasSuffix(uri, "/") {
		uri = path.Join(uri, "/index.html")
	}
	if h = s.FindHandlerExact(uri); h != nil {
		if s.ForceCleanURLS && u.ExtEqualFold(uri, ".html") {
			uri = u.TrimExt(uri)
			h = makePermRedirect(uri)
		}
		return h, false
	}

	// if we support clean urls, try find "/foo.html" for "/foo"
	if (s.CleanURLS || s.ForceCleanURLS) && !commonExt(uri) {
		if h = s.FindHandlerExact(uri + ".html"); h != nil {
			return h, false
		}
	}
	// try 404.html
	a := Gen404Candidates(uri)
	for _, uri404 := range a {
		if h = s.FindHandlerExact(uri404); h != nil {
			return h, true
		}
	}
	return nil, false
}

// don't really use it
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	uri := r.URL.Path
	serve, _ := s.FindHandler(uri)
	if serve != nil {
		serve(w, r)
		return
	}
	http.NotFound(w, r)
}

func WriteServerFilesToDir(dir string, handlers []Handler, onWritten func(path string, d []byte)) error {
	dirCreated := map[string]bool{}

	var err error
	writeFile := func(uri string, d []byte) {
		if err != nil {
			return
		}
		name := strings.TrimPrefix(uri, "/")
		name = filepath.FromSlash(name)
		path := filepath.Join(dir, name)
		// optimize for writing lots of files
		// I assume that even a no-op os.MkdirAll()
		// might be somewhat expensive
		fileDir := filepath.Dir(path)
		if !dirCreated[fileDir] {
			err = os.MkdirAll(fileDir, 0755)
			if err != nil {
				return
			}
			dirCreated[fileDir] = true
		}
		err = os.WriteFile(path, d, 0644)
	}
	IterContent(handlers, writeFile)
	return err
}

func WriteServerFilesToZip(handlers []Handler, onWritten func(path string, d []byte)) ([]byte, error) {
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	zw.RegisterCompressor(zip.Deflate, func(out io.Writer) (io.WriteCloser, error) {
		return flate.NewWriter(out, flate.BestCompression)
	})

	zipWriteFile := func(zw *zip.Writer, name string, data []byte) error {
		fw, err := zw.Create(name)
		if err != nil {
			return err
		}
		_, err = fw.Write(data)
		return err
	}

	var err error
	writeFile := func(uri string, d []byte) {
		if err != nil {
			return
		}
		name := strings.TrimPrefix(uri, "/")
		err = zipWriteFile(zw, name, d)
		if err != nil {
			return
		}
	}
	IterContent(handlers, writeFile)
	return buf.Bytes(), err
}
