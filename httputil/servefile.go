package httputil

import (
	"bytes"
	"io/fs"
	"net/http"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/kjk/common/u"
)

var (
	serveFileMu   sync.Mutex
	globalModTime time.Time = time.Now()
)

type ServeFileOptions struct {
	FS               fs.FS
	DirPrefix        string // e.g. dist/
	SupportCleanURLS bool
	ForceCleanURLS   bool
	ServeCompressed  bool
	// list of url prefixes that should be served as long-lived (e.g. /static/, /assets/)
	LongLivedURLPrefixes []string
	compressedCached     map[string][]byte
}

func serveFileFromFS(w http.ResponseWriter, r *http.Request, opts *ServeFileOptions, fsPath string) bool {
	if !u.FsFileExists(opts.FS, fsPath) {
		return false
	}
	// at this point fsPath is a valid file in fs
	if serveFileMaybeBr(w, r, opts, fsPath) {
		return true
	}
	d, err := fs.ReadFile(opts.FS, fsPath)
	if err != nil {
		return false
	}

	ct := u.MimeTypeFromFileName(fsPath)
	if ct != "" {
		w.Header().Set("Content-Type", ct)
	}
	if r == nil {
		// we can be used by server.makeServeFile which doesn't provide http.Request
		// in that case w is a file
		_, err = w.Write(d)
		return err == nil
	}
	f := bytes.NewReader(d)
	http.ServeContent(w, r, fsPath, globalModTime, f)
	return true
}

func TryServeFileFromFS(w http.ResponseWriter, r *http.Request, opts *ServeFileOptions, fsPath string) bool {
	return serveFileFromFS(w, r, opts, fsPath)
}

// fsPathForURL returns path in fs for url path, under dirPrefix.
// urlPath is cleaned first so that it can't escape dirPrefix with "..".
func fsPathForURL(dirPrefix string, urlPath string) string {
	p := strings.TrimPrefix(path.Clean("/"+urlPath), "/")
	return path.Join(dirPrefix, p)
}

// TryServeURLFromFS serves r.URL.Path from opts.FS (under opts.DirPrefix).
// Returns false if there is no file for this url.
// "/foo/" is served from "foo/index.html" (and "/" from "index.html").
// With SupportCleanURLS "/foo" is served from "foo.html".
// ForceCleanURLS also redirects "/foo.html" => "/foo" and "/foo/index.html" => "/foo/".
func TryServeURLFromFS(w http.ResponseWriter, r *http.Request, opts *ServeFileOptions) bool {
	dirPrefix := opts.DirPrefix
	u.PanicIf(strings.HasPrefix(dirPrefix, "/"), "dirPrefix should not start with /")
	fsys := opts.FS
	uri := r.URL.Path
	isDirURL := uri == "" || strings.HasSuffix(uri, "/")
	cleanURI := path.Clean("/" + uri) // starts with "/", no trailing "/"

	if opts.ForceCleanURLS && !isDirURL && u.FsFileExists(fsys, fsPathForURL(dirPrefix, cleanURI)) {
		// must check before generic .html redirect
		// redirect /foo/index.html => /foo/
		if strings.HasSuffix(cleanURI, "/index.html") {
			SmartPermanentRedirect(w, r, strings.TrimSuffix(cleanURI, "index.html"))
			return true
		}
		// redirect /foo.html => /foo
		ext := path.Ext(cleanURI)
		if strings.EqualFold(ext, ".html") {
			SmartPermanentRedirect(w, r, cleanURI[:len(cleanURI)-len(ext)])
			return true
		}
	}

	var fsPath string
	if isDirURL {
		// try `/foo/` as `/foo/index.html`
		fsPath = fsPathForURL(dirPrefix, path.Join(cleanURI, "index.html"))
	} else {
		fsPath = fsPathForURL(dirPrefix, cleanURI)
	}
	pathExists := u.FsFileExists(fsys, fsPath)

	cleanURLS := opts.SupportCleanURLS || opts.ForceCleanURLS
	if !pathExists && cleanURLS && !isDirURL {
		// try '/foo' as '/foo.html'
		fsPath = fsPathForURL(dirPrefix, cleanURI+".html")
		pathExists = u.FsFileExists(fsys, fsPath)
	}
	if !pathExists {
		return false
	}

	// at this point fsPath exists in fsys
	isLongLived := false
	for _, prefix := range opts.LongLivedURLPrefixes {
		if strings.HasPrefix(cleanURI, prefix) {
			isLongLived = true
			break
		}
	}
	if isLongLived {
		// 31536000 seconds is a year
		w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
	}

	return serveFileFromFS(w, r, opts, fsPath)
}

// canServeBr returns true if client accepts brotli encoding
// i.e. Accept-Encoding contains "br" without "q=0"
func canServeBr(r *http.Request) bool {
	enc := r.Header.Get("Accept-Encoding")
	for part := range strings.SplitSeq(enc, ",") {
		name, params, _ := strings.Cut(strings.TrimSpace(part), ";")
		if !strings.EqualFold(strings.TrimSpace(name), "br") {
			continue
		}
		for param := range strings.SplitSeq(params, ";") {
			k, v, _ := strings.Cut(strings.TrimSpace(param), "=")
			if strings.TrimSpace(k) != "q" {
				continue
			}
			q, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
			if err == nil && q == 0 {
				return false
			}
		}
		return true
	}
	return false
}

// if we have a *.br version in opts.FS, serve it
// otherwise compress on demand if opts.ServeCompressed is true
func serveFileMaybeBr(w http.ResponseWriter, r *http.Request, opts *ServeFileOptions, path string) bool {
	if r == nil || !canServeBr(r) {
		return false
	}
	fsys := opts.FS
	brData, err := fs.ReadFile(fsys, path+".br")
	if err != nil {
		// compress on demand
		if !opts.ServeCompressed {
			return false
		}
		ext := strings.ToLower(filepath.Ext(path))
		switch ext {
		case ".html", ".txt", ".css", ".js", ".xml", ".svg":
			// those we serve compressed
		default:
			// other formats, e.g. png, should not be served compressed
			// fmt.Printf("serveFileMaybeBr: skipping because '%s' not served as br because '%s' should not be served compressed\n", path, ext)
			return false
		}
		serveFileMu.Lock()
		if opts.compressedCached == nil {
			opts.compressedCached = make(map[string][]byte)
		}
		brData = opts.compressedCached[path]
		serveFileMu.Unlock()
		if len(brData) == 0 {
			d, err := fs.ReadFile(fsys, path)
			if err != nil {
				return false
			}
			brData, err = u.BrCompressDataBest(d)
			if err != nil {
				return false
			}
			// only cache data we compressed on demand; opts.compressedCached
			// is only allocated in this branch
			serveFileMu.Lock()
			opts.compressedCached[path] = brData
			serveFileMu.Unlock()
		}
	}

	if len(brData) == 0 {
		return false
	}

	ct := u.MimeTypeFromFileName(path)
	if ct != "" {
		w.Header().Set("Content-Type", ct)
	}
	// https://www.maxcdn.com/blog/accept-encoding-its-vary-important/
	// prevent caching non-compressed version
	w.Header().Add("Vary", "Accept-Encoding")
	w.Header().Set("Content-Encoding", "br")
	f := bytes.NewReader(brData)
	http.ServeContent(w, r, path, globalModTime, f)
	// fmt.Printf("serveFileMaybeBr: served '%s'\n", pathBr)
	return true
}
