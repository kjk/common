package httputil

import (
	"bytes"
	"io/fs"
	"net/http"
	"path"
	"path/filepath"
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

func TryServeURLFromFS(w http.ResponseWriter, r *http.Request, opts *ServeFileOptions) bool {
	dirPrefix := opts.DirPrefix
	u.PanicIf(strings.HasPrefix(dirPrefix, "/"), "dirPrefix should not start with /")
	uri := r.URL.Path
	fsPath := path.Join(dirPrefix, uri)
	fsys := opts.FS
	pathExists := u.FsFileExists(fsys, fsPath)
	if pathExists && opts.ForceCleanURLS {
		ext := filepath.Ext(uri)
		// redirect /foo.html => /foo
		if strings.EqualFold(ext, ".html") {
			uri = uri[:len(uri)-len(ext)]
			SmartPermanentRedirect(w, r, uri)
			return true
		}
		// redirect /foo/index.html => /foo/
		if strings.HasSuffix(uri, "/index.html") {
			uri = strings.TrimSuffix(uri, "index.html")
			SmartPermanentRedirect(w, r, uri)
			return true
		}
	}
	// try `/foo/` as `/foo/index.html
	if strings.HasSuffix(uri, "/") {
		fsPath = path.Join(dirPrefix, uri+"index.html")
		pathExists = u.FsFileExists(fsys, fsPath)
	}

	cleanURLS := opts.SupportCleanURLS || opts.ForceCleanURLS
	if !pathExists && cleanURLS {
		// try '/foo' as '/foo.html'
		fsPath = path.Join(dirPrefix, uri+".html")
		if !u.FsFileExists(fsys, fsPath) {
			return false
		}
	}

	// at this point fsPath exists in fsys
	isLongLived := false
	for _, prefix := range opts.LongLivedURLPrefixes {
		if strings.HasPrefix(uri, prefix) {
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

func canServeBr(r *http.Request) bool {
	enc := r.Header.Get("Accept-Encoding")
	return strings.Contains(enc, "br")
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
		}
	}

	if len(brData) == 0 {
		return false
	}

	serveFileMu.Lock()
	opts.compressedCached[path] = brData
	serveFileMu.Unlock()

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
