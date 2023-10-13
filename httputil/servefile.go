package httputil

import (
	"bytes"
	"io/fs"
	"net/http"
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
	SupportCleanURLS bool
	ForceCleanURLS   bool
	ServeCompressed  bool
	compressedCached map[string][]byte
}

func TryServeFile(w http.ResponseWriter, r *http.Request, opts *ServeFileOptions) bool {
	urlPath := r.URL.Path
	return TryServeFileFromURL(w, r, urlPath, opts)
}

func TryServeFileFromURL(w http.ResponseWriter, r *http.Request, urlPath string, opts *ServeFileOptions) bool {
	fsys := opts.FS
	if opts.ForceCleanURLS {
		ext := filepath.Ext(urlPath)
		if strings.EqualFold(ext, ".html") {
			if u.FsFileExists(fsys, urlPath) {
				urlPath = urlPath[:len(urlPath)-len(ext)]
				SmartPermanentRedirect(w, r, urlPath)
				return true
			}
		}
	}
	if strings.HasSuffix(urlPath, "/") {
		urlPath += "index.html"
	}
	// for fs.FS paths cannot start with "/"
	urlPath = strings.TrimPrefix(urlPath, "/")

	// TODO: maybe also resolve /foo into /foo/index.html ?
	path := urlPath
	cleanURLS := opts.SupportCleanURLS || opts.ForceCleanURLS
	if !u.FsFileExists(fsys, path) && cleanURLS {
		path = path + ".html"
		if !u.FsFileExists(fsys, path) {
			return false
		}
	}

	// at this point path is a valid file in fs
	if serveFileMaybeBr(w, r, opts, path) {
		return true
	}
	d, err := fs.ReadFile(fsys, path)
	if err != nil {
		return false
	}

	ct := u.MimeTypeFromFileName(path)
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
	http.ServeContent(w, r, path, globalModTime, f)
	return true
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
			brData, err = u.BrCompressData(d)
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
