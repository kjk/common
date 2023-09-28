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
	fs := opts.FS
	if opts.ForceCleanURLS {
		ext := filepath.Ext(urlPath)
		if strings.EqualFold(ext, ".html") {
			if u.FsFileExists(fs, urlPath) {
				urlPath = urlPath[:len(urlPath)-len(ext)]
				SmartPermanentRedirect(w, r, urlPath)
				return true
			}
		}
	}
	if strings.HasSuffix(urlPath, "/") {
		urlPath += "index.html"
	}
	// TODO: maybe also resolve /foo into /foo/index.html ?
	path := urlPath
	cleanURLS := opts.SupportCleanURLS || opts.ForceCleanURLS
	if !u.FsFileExists(fs, path) && cleanURLS {
		path = path + ".html"
		if !u.FsFileExists(fs, path) {
			return false
		}
	}

	// at this point path is a valid file in fs
	if r != nil && opts.ServeCompressed {
		if serveFileMaybeBr(w, r, opts, path) {
			return true
		}
		// TODO: maybe add serveFileMaybeGz
		// but then again modern browsers probably support br so that would be redundant
	}
	d, err := u.FsReadFile(fs, path)
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

func serveFileMaybeBr(w http.ResponseWriter, r *http.Request, opts *ServeFileOptions, path string) bool {
	ext := strings.ToLower(filepath.Ext(path))
	switch ext {
	case ".html", ".txt", ".css", ".js", ".xml", ".svg":
		// those we serve compressed
	default:
		// other formats, e.g. png, should not be served compressed
		// fmt.Printf("serveFileMaybeBr: skipping because '%s' not served as br because '%s' should not be served compressed\n", path, ext)
		return false
	}

	if r == nil {
		return false
	}
	enc := r.Header.Get("Accept-Encoding")
	// fmt.Printf("serveFileMaybeBr: enc: '%s'\n", enc)
	if !strings.Contains(enc, "br") {
		// fmt.Printf("serveFileMaybeBr: doesn't accept 'br'\n")
		return false
	}
	fs := opts.FS
	serveFileMu.Lock()
	if opts.compressedCached == nil {
		opts.compressedCached = make(map[string][]byte)
	}
	brData := opts.compressedCached[path]
	serveFileMu.Unlock()

	if len(brData) == 0 {
		d, err := u.FsReadFile(fs, path)
		if err != nil {
			return false
		}
		brData, err = u.BrCompressData(d)
		if err != nil {
			return false
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
