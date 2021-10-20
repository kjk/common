package httputil

import (
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/andybalholm/brotli"
	"github.com/kjk/common/u"
)

var (
	serveFileMu sync.Mutex
)

type FileServeOpts struct {
	Dir              string
	SupportCleanURLS bool
	ForceCleanURLS   bool
	ServeCompressed  bool
}

func TryServeFile(w http.ResponseWriter, r *http.Request, opts *FileServeOpts) bool {
	uriPath := r.URL.Path

	if opts.ForceCleanURLS {
		ext := filepath.Ext(uriPath)
		if strings.EqualFold(ext, ".html") {
			path := filepath.Join(opts.Dir, uriPath)
			if u.FileExists(path) {
				uriPath = uriPath[:len(uriPath)-len(ext)]
				SmartPermanentRedirect(w, r, uriPath)
				return true
			}
		}
	}
	if uriPath == "/" {
		uriPath = "index.html"
	}
	path := filepath.Join(opts.Dir, uriPath)
	cleanURLS := opts.SupportCleanURLS || opts.ForceCleanURLS
	if !u.FileExists(path) && cleanURLS {
		path = path + ".html"
		if !u.FileExists(path) {
			return false
		}
	}

	if opts.ServeCompressed && canServeCompressed(path) {
		if serveFileMaybeBr(w, r, path) {
			return true
		}
		// TODO: maybe add serveFileMaybeGz
		// but then again modern browsers probably support br
	}
	ct := u.MimeTypeFromFileName(path)
	if ct != "" {
		w.Header().Set("Content-Type", ct)
	}
	http.ServeFile(w, r, path)
	return serveFileMaybeBr(w, r, path)
}

func canServeCompressed(path string) bool {
	ext := strings.ToLower(filepath.Ext(path))
	switch ext {
	case ".html", ".txt", ".css", ".js", ".xml":
		return true
	}
	return false
}

func serveFileMaybeBr(w http.ResponseWriter, r *http.Request, path string) bool {
	if r == nil {
		return false
	}
	enc := r.Header.Get("Accept-Encoding")
	// fmt.Printf("serveFileMaybeBr: enc: '%s'\n", enc)
	if !strings.Contains(enc, "br") {
		// fmt.Printf("serveFileMaybeBr: doesn't accept 'br'\n")
		return false
	}
	pathBr := path + ".br"
	// fmt.Printf("serveFileMaybeBr: '%s', '%s'\n", path, pathBr)
	if !u.FileExists(pathBr) {
		if !u.FileExists(path) {
			// fmt.Printf("serveFileMaybeBr: '%s' not found\n", path)
			http.NotFound(w, r)
			return true
		}
		serveFileMu.Lock()
		err := compressBr(path, pathBr)
		// fmt.Printf("serveFileMaybeBr: compressBr('%s', '%s'), err: %v\n", path, pathBr, err)
		serveFileMu.Unlock()
		if err != nil {
			return false
		}
	}
	f, err := os.Open(pathBr)
	if err != nil {
		// fmt.Printf("serveFileMaybeBr: os.Open('%s') failed with err: %v\n", pathBr, err)
		return false
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		// fmt.Printf("serveFileMaybeBr: f.Stat() '%s' failed with err: %v\n", pathBr, err)
		return false
	}
	ct := u.MimeTypeFromFileName(path)
	if ct != "" {
		w.Header().Set("Content-Type", ct)
	}
	// https://www.maxcdn.com/blog/accept-encoding-its-vary-important/
	// prevent caching non-gzipped version
	w.Header().Add("Vary", "Accept-Encoding")
	w.Header().Set("Content-Encoding", "br")
	http.ServeContent(w, r, path, st.ModTime(), f)
	// fmt.Printf("serveFileMaybeBr: served '%s'\n", pathBr)
	return true
}

func compressBr(path string, pathBr string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	dst, err := os.Create(pathBr)
	if err != nil {
		return err
	}
	w := brotli.NewWriterLevel(dst, brotli.BestCompression)
	_, err = io.Copy(w, f)
	err2 := w.Close()
	err3 := dst.Close()

	if err != nil || err2 != nil || err3 != nil {
		os.Remove(pathBr)
		if err != nil {
			return err
		}
		if err2 != nil {
			return err2
		}
		return err3
	}
	return nil
}
