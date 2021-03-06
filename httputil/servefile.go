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

type ServeFileOptions struct {
	Dir              string
	SupportCleanURLS bool
	ForceCleanURLS   bool
	ServeCompressed  bool
}

func TryServeFile(w http.ResponseWriter, r *http.Request, opts *ServeFileOptions) bool {
	urlPath := r.URL.Path
	return TryServeFileFromURL(w, r, urlPath, opts)
}

func TryServeFileFromURL(w http.ResponseWriter, r *http.Request, urlPath string, opts *ServeFileOptions) bool {
	if opts.ForceCleanURLS {
		ext := filepath.Ext(urlPath)
		if strings.EqualFold(ext, ".html") {
			path := filepath.Join(opts.Dir, urlPath)
			if u.FileExists(path) {
				urlPath = urlPath[:len(urlPath)-len(ext)]
				SmartPermanentRedirect(w, r, urlPath)
				return true
			}
		}
	}
	if urlPath == "/" {
		urlPath = "index.html"
	}
	path := filepath.Join(opts.Dir, urlPath)
	cleanURLS := opts.SupportCleanURLS || opts.ForceCleanURLS
	if !u.FileExists(path) && cleanURLS {
		path = path + ".html"
		if !u.FileExists(path) {
			return false
		}
	}

	if r != nil && opts.ServeCompressed {
		if serveFileMaybeBr(w, r, path) {
			return true
		}
		// TODO: maybe add serveFileMaybeGz
		// but then again modern browsers probably support br so that would be redundant
	}
	ct := u.MimeTypeFromFileName(path)
	if ct != "" {
		w.Header().Set("Content-Type", ct)
	}
	if r == nil {
		// we can be used by server.makeServeFile which doesn't provide http.Request
		// in that case w is a file
		f, err := os.Open(path)
		if err != nil {
			return false
		}
		defer f.Close()
		_, err = io.Copy(w, f)
		return err == nil
	}
	http.ServeFile(w, r, path)
	return true
}

func serveFileMaybeBr(w http.ResponseWriter, r *http.Request, path string) bool {
	ext := strings.ToLower(filepath.Ext(path))
	switch ext {
	case ".html", ".txt", ".css", ".js", ".xml", ".svg":
		// no-op those we serve compressed
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
