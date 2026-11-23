package httputil

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/kjk/common/assert"
	"github.com/kjk/common/u"
)

func newTestFS() *u.MemoryFS {
	return u.NewMemoryFS(map[string][]byte{
		"dist/index.html":     []byte("root index"),
		"dist/foo.html":       []byte("foo"),
		"dist/bar/index.html": []byte("bar index"),
		"dist/style.css":      []byte("css"),
		"dist/pre.html":       []byte("pre"),
		"dist/pre.html.br":    []byte("pre-compressed"),
		"secret.txt":          []byte("secret"),
		"top.html":            []byte("top"),
	})
}

type serveResult struct {
	served   bool
	code     int
	body     string
	location string
}

func serveURL(opts *ServeFileOptions, urlPath string, hdrs ...string) serveResult {
	r := httptest.NewRequest("GET", "http://example.com/", nil)
	r.URL.Path = urlPath // set directly so it's not cleaned
	for i := 0; i+1 < len(hdrs); i += 2 {
		r.Header.Set(hdrs[i], hdrs[i+1])
	}
	w := httptest.NewRecorder()
	served := TryServeURLFromFS(w, r, opts)
	res := w.Result()
	body, _ := io.ReadAll(res.Body)
	return serveResult{
		served:   served,
		code:     res.StatusCode,
		body:     string(body),
		location: res.Header.Get("Location"),
	}
}

func TestTryServeURLFromFS(t *testing.T) {
	opts := &ServeFileOptions{FS: newTestFS(), DirPrefix: "dist", SupportCleanURLS: true}

	res := serveURL(opts, "/")
	assert.True(t, res.served)
	assert.Equal(t, "root index", res.body)

	res = serveURL(opts, "/bar/")
	assert.Equal(t, "bar index", res.body)

	res = serveURL(opts, "/foo")
	assert.Equal(t, "foo", res.body)

	res = serveURL(opts, "/foo.html")
	assert.Equal(t, "foo", res.body)

	res = serveURL(opts, "/style.css")
	assert.Equal(t, "css", res.body)

	res = serveURL(opts, "/missing")
	assert.False(t, res.served)

	// must not escape DirPrefix
	res = serveURL(opts, "/../secret.txt")
	assert.False(t, res.served)
	res = serveURL(opts, "/../top")
	assert.False(t, res.served)
}

func TestTryServeURLFromFSEmptyDirPrefix(t *testing.T) {
	opts := &ServeFileOptions{FS: newTestFS(), SupportCleanURLS: true}
	res := serveURL(opts, "/top.html")
	assert.True(t, res.served)
	assert.Equal(t, "top", res.body)
	res = serveURL(opts, "/top")
	assert.Equal(t, "top", res.body)
	res = serveURL(opts, "/dist/")
	assert.Equal(t, "root index", res.body)
}

func TestTryServeURLFromFSForceCleanURLS(t *testing.T) {
	opts := &ServeFileOptions{FS: newTestFS(), DirPrefix: "dist", ForceCleanURLS: true}

	res := serveURL(opts, "/foo.html")
	assert.Equal(t, http.StatusMovedPermanently, res.code)
	assert.Equal(t, "/foo", res.location)

	res = serveURL(opts, "/bar/index.html")
	assert.Equal(t, http.StatusMovedPermanently, res.code)
	assert.Equal(t, "/bar/", res.location)

	res = serveURL(opts, "/index.html")
	assert.Equal(t, http.StatusMovedPermanently, res.code)
	assert.Equal(t, "/", res.location)

	// directory urls are served, not redirected
	res = serveURL(opts, "/")
	assert.Equal(t, http.StatusOK, res.code)
	assert.Equal(t, "root index", res.body)
	res = serveURL(opts, "/bar/")
	assert.Equal(t, "bar index", res.body)

	res = serveURL(opts, "/foo")
	assert.Equal(t, "foo", res.body)
}

func TestServePrecompressedBr(t *testing.T) {
	// used to panic on nil compressedCached map
	opts := &ServeFileOptions{FS: newTestFS(), DirPrefix: "dist"}
	res := serveURL(opts, "/pre.html", "Accept-Encoding", "gzip, br")
	assert.True(t, res.served)
	assert.Equal(t, "pre-compressed", res.body)

	// br;q=0 means: don't send br
	res = serveURL(opts, "/pre.html", "Accept-Encoding", "gzip, br;q=0")
	assert.Equal(t, "pre", res.body)
}

func TestCanServeBr(t *testing.T) {
	tests := []struct {
		enc string
		exp bool
	}{
		{"", false},
		{"gzip", false},
		{"br", true},
		{"gzip, deflate, br", true},
		{"gzip, br;q=0.5", true},
		{"br;q=0", false},
		{"br; q=0.0", false},
		{"BR", true},
		{"brotli", false},
	}
	for _, tc := range tests {
		r := httptest.NewRequest("GET", "/", nil)
		r.Header.Set("Accept-Encoding", tc.enc)
		assert.Equal(t, tc.exp, canServeBr(r), tc.enc)
	}
}
