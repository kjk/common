package server

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/kjk/common/assert"
)

func findAndServe(s *Server, uri string) *httptest.ResponseRecorder {
	h, _ := s.FindHandler(uri)
	w := httptest.NewRecorder()
	if h != nil {
		r := httptest.NewRequest("GET", uri, nil)
		h(w, r)
	}
	return w
}

func TestFindHandlerForceCleanURLS(t *testing.T) {
	files := NewInMemoryFilesHandler("/index.html", []byte("root"))
	files.Add("/foo.html", []byte("foo"))
	files.Add("/bar/index.html", []byte("bar"))
	s := &Server{Handlers: []Handler{files}, ForceCleanURLS: true}

	// directory urls are served, not redirected to /index
	w := findAndServe(s, "/")
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "root", w.Body.String())
	w = findAndServe(s, "/bar/")
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "bar", w.Body.String())

	w = findAndServe(s, "/foo.html")
	assert.Equal(t, http.StatusMovedPermanently, w.Code)
	assert.Equal(t, "/foo", w.Header().Get("Location"))

	w = findAndServe(s, "/bar/index.html")
	assert.Equal(t, http.StatusMovedPermanently, w.Code)
	assert.Equal(t, "/bar/", w.Header().Get("Location"))

	w = findAndServe(s, "/foo")
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "foo", w.Body.String())
}
