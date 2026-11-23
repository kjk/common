package caching_http_client

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/kjk/common/assert"
)

// echoServer responds with method and request body; counts requests
func echoServer(t *testing.T) (*httptest.Server, *atomic.Int32) {
	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n.Add(1)
		body, _ := io.ReadAll(r.Body)
		w.Header().Set("X-Test", "orig")
		fmt.Fprintf(w, "%s %s %s", r.Method, r.URL.Path, body)
	}))
	t.Cleanup(srv.Close)
	return srv, &n
}

func doReq(t *testing.T, c *http.Client, method, uri, body string) (string, http.Header) {
	var r io.Reader
	if body != "" {
		r = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, uri, r)
	assert.NoError(t, err)
	rsp, err := c.Do(req)
	assert.NoError(t, err)
	defer rsp.Body.Close()
	d, err := io.ReadAll(rsp.Body)
	assert.NoError(t, err)
	return string(d), rsp.Header
}

func TestBodyComparedForAllMethods(t *testing.T) {
	srv, n := echoServer(t)
	c := New(nil)
	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodPatch} {
		got, _ := doReq(t, c, method, srv.URL+"/x", "body1")
		assert.Equal(t, method+" /x body1", got)
		// different body must not be served from cache
		got, _ = doReq(t, c, method, srv.URL+"/x", "body2")
		assert.Equal(t, method+" /x body2", got)
		// same body is served from cache
		before := n.Load()
		got, _ = doReq(t, c, method, srv.URL+"/x", "body1")
		assert.Equal(t, method+" /x body1", got)
		assert.Equal(t, before, n.Load())
	}
}

func TestRequestNotModified(t *testing.T) {
	srv, _ := echoServer(t)
	rt := NewRoundTripper(nil)
	body := io.NopCloser(strings.NewReader("data"))
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/y", body)
	assert.NoError(t, err)
	origBody := req.Body
	rsp, err := rt.RoundTrip(req)
	assert.NoError(t, err)
	rsp.Body.Close()
	assert.True(t, req.Body == origBody, "RoundTrip must not replace request body")
}

func TestCachedHeaderIsCopy(t *testing.T) {
	srv, _ := echoServer(t)
	c := New(nil)
	_, hdr := doReq(t, c, http.MethodGet, srv.URL+"/h", "")
	hdr.Set("X-Test", "modified")
	_, hdr = doReq(t, c, http.MethodGet, srv.URL+"/h", "")
	assert.Equal(t, "orig", hdr.Get("X-Test"))
	hdr.Set("X-Test", "modified")
	_, hdr = doReq(t, c, http.MethodGet, srv.URL+"/h", "")
	assert.Equal(t, "orig", hdr.Get("X-Test"))

	rt := GetCachingRoundTripper(c)
	fromCache, notFromCache := rt.Stats()
	assert.Equal(t, 2, fromCache)
	assert.Equal(t, 1, notFromCache)
}

func TestConcurrentUse(t *testing.T) {
	srv, _ := echoServer(t)
	c := New(nil)
	var wg sync.WaitGroup
	for i := range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range 20 {
				uri := fmt.Sprintf("%s/c%d", srv.URL, (i+j)%7)
				got, _ := doReq(t, c, http.MethodGet, uri, "")
				assert.Equal(t, fmt.Sprintf("GET /c%d ", (i+j)%7), got)
			}
		}()
	}
	wg.Wait()
	rt := GetCachingRoundTripper(c)
	fromCache, notFromCache := rt.Stats()
	assert.Equal(t, 400, fromCache+notFromCache)
}
