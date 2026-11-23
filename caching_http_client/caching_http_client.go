package caching_http_client

import (
	"bytes"
	"io"
	"net/http"
	"sync"
	"time"
)

// readBody reads and closes body. Returns non-nil (possibly empty) slice
// so that it can be used as cachedBody in FindCachedResponse
func readBody(body io.ReadCloser) ([]byte, error) {
	if body == nil || body == http.NoBody {
		return []byte{}, nil
	}
	d, err := io.ReadAll(body)
	err2 := body.Close()
	if err != nil {
		return nil, err
	}
	if err2 != nil {
		return nil, err2
	}
	if d == nil {
		d = []byte{}
	}
	return d, nil
}

func newBody(d []byte) io.ReadCloser {
	return io.NopCloser(bytes.NewReader(d))
}

var _ http.RoundTripper = &CachingRoundTripper{}

// CachingRoundTripper is a http round-tripper that implements caching
// of past requests. It's safe for concurrent use if Cache is
// (MemoryCache is).
type CachingRoundTripper struct {
	Cache RequestResponseCache
	// this is RoundTripper to use to make the actual request
	// if nil, will use http.DefaultTransport
	RoundTripper http.RoundTripper

	// for diagnostics, you can check how many http requests
	// were served from a cache and how many from network requests.
	// Use Stats() to read them while requests might be in progress.
	RequestsFromCache    int `json:"-"`
	RequestsNotFromCache int `json:"-"`

	mu sync.Mutex
}

// Stats returns number of requests served from cache and from network.
// Safe to call concurrently with requests.
func (t *CachingRoundTripper) Stats() (fromCache int, notFromCache int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.RequestsFromCache, t.RequestsNotFromCache
}

func (t *CachingRoundTripper) cachedRoundTrip(r *http.Request, requestBody []byte) (*http.Response, error) {
	transport := t.RoundTripper
	if transport == nil {
		transport = http.DefaultTransport
	}
	rsp, err := transport.RoundTrip(r)
	if err != nil {
		return rsp, err
	}

	// only cache 200 responses
	if rsp.StatusCode != 200 {
		return rsp, nil
	}

	d, err := readBody(rsp.Body)
	if err != nil {
		return nil, err
	}
	rsp.Body = newBody(d)

	rr := &RequestResponse{
		Method: r.Method,
		URL:    r.URL.String(),
		Body:   requestBody,

		Response: d,
		// clone so that caller modifying rsp.Header doesn't change cached value
		Header: rsp.Header.Clone(),
	}
	t.Cache.Add(rr)
	t.mu.Lock()
	t.RequestsNotFromCache++
	t.mu.Unlock()
	return rsp, nil
}

// RoundTrip is to satisfy http.RoundTripper interface
func (t *CachingRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	// per http.RoundTripper contract we must not modify r and must close r.Body
	// so we read the body and send a clone of the request with a copy of the body
	requestBody, err := readBody(r.Body)
	if err != nil {
		return nil, err
	}
	r2 := r.Clone(r.Context())
	r2.Body = http.NoBody
	if len(requestBody) > 0 {
		r2.Body = newBody(requestBody)
		r2.GetBody = func() (io.ReadCloser, error) {
			return newBody(requestBody), nil
		}
	}

	cachedBody := requestBody
	rr, err := t.Cache.FindCachedResponse(r2, &cachedBody)
	if err != nil {
		return nil, err
	}

	if rr == nil {
		return t.cachedRoundTrip(r2, requestBody)
	}

	t.mu.Lock()
	t.RequestsFromCache++
	t.mu.Unlock()
	d := rr.Response
	rsp := &http.Response{
		Status:        "200 OK",
		StatusCode:    200,
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Header:        rr.Header.Clone(),
		Body:          newBody(d),
		ContentLength: int64(len(d)),
		Request:       r,
	}
	return rsp, nil
}

// NewRoundTripper creates http.RoundTripper that caches requests
func NewRoundTripper(cache RequestResponseCache) *CachingRoundTripper {
	if cache == nil {
		cache = NewMemoryCache()
	}
	return &CachingRoundTripper{
		Cache: cache,
	}
}

// New creates http.Client with 30 seconds timeout that caches requests
// in cache (if nil, a new MemoryCache is created)
func New(cache *MemoryCache) *http.Client {
	if cache == nil {
		cache = NewMemoryCache()
	}
	c := *http.DefaultClient
	c.Timeout = time.Second * 30
	origTransport := c.Transport
	c.Transport = &CachingRoundTripper{
		Cache:        cache,
		RoundTripper: origTransport,
	}
	return &c
}

// GetCache gets from the client if it's a client created by us
func GetCache(c *http.Client) RequestResponseCache {
	rt := GetCachingRoundTripper(c)
	if rt == nil {
		return nil
	}
	return rt.Cache
}

// GetCachingRoundTripper returns CachingRoundTripper from c.Transport
func GetCachingRoundTripper(c *http.Client) *CachingRoundTripper {
	if c == nil {
		return nil
	}
	t := c.Transport
	if t == nil {
		return nil
	}
	if ct, ok := t.(*CachingRoundTripper); ok {
		return ct
	}
	return nil
}
