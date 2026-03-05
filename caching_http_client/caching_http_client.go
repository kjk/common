package caching_http_client

import (
	"bytes"
	"io"
	"io/ioutil"
	"net/http"
	"time"
)

// closeableBuffer adds Close() error method to bytes.Buffer
// to satisfy io.ReadCloser interface
type closeableBuffer struct {
	*bytes.Buffer
}

// Close is to satisfy io.Closer interface
func (b *closeableBuffer) Close() error {
	// nothing to do
	return nil
}

func readAndReplaceReadCloser(pBody *io.ReadCloser) ([]byte, error) {
	// have to read the body from r and put it back
	var err error
	body := *pBody
	// not all requests have body
	if body == nil {
		return nil, nil
	}
	d, err := ioutil.ReadAll(body)
	if err != nil {
		return nil, err
	}
	buf := &closeableBuffer{bytes.NewBuffer(d)}
	*pBody = buf
	return d, nil
}

var _ http.RoundTripper = &CachingRoundTripper{}

// CachingRoundTripper is a http round-tripper that implements caching
// of past requests
type CachingRoundTripper struct {
	Cache RequestResponseCache
	// this is RoundTripper to use to make the actual request
	// if nil, will use http.DefaultTransport
	RoundTripper http.RoundTripper

	// for diagnostics, you can check how many http requests
	// were served from a cache and how many from network requests
	RequestsFromCache    int `json:"-"`
	RequestsNotFromCache int `json:"-"`
}

func (t *CachingRoundTripper) cachedRoundTrip(r *http.Request, cachedRequestBody []byte) (*http.Response, error) {
	transport := t.RoundTripper
	if transport == nil {
		transport = http.DefaultTransport
	}
	if cachedRequestBody == nil {
		var err error
		cachedRequestBody, err = readAndReplaceReadCloser(&r.Body)
		if err != nil {
			return nil, err
		}
	}
	rsp, err := transport.RoundTrip(r)
	if err != nil {
		return rsp, err
	}

	// only cache 200 responses
	if rsp.StatusCode != 200 {
		return rsp, nil
	}

	d, err := readAndReplaceReadCloser(&rsp.Body)
	if err != nil {
		return nil, err
	}

	rr := &RequestResponse{
		Method: r.Method,
		URL:    r.URL.String(),
		Body:   cachedRequestBody,

		Response: d,
		Header:   rsp.Header,
	}
	t.Cache.Add(rr)
	t.RequestsNotFromCache++
	return rsp, nil
}

// RoundTrip is to satisfy http.RoundTripper interface
func (t *CachingRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	var cachedRequestBody []byte
	rr, err := t.Cache.FindCachedResponse(r, &cachedRequestBody)
	if err != nil {
		return nil, err
	}

	if rr == nil {
		return t.cachedRoundTrip(r, cachedRequestBody)
	}

	t.RequestsFromCache++
	d := rr.Response
	rsp := &http.Response{
		Status:        "200",
		StatusCode:    200,
		Header:        rr.Header,
		Body:          &closeableBuffer{bytes.NewBuffer(d)},
		ContentLength: int64(len(d)),
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

// New creates http.Client
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
