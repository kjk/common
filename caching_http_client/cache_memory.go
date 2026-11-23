package caching_http_client

import (
	"bytes"
	"encoding/json"
	"net/http"
	"sync"

	"github.com/tidwall/pretty"
)

var _ RequestResponseCache = &MemoryCache{}

// MemoryCache remembers past requests and responses.
// Add and FindCachedResponse are safe for concurrent use.
type MemoryCache struct {
	// CachedRequests remembers past requests and their responses.
	// Only access directly when no requests are in progress
	CachedRequests []*RequestResponse `json:"cached_requests"`

	// if true, will not return cached responses (but will still
	// record requests / responses)
	// Useful for tracing requests (but only those that return 200)
	DisableRespondingFromCache bool

	// if true, when comparing body of the request, and the body
	// is json, we'll normalize JSON
	CompareNormalizedJSONBody bool

	mu sync.Mutex
}

var prettyOpts = pretty.Options{
	Width:  80,
	Prefix: "",
	Indent: "  ",
	// sorting keys only slightly slower
	SortKeys: true,
}

// pretty-print if valid JSON. If not, return unchanged
// about 4x faster than naive version using json.Unmarshal() + json.Marshal()
func ppJSON(js []byte) []byte {
	if !json.Valid(js) {
		return js
	}
	return pretty.PrettyOptions(js, &prettyOpts)
}

// NewMemoryCache returns a cache for http requests
func NewMemoryCache() *MemoryCache {
	return &MemoryCache{}
}

// Add remembers a given RequestResponse
func (c *MemoryCache) Add(rr *RequestResponse) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.CachedRequests = append(c.CachedRequests, rr)
}

// isBodySame compares body of the request with body of cached request.
// Body is compared for all methods (POST, PUT, PATCH etc.).
// *cachedBody is the body of r. If nil, r.Body is read (and replaced
// with a copy) and *cachedBody is set
func (c *MemoryCache) isBodySame(r *http.Request, rr *RequestResponse, cachedBody *[]byte) (bool, error) {
	d := *cachedBody
	if d == nil {
		var err error
		d, err = readBody(r.Body)
		if err != nil {
			return false, err
		}
		r.Body = newBody(d)
		*cachedBody = d
	}
	if len(d) == 0 && len(rr.Body) == 0 {
		return true, nil
	}
	rrBody := rr.Body
	if c.CompareNormalizedJSONBody {
		// normalize copies, *cachedBody must remain the actual body
		d = ppJSON(d)
		rrBody = ppJSON(rrBody)
	}
	return bytes.Equal(d, rrBody), nil
}

func (c *MemoryCache) isCachedRequest(r *http.Request, rr *RequestResponse, cachedBody *[]byte) (bool, error) {
	if rr.Method != r.Method {
		return false, nil
	}
	uri1 := rr.URL
	uri2 := r.URL.String()
	if uri1 != uri2 {
		return false, nil
	}
	return c.isBodySame(r, rr, cachedBody)
}

// FindCachedResponse returns
func (c *MemoryCache) FindCachedResponse(r *http.Request, cachedBody *[]byte) (*RequestResponse, error) {
	if c.DisableRespondingFromCache {
		return nil, nil
	}

	// iterate over a snapshot so that we don't hold the lock while comparing
	c.mu.Lock()
	cached := c.CachedRequests
	c.mu.Unlock()
	for _, rr := range cached {
		same, err := c.isCachedRequest(r, rr, cachedBody)
		if err != nil {
			return nil, err
		}
		if same {
			return rr, nil
		}
	}
	return nil, nil
}
