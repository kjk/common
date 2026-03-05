package caching_http_client

import (
	"bytes"
	"encoding/json"
	"net/http"

	"github.com/tidwall/pretty"
)

var _ RequestResponseCache = &MemoryCache{}

// MemoryCache remembers past requests and responses
type MemoryCache struct {
	// CachedRequests remembers past requests and their responses
	CachedRequests []*RequestResponse `json:"cached_requests"`

	// if true, will not return cached responses (but will still
	// record requests / responses)
	// Useful for tracing requests (but only those that return 200)
	DisableRespondingFromCache bool

	// if true, when comparing body of the request, and the body
	// is json, we'll normalize JSON
	CompareNormalizedJSONBody bool
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
	c.CachedRequests = append(c.CachedRequests, rr)
}

func (c *MemoryCache) isBodySame(r *http.Request, rr *RequestResponse, cachedBody *[]byte) (bool, error) {
	// only POST request takes body
	if r.Method != http.MethodPost {
		return true, nil
	}
	if r.Body == nil && len(rr.Body) == 0 {
		return true, nil
	}

	d := *cachedBody
	if d == nil {
		var err error
		d, err = readAndReplaceReadCloser(&r.Body)
		if err != nil {
			return false, err
		}
		if d == nil {
			*cachedBody = []byte{}
		} else {
			if c.CompareNormalizedJSONBody {
				d = ppJSON(d)
			}
			*cachedBody = d
		}
	}
	rrBody := rr.Body
	if c.CompareNormalizedJSONBody {
		rrBody = ppJSON(rr.Body)
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

	for _, rr := range c.CachedRequests {
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
