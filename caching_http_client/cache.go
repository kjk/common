package caching_http_client

import "net/http"

// RequestResponse is a cache entry. It remembers important details
// of the request and response
type RequestResponse struct {
	Method string `json:"method"`
	URL    string `json:"url"`
	Body   []byte `json:"body"`

	Response []byte      `json:"response"`
	Header   http.Header `json:"header"`
}

// RequestResponseCache defines interface
type RequestResponseCache interface {
	Add(rr *RequestResponse)
	FindCachedResponse(r *http.Request, cachedBody *[]byte) (*RequestResponse, error)
}
