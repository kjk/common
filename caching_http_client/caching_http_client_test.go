package caching_http_client

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/alecthomas/assert"
)

var (
	mu        sync.Mutex
	startPort = 5892
	httpAddr  = ""
	httpRoot  = ""
)

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func listenOnUniquePort(httpSrv *http.Server) {
	var err error
	// there's a very small chance of port conflict
	for i := 0; i < 10; i++ {
		port := startPort + i
		mu.Lock()
		// running explicitly on 127.0.0.1 to not trigger windows firewall
		httpAddr = fmt.Sprintf("127.0.0.1:%d", port)
		httpRoot = "http://" + httpAddr
		httpSrv.Addr = httpAddr
		mu.Unlock()
		err := httpSrv.ListenAndServe()
		// mute error caused by Shutdown()
		if err == http.ErrServerClosed {
			return
		}
	}
	must(err)
}

func getHTTPRoot() string {
	mu.Lock()
	defer mu.Unlock()
	return httpRoot
}

func waitForServerToStart() {
	// wait for the server to start
	for i := 0; i < 10; i++ {
		time.Sleep(100 * time.Millisecond)
		uri := getHTTPRoot() + "/ping"
		req, err := http.NewRequest(http.MethodGet, uri, nil)
		must(err)
		rsp, err := http.DefaultClient.Do(req)
		if err != nil {
			continue
		}
		rsp.Body.Close()
		return
	}
}

func startServer() func() {

	handler := func(w http.ResponseWriter, r *http.Request) {
		uri := r.URL.String()
		rsp := "pong"
		if uri != "/ping" {
			rsp = fmt.Sprintf("URL: %s\n", uri)
		}
		w.Write([]byte(rsp))
	}

	mux := &http.ServeMux{}
	mux.HandleFunc("/", handler)
	httpSrv := &http.Server{
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
		IdleTimeout:  120 * time.Second, // introduced in Go 1.8
		Handler:      mux,
	}

	go listenOnUniquePort(httpSrv)

	waitForServerToStart()

	closeServer := func() {
	}
	return closeServer
}

func TestDidCache(t *testing.T) {
	cancel := startServer()
	defer cancel()
	cache := NewMemoryCache()
	assert.Equal(t, 0, len(cache.CachedRequests))

	client := New(cache)
	cache2 := GetCache(client)
	assert.Equal(t, cache, cache2)

	uri := getHTTPRoot() + "/test"

	var rspBody []byte
	tr := GetCachingRoundTripper(client)
	assert.NotNil(t, tr)
	//var err error
	{
		req, err := http.NewRequest(http.MethodGet, uri, nil)
		assert.NoError(t, err)
		rsp, err := client.Do(req)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(cache.CachedRequests))
		assert.Equal(t, 0, tr.RequestsFromCache)
		assert.Equal(t, 1, tr.RequestsNotFromCache)
		rspBody, err = ioutil.ReadAll(rsp.Body)
		assert.NoError(t, err)
		rsp.Body.Close()
	}

	{
		var cachedBody []byte
		req, err := http.NewRequest(http.MethodGet, uri, nil)
		assert.NoError(t, err)
		rr, err := cache.FindCachedResponse(req, &cachedBody)
		assert.NoError(t, err)
		assert.NotNil(t, rr)
		assert.Equal(t, rspBody, rr.Response)
	}

	{
		req, err := http.NewRequest(http.MethodGet, uri, nil)
		assert.NoError(t, err)
		rsp, err := client.Do(req)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(cache.CachedRequests))
		assert.Equal(t, 1, tr.RequestsNotFromCache)
		assert.Equal(t, 1, tr.RequestsFromCache)
		rsp.Body.Close()
	}
}
