package httputil

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"time"
)

// can be used for http.Get() requests with better timeouts. New one must be created
// for each Get() request
func newTimeoutClient(connectTimeout time.Duration, readWriteTimeout time.Duration) *http.Client {
	timeoutDialer := func(cTimeout time.Duration, rwTimeout time.Duration) func(net, addr string) (c net.Conn, err error) {
		return func(netw, addr string) (net.Conn, error) {
			conn, err := net.DialTimeout(netw, addr, cTimeout)
			if err != nil {
				return nil, err
			}
			conn.SetDeadline(time.Now().Add(rwTimeout))
			return conn, nil
		}
	}

	return &http.Client{
		Transport: &http.Transport{
			Dial:  timeoutDialer(connectTimeout, readWriteTimeout),
			Proxy: http.ProxyFromEnvironment,
		},
	}
}

func Post(uri string, body []byte) ([]byte, error) {
	// default timeout for http.Get() is really long, so dial it down
	// for both connection and read/write timeouts
	timeoutClient := newTimeoutClient(time.Second*120, time.Second*120)
	resp, err := timeoutClient.Post(uri, "", bytes.NewBuffer(body))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("'%s': status code not 200 (%d)", uri, resp.StatusCode)
	}
	return ioutil.ReadAll(resp.Body)
}
