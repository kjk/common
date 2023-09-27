package httputil

import (
	"bytes"
	"fmt"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/kjk/common/u"
)

// can be used for http.Get() requests with better timeouts. New one must be created
// for each Get() request
func NewTimeoutClient(connectTimeout time.Duration, readWriteTimeout time.Duration) *http.Client {
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

func NewDefaultTimeoutClient() *http.Client {
	return NewTimeoutClient(time.Second*120, time.Second*120)
}

func Get(uri string) ([]byte, error) {
	c := NewDefaultTimeoutClient()
	resp, err := c.Get(uri)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("'%s': status code not 200 (%d)", uri, resp.StatusCode)
	}
	return io.ReadAll(resp.Body)
}

func GetToFile(uri string, f *os.File) error {
	c := NewDefaultTimeoutClient()
	resp, err := c.Get(uri)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("'%s': status code not 200 (%d)", uri, resp.StatusCode)
	}
	_, err = io.Copy(f, resp.Body)
	return err
}

func Post(uri string, body []byte) ([]byte, error) {
	c := NewDefaultTimeoutClient()
	resp, err := c.Post(uri, "", bytes.NewBuffer(body))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("'%s': status code not 200 (%d)", uri, resp.StatusCode)
	}
	return io.ReadAll(resp.Body)
}

func createMultiPartForm(form map[string]string) (string, io.Reader, error) {
	body := new(bytes.Buffer)
	mp := multipart.NewWriter(body)
	defer mp.Close()
	for key, val := range form {
		if strings.HasPrefix(val, "@") {
			val = val[1:]
			file, err := os.Open(val)
			if err != nil {
				return "", nil, err
			}
			defer file.Close()
			part, err := mp.CreateFormFile(key, val)
			if err != nil {
				return "", nil, err
			}
			io.Copy(part, file)
		} else {
			mp.WriteField(key, val)
		}
	}
	return mp.FormDataContentType(), body, nil
}

func PostMultiPart(uri string, files map[string]string) ([]byte, error) {
	contentType, body, err := createMultiPartForm(files)
	if err != nil {
		return nil, err
	}
	// default timeout for http.Get() is really long, so dial it down
	// for both connection and read/write timeouts
	timeoutClient := NewTimeoutClient(time.Second*120, time.Second*120)
	resp, err := timeoutClient.Post(uri, contentType, body)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("'%s': status code not 200 (%d)", uri, resp.StatusCode)
	}
	return io.ReadAll(resp.Body)
}

func JoinURL(s1, s2 string) string {
	if strings.HasSuffix(s1, "/") {
		if strings.HasPrefix(s2, "/") {
			return s1 + s2[1:]
		}
		return s1 + s2
	}

	if strings.HasPrefix(s2, "/") {
		return s1 + s2
	}
	return s1 + "/" + s2
}

func MakeFullRedirectURL(path string, reqURL *url.URL) string {
	// TODO: could verify that path is really a path
	// and doesn't have query / fragment
	if reqURL.RawQuery != "" {
		path = path + "?" + reqURL.RawQuery
	}
	if reqURL.Fragment != "" {
		path = path + "#" + reqURL.EscapedFragment()
	}
	return path
}

// SmartRedirect redirects to uri but also adds query / fragment from r.URL
func SmartRedirect(w http.ResponseWriter, r *http.Request, uri string, code int) {
	u.PanicIf(code < 300 || code >= 400)
	uri = MakeFullRedirectURL(uri, r.URL)
	http.Redirect(w, r, uri, code)
}

func SmartPermanentRedirect(w http.ResponseWriter, r *http.Request, uri string) {
	SmartRedirect(w, r, uri, http.StatusMovedPermanently) // 301
}
