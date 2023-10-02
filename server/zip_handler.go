package server

import (
	"archive/zip"
	"net/http"
	"strings"

	"github.com/kjk/common/httputil"
	"github.com/kjk/common/u"
)

type ZipHandler struct {
	URLPrefix string

	URL     []string
	content [][]byte // same order as URL
}

func (h *ZipHandler) Get(uri string) func(w http.ResponseWriter, r *http.Request) {
	for i, u := range h.URL {
		// urls are case-insensitive
		if strings.EqualFold(u, uri) {
			return MakeServeContent(uri, h.content[i])
		}
	}
	return nil
}

func (h *ZipHandler) URLS() []string {
	return h.URL
}

func NewZipHandler(zipData []byte, urlPrefix string) (*ZipHandler, error) {
	var urls []string
	var content [][]byte

	err := u.IterZipData(zipData, func(f *zip.File, data []byte) error {
		uri := httputil.JoinURL(urlPrefix, f.Name)
		urls = append(urls, uri)
		content = append(content, data)
		return nil
	})
	if err != nil {
		return nil, err
	}

	return &ZipHandler{
		URLPrefix: urlPrefix,
		URL:       urls,
		content:   content,
	}, nil
}
