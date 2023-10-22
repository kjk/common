package server

import (
	"embed"
	"io/fs"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/kjk/common/u"
)

type EmbedFSHandler struct {
	fs        embed.FS
	URLPrefix string
	urls      []string
	paths     []string // same order as URL
	modTime   time.Time
}

func NewEmbedFSHandler(fsys embed.FS, dirPrefix, urlPrefix string) *EmbedFSHandler {
	var urls, paths []string
	u.IterReadDirFS(fsys, dirPrefix, func(filePath string, d fs.DirEntry) error {
		dir := strings.TrimPrefix(filePath, dirPrefix)
		dir = strings.TrimPrefix(dir, dirPrefix)
		// embed.FS uses "/" as path separator
		uri := path.Join(urlPrefix, dir)
		urls = append(urls, uri)
		paths = append(paths, filePath)
		return nil
	})
	u.PanicIf(len(urls) == 0)
	return &EmbedFSHandler{
		fs:        fsys,
		URLPrefix: urlPrefix,
		urls:      urls,
		paths:     paths,
		modTime:   time.Now(),
	}
}

func (h *EmbedFSHandler) URLS() []string {
	return h.urls
}

func (h *EmbedFSHandler) Get(uri string) func(w http.ResponseWriter, r *http.Request) {
	for i, url := range h.urls {
		// urls are case-insensitive
		if strings.EqualFold(url, uri) {
			code := http.StatusOK
			if strings.HasSuffix(uri, "/404.html") {
				code = http.StatusNotFound
			}
			path := h.paths[i]
			d, err := fs.ReadFile(h.fs, path)
			u.PanicIfErr(err)
			return MakeServeContent(uri, d, code, h.modTime)
		}
	}
	return nil
}
