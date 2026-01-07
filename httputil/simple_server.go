package httputil

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/kjk/common/u"
)

type SimpleServerOptions struct {
	Dir         string
	HTTPAddress string // e.g. ":8080" or
}

// run HTTP server serving a given directory
func SimpleServer(opts SimpleServerOptions) error {
	if !u.DirExists(opts.Dir) {
		return fmt.Errorf("directory '%s' doesn't exist", opts.Dir)
	}
	if opts.HTTPAddress == "" {
		return errors.New("need to provide opts.HTTPAddress")
	}

	findFileForURL := func(name string) string {
		path := ""
		fileExists := func(name string) bool {
			path = filepath.Join(opts.Dir, name)
			return u.FileExists(path)
		}

		name = strings.TrimPrefix(name, "/")
		name = filepath.FromSlash(name)
		if fileExists(name) {
			return path
		}
		// foo/ => foo/index.html
		if fileExists(name + "index.html") {
			return path
		}
		// foo/bar => foo/bar.html
		if fileExists(name + ".html") {
			return path
		}
		return ""
	}

	handlerFn := func(w http.ResponseWriter, r *http.Request) {
		path := findFileForURL(r.URL.Path)
		if path != "" {
			http.ServeFile(w, r, path)
			return
		}

		// TODO: serve custom 404.html
		http.NotFound(w, r)
	}

	mux := &http.ServeMux{}
	mux.HandleFunc("/", handlerFn)
	var handler http.Handler = mux
	httpSrv := &http.Server{
		ReadTimeout:  120 * time.Second,
		WriteTimeout: 120 * time.Second,
		IdleTimeout:  120 * time.Second, // introduced in Go 1.8
		Handler:      handler,
	}
	httpSrv.Addr = opts.HTTPAddress
	chServerClosed := make(chan bool, 1)
	go func() {
		err := httpSrv.ListenAndServe()
		// mute error caused by Shutdown()
		if err == http.ErrServerClosed {
			err = nil
		}
		u.Must(err)
		chServerClosed <- true
	}()

	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt /* SIGINT */, syscall.SIGTERM)
	<-c

	if httpSrv != nil {
		// Shutdown() needs a non-nil context
		go func() {
			_ = httpSrv.Shutdown(ctx())
		}()
		select {
		case <-chServerClosed:
			// do nothing
		case <-time.After(time.Second * 5):
			// timeout
		}
	}
	return nil
}

func ctx() context.Context {
	return context.Background()
}
