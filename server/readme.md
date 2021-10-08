Those are useful snippets that use the server code for re-use.
Copy & paste & modify for your purpose.

```go
import (
	"github.com/kjk/cheatsheets/pkg/server"
)
```

```go
func MakeHTTPServer(srv *server.Server) *http.Server {
	panicIf(srv == nil, "must provide files")
	httpPort := 8080
	if srv.Port != 0 {
		httpPort = srv.Port
	}
	httpAddr := fmt.Sprintf(":%d", httpPort)
	if isWindows() {
		httpAddr = "localhost" + httpAddr
	}
	httpSrv := &http.Server{
		ReadTimeout:  120 * time.Second,
		WriteTimeout: 120 * time.Second,
		IdleTimeout:  120 * time.Second, // introduced in Go 1.8
		Handler:      srv,
	}
	httpSrv.Addr = httpAddr
	return httpSrv
}

// returns function that will wait for SIGTERM signal (e.g. Ctrl-C) and
// shutdown the server
func StartHTTPServer(httpSrv *http.Server) func() {
	logf(ctx(), "Starting server on http://%s'\n", httpSrv.Addr)
	if isWindows() {
		openBrowser(fmt.Sprintf("http://%s", httpSrv.Addr))
	}

	chServerClosed := make(chan bool, 1)
	go func() {
		err := httpSrv.ListenAndServe()
		// mute error caused by Shutdown()
		if err == http.ErrServerClosed {
			err = nil
		}
		must(err)
		logf(ctx(), "trying to shutdown HTTP server\n")
		chServerClosed <- true
	}()

	return func() {
		c := make(chan os.Signal, 2)
		signal.Notify(c, os.Interrupt /* SIGINT */, syscall.SIGTERM)

		sig := <-c
		logf(ctx(), "Got signal %s\n", sig)

		if httpSrv != nil {
			go func() {
				// Shutdown() needs a non-nil context
				_ = httpSrv.Shutdown(ctx())
			}()
			select {
			case <-chServerClosed:
				// do nothing
				logf(ctx(), "server shutdown cleanly\n")
			case <-time.After(time.Second * 5):
				// timeout
				logf(ctx(), "server killed due to shutdown timeout\n")
			}
		}
	}
}

func StartServer(srv *server.Server) func() {
	httpServer := MakeHTTPServer(srv)
	return StartHTTPServer(httpServer)
}
```

```

Utility functions used above:

```go
func must(err error) {
	if err != nil {
		panic(err.Error())
	}
}

func ctx() context.Context {
	return context.Background()
}

func panicIf(cond bool, arg ...interface{}) {
	if !cond {
		return
	}
	s := "condition failed"
	if len(arg) > 0 {
		s = fmt.Sprintf("%s", arg[0])
		if len(arg) > 1 {
			s = fmt.Sprintf(s, arg[1:]...)
		}
	}
	panic(s)
}

func isWindows() bool {
	return strings.Contains(runtime.GOOS, "windows")
}

func openBrowser(url string) {
	var err error

	switch runtime.GOOS {
	case "linux":
		err = exec.Command("xdg-open", url).Start()
	case "windows":
		err = exec.Command("rundll32", "url.dll,FileProtocolHandler", url).Start()
	case "darwin":
		err = exec.Command("open", url).Start()
	default:
		err = fmt.Errorf("unsupported platform")
	}
	if err != nil {
		log.Fatal(err)
	}
}

func isWindows() bool {
	return strings.Contains(runtime.GOOS, "windows")
}

func formatSize(n int64) string {
	sizes := []int64{1024 * 1024 * 1024, 1024 * 1024, 1024}
	suffixes := []string{"GB", "MB", "kB"}

	for i, size := range sizes {
		if n >= size {
			s := fmt.Sprintf("%.2f", float64(n)/float64(size))
			return strings.TrimSuffix(s, ".00") + " " + suffixes[i]
		}
	}
	return fmt.Sprintf("%d bytes", n)
}

```