package httputil

import "net/http"

type CapturingResponseWriter struct {
	http.ResponseWriter
	StatusCode int
	Size       int64
}

func (w *CapturingResponseWriter) WriteHeader(statusCode int) {
	w.StatusCode = statusCode
	w.ResponseWriter.WriteHeader(statusCode)
}

func (w *CapturingResponseWriter) Write(d []byte) (int, error) {
	w.Size += int64(len(d))
	return w.ResponseWriter.Write(d)
}
