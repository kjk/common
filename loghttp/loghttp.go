package loghttp

import (
	"context"
	"fmt"
	"io"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/andybalholm/brotli"
	"github.com/kjk/common/filerotate"
	"github.com/kjk/common/siser"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

var (
	logsDirCached = ""
	httpLogSiser  *siser.Writer
	httpLogRec    siser.Record
	httpLogMu     sync.Mutex
	httpLogApp    = ""
)

type Config struct {
	Dir     string
	AppName string

	// defines s3-copmatible storage
	// if not provided, will not upload
	Secret   string
	Access   string
	Bucket   string
	Endpoint string
}

func getLogsDir() string {
	if logsDirCached != "" {
		return logsDirCached
	}
	logsDirCached = "logs"
	must(os.MkdirAll(logsDirCached, 0755))
	return logsDirCached
}

// <dir>/httplog-2021-10-06_01.txt.br
// =>
//apps/cheatsheet/httplog/2021/10-06/2021-10-06_01.txt.br
// return "" if <path> is in unexpected format
func remotePathFromFilePath(path string) string {
	name := filepath.Base(path)
	parts := strings.Split(name, "_")
	if len(parts) != 2 {
		return ""
	}
	// parts[1]: 01.txt.br
	hr := strings.Split(parts[1], ".")[0]
	if len(hr) != 2 {
		return ""
	}
	// parts[0]: httplog-2021-10-06
	parts = strings.Split(parts[0], "-")
	if len(parts) != 4 {
		return ""
	}
	year := parts[1]
	month := parts[2]
	day := parts[3]
	name = fmt.Sprintf("%s/%s-%s/%s-%s-%s_%s.txt.br", year, month, day, year, month, day, hr)
	return fmt.Sprintf("apps/%s/httplog/%s", httpLogApp, name)
}

// upload httplog-2021-10-06_01.txt as
// apps/cheatsheet/httplog/2021/10-06/2021-10-06_01.txt.br
func uploadCompressedHTTPLog(path string) error {
	pathBr := path + ".br"
	createCompressed := func() error {
		r, err := os.Open(path)
		if err != nil {
			return err
		}
		defer r.Close()
		os.Remove(pathBr)
		f, err := os.Create(pathBr)
		if err != nil {
			return err
		}
		w := brotli.NewWriterLevel(f, brotli.BestCompression)
		_, err = io.Copy(w, r)
		err2 := w.Close()
		err3 := f.Close()
		if err != nil {
			return err
		}
		if err2 != nil {
			return err2
		}
		return err3
	}
	defer os.Remove(pathBr)

	// timeStart := time.Now()
	err := createCompressed()
	if err != nil {
		return err
	}
	/*
		dur := time.Since(timeStart)
			origSize := getFileSize(path)
			comprSize := getFileSize(pathBr)
			p := perc(origSize, comprSize)
			logf(ctx(), "uploadCompressedHTTPLog: compressed '%s' as '%s', %s => %s (%.2f%%) in %s\n", path, pathBr, formatSize(origSize), formatSize(comprSize), p, dur)
	*/
	// timeStart = time.Now()
	mc := newMinioSpacesClient()
	remotePath := remotePathFromFilePath(pathBr)
	if remotePath == "" {
		// logf(ctx(), "uploadCompressedHTTPLog: remotePathFromFilePath() failed for '%s'\n", pathBr)
		return nil
	}
	err = minioUploadFilePublic(mc, remotePath, pathBr)
	if err != nil {
		// logerrf(ctx(), "uploadCompressedHTTPLog: minioUploadFilePublic() failed with '%s'\n", err)
		return nil
	}
	// logf(ctx(), "uploadCompressedHTTPLog: uploaded '%s' as '%s' in %s\n", pathBr, remotePath, time.Since(timeStart))
	return nil
}

func didRotateHTTPLog(path string, didRotate bool) {
	canUpload := hasSpacesCreds()
	// logf(ctx(), "didRotateHTTPLog: '%s', didRotate: %v, hasSpacesCreds: %v\n", path, didRotate, canUpload)
	if !canUpload || !didRotate {
		return
	}
	go uploadCompressedHTTPLog(path)
}

func NewLogHourly(dir string, didClose func(path string, didRotate bool)) (*filerotate.File, error) {
	hourly := func(creationTime time.Time, now time.Time) string {
		if filerotate.IsSameHour(creationTime, now) {
			return ""
		}
		name := "httplog-" + now.Format("2006-01-02_15") + ".txt"
		path := filepath.Join(dir, name)
		// logf(ctx(), "NewLogHourly: '%s'\n", path)
		return path
	}
	config := filerotate.Config{
		DidClose:           didClose,
		PathIfShouldRotate: hourly,
	}
	return filerotate.New(&config)
}

func OpenHTTPLog(app string) func() {
	panicIf(app == "")
	dir := getLogsDir()

	logFile, err := NewLogHourly(dir, didRotateHTTPLog)
	must(err)
	httpLogSiser = siser.NewWriter(logFile)
	// TODO: should I change filerotate so that it opens the file immedaitely?
	return func() {
		_ = logFile.Close()
		httpLogSiser = nil
	}
}

var (
	hdrsToNotLog = []string{
		"Connection",
		"Sec-Ch-Ua-Mobile",
		"Sec-Fetch-Dest",
		"Sec-Ch-Ua-Platform",
		"Dnt",
		"Upgrade-Insecure-Requests",
		"Sec-Fetch-Site",
		"Sec-Fetch-Mode",
		"Sec-Fetch-User",
		"If-Modified-Since",
		"Accept-Language",
		"Cf-Ray",
		"CF-Visitor",
		"X-Request-Start",
		"Cdn-Loop",
		"X-Forwarded-Proto",
	}
	hdrsToNotLogMap map[string]bool
)

func shouldLogHeader(s string) bool {
	if hdrsToNotLogMap == nil {
		hdrsToNotLogMap = map[string]bool{}
		for _, h := range hdrsToNotLog {
			h = strings.ToLower(h)
			hdrsToNotLogMap[h] = true
		}
	}
	s = strings.ToLower(s)
	return !hdrsToNotLogMap[s]
}

func recWriteNonEmpty(rec *siser.Record, k, v string) {
	if v != "" {
		rec.Write(k, v)
	}
}

func LogHTTPReq(r *http.Request, code int, size int64, dur time.Duration) error {
	uri := r.URL.Path
	if strings.HasPrefix(uri, "/ping") {
		// our internal health monitoring endpoint is called frequently, don't log
		return nil
	}

	httpLogMu.Lock()
	defer httpLogMu.Unlock()

	if httpLogSiser == nil {
		return nil
	}

	rec := &httpLogRec
	rec.Reset()
	rec.Write("req", fmt.Sprintf("%s %s %d", r.Method, r.RequestURI, code))
	recWriteNonEmpty(rec, "host", r.Host)
	rec.Write("ipaddr", requestGetRemoteAddress(r))
	rec.Write("size", strconv.FormatInt(size, 10))
	durMicro := int64(dur / time.Microsecond)
	rec.Write("durmicro", strconv.FormatInt(durMicro, 10))

	// to minimize logging, we don't log headers if this is
	// self-referal
	skipLoggingHeaders := func() bool {
		ref := r.Header.Get("Referer")
		if ref == "" {
			return false
		}
		return strings.Contains(ref, r.Host)
	}

	if !skipLoggingHeaders() {
		for k, v := range r.Header {
			if !shouldLogHeader(k) {
				continue
			}
			if len(v) > 0 && len(v[0]) > 0 {
				rec.Write(k, v[0])
			}
		}
	}

	_, err := httpLogSiser.WriteRecord(rec)
	return err
}

// requestGetRemoteAddress returns ip address of the client making the request,
// taking into account http proxies
func requestGetRemoteAddress(r *http.Request) string {
	hdr := r.Header
	hdrRealIP := hdr.Get("x-real-ip")
	hdrForwardedFor := hdr.Get("x-forwarded-for")
	// Request.RemoteAddress contains port, which we want to remove i.e.:
	// "[::1]:58292" => "[::1]"
	ipAddrFromRemoteAddr := func(s string) string {
		idx := strings.LastIndex(s, ":")
		if idx == -1 {
			return s
		}
		return s[:idx]
	}
	if hdrRealIP == "" && hdrForwardedFor == "" {
		return ipAddrFromRemoteAddr(r.RemoteAddr)
	}
	if hdrForwardedFor != "" {
		// X-Forwarded-For is potentially a list of addresses separated with ","
		parts := strings.Split(hdrForwardedFor, ",")
		for i, p := range parts {
			parts[i] = strings.TrimSpace(p)
		}
		// TODO: should return first non-local address
		return parts[0]
	}
	return hdrRealIP
}

func hasSpacesCreds() bool {
	return os.Getenv("SPACES_KEY") != "" && os.Getenv("SPACES_SECRET") != ""
}

func newMinioSpacesClient() *MinioClient {
	bucket := "kjklogs"
	key := os.Getenv("SPACES_KEY")
	secret := os.Getenv("SPACES_SECRET")
	mc, err := minio.New("nyc3.digitaloceanspaces.com", &minio.Options{
		Creds:  credentials.NewStaticV4(key, secret, ""),
		Secure: true,
	})
	must(err)
	found, err := mc.BucketExists(ctx(), bucket)
	must(err)
	panicIf(!found, "bucket '%s' doesn't exist", bucket)
	return &MinioClient{
		c:      mc,
		bucket: bucket,
	}
}

func minioUploadFilePublic(mc *MinioClient, remotePath string, path string) error {
	contentType := mimeTypeFromFileName(remotePath)
	opts := minio.PutObjectOptions{
		ContentType: contentType,
	}
	minioSetPublicObjectMetadata(&opts)
	_, err := mc.c.FPutObject(ctx(), mc.bucket, remotePath, path, opts)
	return err
}

func minioSetPublicObjectMetadata(opts *minio.PutObjectOptions) {
	if opts.UserMetadata == nil {
		opts.UserMetadata = map[string]string{}
	}
	opts.UserMetadata["x-amz-acl"] = "public-read"
}

type MinioClient struct {
	c *minio.Client

	bucket string
}

func (c *MinioClient) URLBase() string {
	url := c.c.EndpointURL()
	return fmt.Sprintf("https://%s.%s/", c.bucket, url.Host)
}

// --------------------- utils

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func panicIf(cond bool, args ...interface{}) {
	if !cond {
		return
	}
	s := "condition failed"
	if len(args) > 0 {
		s = fmt.Sprintf("%s", args[0])
		if len(args) > 1 {
			s = fmt.Sprintf(s, args[1:]...)
		}
	}
	panic(s)
}

var mimeTypes = map[string]string{
	// not present in mime.TypeByExtension()
	".txt": "text/plain",
	".exe": "application/octet-stream",
}

func mimeTypeFromFileName(path string) string {
	ext := strings.ToLower(filepath.Ext(path))
	ct := mimeTypes[ext]
	if ct == "" {
		ct = mime.TypeByExtension(ext)
	}
	if ct == "" {
		// if all else fails
		ct = "application/octet-stream"
	}
	return ct
}

func ctx() context.Context {
	return context.Background()
}
