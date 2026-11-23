A bunch of Go packages that I use in multiple projects.

An overview of packages:
* `u` : utility functions that I use all the time. Very short package name
is on purpose
* `atomicfile` : write to a file, atomically
* `filerotate` : implements a file you can write to and rotates on a schedule
(e.g. daily or hourly). I use it for log files
* `siser` : Simple Serialization format
* `server` : a very specific abstraction over http server that allows
me to write http server that is dynamic during dev (e.g. generates .html
files from templates) and can be turned into a fully static website easily
* `appendstore`, `appendstore2` : append-only storage with an index file and a data file
* `pak` : simple archive format (files + metadata in a single file)
* `httputil` : http helpers (timeout clients, serving files from `fs.FS`, simple static server)
* `httplogger` : logs http requests to a daily-rotated `siser` file
* `httplogproxy` : http(s) proxy that logs requests and responses
* `caching_http_client` : `http.RoundTripper` that caches responses
* `log` : logging to stdout and daily files, plus structured events
* `logtastic` : client for sending logs, events and errors to a logtastic server
* `siserlogger` : daily-rotated logger writing `siser` records
* `minioutil` : helpers for S3-compatible storage via minio client
* `deploy` : helpers for deploying a Go binary to a server over ssh / sftp
* `assert`, `require` : test assertions (trimmed down version of testify)
