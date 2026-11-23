`github.com/kjk/common/httplogger` logs http requests using `siser` to a file that rotates every hour (using `filerotate`).

`New(dir, didRotateFn)` calls `didRotateFn` with the path of each completed hourly log. You can use it to e.g. compress the log with brotli and upload it to s3-compatible storage.

Then you can write code to analyze the logs.
