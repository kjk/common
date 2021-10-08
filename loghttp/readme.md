`github.com/kjk/common/loghttp`logs http requests using `siser` to a file that rotates every hour (using `filerate`).

Optionally it can upload those hourly logs, compressed with brotli, to s3-compatible storage.

Then you can write code to analyze the logs.
