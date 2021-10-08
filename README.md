A bunch of Go packages that I use in multiple projects.

An overview of packages:
* `u` : utility functions that I use all the time. Very short package name
is on purpose
* `filerotate` : implements a file you can write to and rotates on a schedule
(e.g. daily or hourly). I use it for log files
* `server` : a very specific abstraction over http server that allows
me to write http server that is dynamic during dev (e.g. generates .html
files from templates) and can be turned into a fully static website easily
* `siser` : Simple Serialization format
