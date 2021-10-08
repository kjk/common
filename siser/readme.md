# siser

Package `siser` is a Simple Serialization library for Go

Imagine you want to write many records of somewhat structured data
to a file. Think of it as structured logging.

You could use csv format, but csv values are identified by a position,
not name. They are also hard to read.

You could serialize as json and write one line per json record but
json isn't great for human readability (imagine you `tail -f` a log
file with json records).

This library is meant to be a middle ground:
* you can serialize arbitrary records with multiple key/value pairs
* the output is human-readable
* it's designed to be efficient and simple to use

## API usage

Imagine you want log basic info about http requests.

```go
func createWriter() (*siser.Writer, error) {
	f, err := os.Create("http_access.log")
	if err != nil {
		return nil, err
	}
	w := siser.NewWriter(f)
	return w, nil
}

func logHTTPRequest(w *siser.Writer, url string, ipAddr string, statusCode int) error {
	var rec siser.Record
	// you can append multiple key/value pairs at once
	rec.Write("url", url, "ipaddr", ipAddr)
	// or assemble with multiple calls
	rec.Writes("code", strconv.Itoa(statusCode))
	_, err := w.WriteRecord(&rec)
	return err
}
```

The data will be written to writer underlying `siser.Writer` as:
```
61 1553488435903 httplog
url: https://blog.kowalczyk.info
ipaddr: 10.0.0.1
code: 200
```

Here's what and why:
* `61` is the size of the data. This allows us to read the exact number of bytes in the record
* `1553488435903` is a timestamp which is Unix epoch time in milliseconds (more precision than standard Unix time which is in seconds)
* `httplog` is optional name of the record. This allows you to easily write multiple types of records to a file

To read all records from the file:
```go
f, err := os.Open("http_access.log")
fatalIfErr(err)
defer f.Close()
reader := siser.NewReader(f)
for reader.ReadNextRecord() {
	rec := r.Record
	name := rec.Name // "httplog"
	timestamp := rec.Timestamp
	code, ok := rec.Get("code")
	// get rest of values and and do something with them
}
fatalIfErr(rec.Err())
```

## Usage scenarios

I use `siser` for in my web services for 2 use cases:

* logging to help in debugging issues after they happen
* implementing poor-man's analytics

Logging for debugging adds a little bit more structure over
ad hoc logging. I can add some meta-data to log entries
and in addition to reading the logs I can quickly write
programs that filter the logs. For example if I add serving time
to http request log I could easily write a program that shows
requests that take over 1 second to serve.

Another one is poor-man's analytics. For example, if you're building
a web service that converts .png file to .ico file, it would be
good to know daily statistics about how many files were converted,
how much time an average conversion takes etc.

## Performance and implementation notes

Some implementation decisions were made with performance in mind.

Given key/value nature of the record, an easy choice would be to use map[string]string as source to encode/decode functions.

However `[]string` is more efficient than a `map`. Additionally, a slice can be reused across multiple records. We can clear it by setting the size to zero and reuse the underlying array. A map would require allocating a new instance for each record, which would create a lot of work for garbage collector.

When serializing, you need to use `Reset` method to get the benefit of efficient re-use of the `Record`.

When reading and deserializing records, `siser.Reader` uses this optimization internally.

The format avoids the need for escaping keys and values, which helps in making encoding/decoding fast.

How does that play out in real life? I wrote a benchmark comparing siser vs. json.Marshal. It’s about 30% faster:

```
$ go test -bench=.
BenchmarkSiserMarshal-8   	 1000000	      1903 ns/op
BenchmarkJSONMarshal-8    	  500000	      2905 ns/op
```

The format is binary-safe and works for serializing large values e.g. you can use png image as value.

It’s also very easy to implement in any language.
