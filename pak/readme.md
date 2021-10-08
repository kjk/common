## What is `pak`

`pak` is a go library for creating an archive (like a `.zip` archive).

Use it when you need to bundle multiple files as a single file.

Why not just use a `.zip`?

The difference between a `.zip` and `pak` archive:

- `pak` supports arbitrary file metadata as key / value pairs
- `pak` doesn't compress by itself but you can use any compression type you like (record the compression type in metadata)
- `pak` archive header is at the beginning of the file and the format is optimized for random access to files

What are the limitations? It's meant for 'create once, read multiple times'. It doesn't support updating archives after creation.

## Create `pak` archive

```go
a := pak.NewWriter()
meta := pak.Metadata{}
// optional, add arbitrary meta-data about the file
meta.Set("Type", "text file")
// add a file "foo.txt" to the archive
err := a.AddFile("foo.txt", meta)
if err != nil {
    log.Fatal("failed to add a file\n")
}

// add data with name "bar.txt"
err := a.AddData([]byte("content of the file", "bar.txt", pak.Metadata{}))
if err != nil {
    log.Fatal("failed to add a file\n")
}
// ... add more files

// write creates an archive
err = a.WriteToFile("myarchive.pak")
if err != nil {
    log.Fatal("failed to write an archive\n")
}
// alternatively, write to any io.Writer:
// var wr io.Writer = ...
// err = w.Write(wr)
```

## Read `pak` archive

```go
archive, err := pak.ReadArchive("myarchive.pak")
if err != nil {
    log.Fatalf("failed to open an archive with '%s'\n", err)
}
defer f.Close()
entries := archive.Entries
fmt.Printf("Archive has %d entries\n", len(entries))
entry := entries[0]
d, err := archive.ReadEntry(entry)
if err != nil {
    log.Fatalf("archive.ReadEntry() failed with '%s'\n", err)
}
// alternatively, you can use entry.Offset and entry.Size to read the data

// print metadata entries
for _, m := entry.Metadata.Meta {
	fmt.Printf("metadata entry: key='%s', value='%s'\n", m.Key, m.Value)
}
```

## More on motivation

I wanted to package assets (.html files etc.) to be served via a web server into a single file.

Usually I would `.zip` file for that but `ZIP` is an old, quirky file format.

I wanted a simple and fast way to send a file out of the archive to the browser, already pre-compressed with a browser-compatible compression algorithm like brotli.

Zip doesn't natively support brotli compression. Not an unfixable problem (zip allows to store uncompressed files) but I took this opportunity to implement my own archive format which supports arbitrary per-file meta-data.

Building on top of my [siser](https://github.com/kjk/siser) library it's under 400 lines of code.

The format is very simple: the header at the beginning of the file contains a list of files with their mata-data and position in the archive.

The header is stored in [siser](https://github.com/kjk/siser) format.

The data for each file follows.

The archive is meant for batch creation. It doesn't support adding/removing/updating of files after the archive has been created.
