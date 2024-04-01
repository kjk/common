package u

import (
	"archive/zip"
	"bytes"
	"compress/bzip2"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/andybalholm/brotli"
	"github.com/klauspost/compress/zstd"
)

// implement io.ReadCloser over os.File wrapped with io.Reader.
// io.Closer goes to os.File, io.Reader goes to wrapping reader
type readerWrappedFile struct {
	f *os.File
	r io.Reader
}

func (rc *readerWrappedFile) Close() error {
	return rc.f.Close()
}

func (rc *readerWrappedFile) Read(p []byte) (int, error) {
	return rc.r.Read(p)
}

func wrapInReadeCloser(f *os.File, r io.Reader, err error) (io.ReadCloser, error) {
	if err != nil {
		f.Close()
		return nil, err
	}
	return &readerWrappedFile{
		f: f,
		r: r,
	}, nil
}

// OpenFileMaybeCompressed opens a file that might be compressed with gzip
// or bzip2 or zstd or brotli
// TODO: could sniff file content instead of checking file extension
func OpenFileMaybeCompressed(path string) (io.ReadCloser, error) {
	ext := strings.ToLower(filepath.Ext(path))
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	if ext == ".gz" {
		r, err := gzip.NewReader(f)
		return wrapInReadeCloser(f, r, err)
	}
	if ext == ".bz2" {
		r := bzip2.NewReader(f)
		return wrapInReadeCloser(f, r, err)
	}
	if ext == ".zstd" {
		r, err := zstd.NewReader(f)
		return wrapInReadeCloser(f, r, err)
	}
	if ext == ".br" {
		r := brotli.NewReader(f)
		return wrapInReadeCloser(f, r, err)
	}
	return f, nil
}

// ReadFileMaybeCompressed reads file. Ungzips if it's gzipped.
func ReadFileMaybeCompressed(path string) ([]byte, error) {
	r, err := OpenFileMaybeCompressed(path)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(r)
}

// WriteFileGzipped writes data to a path, using best gzip compression
func WriteFileGzipped(path string, data []byte) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	w, err := gzip.NewWriterLevel(f, gzip.BestCompression)
	if err != nil {
		return err
	}
	_, err = w.Write(data)
	if err != nil {
		f.Close()
		os.Remove(path)
		return err
	}
	err = w.Close()
	if err != nil {
		f.Close()
		os.Remove(path)
		return err
	}
	err = f.Close()
	if err != nil {
		os.Remove(path)
		return err
	}
	return nil
}

// GzipFile compresses srcPath with gzip and saves as dstPath
func GzipFile(dstPath, srcPath string) error {
	fSrc, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer fSrc.Close()
	fDst, err := os.Create(dstPath)
	if err != nil {
		return err
	}
	defer fDst.Close()
	w, err := gzip.NewWriterLevel(fDst, gzip.BestCompression)
	if err != nil {
		return err
	}
	_, err = io.Copy(w, fSrc)
	if err != nil {
		return err
	}
	return w.Close()
}

func ZipDir(dirToZip string) ([]byte, error) {
	var buf bytes.Buffer
	err := ZipDirToWriter(&buf, dirToZip)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func ZipDirToWriter(w io.Writer, dirToZip string) error {
	zw := zip.NewWriter(w)
	err := filepath.Walk(dirToZip, func(pathToZip string, info os.FileInfo, err error) error {
		if err != nil {
			//fmt.Printf("WalkFunc() received err %s from filepath.Wath()\n", err.Error())
			return err
		}
		//fmt.Printf("%s\n", path)
		isDir, err := PathIsDir(pathToZip)
		if err != nil {
			//fmt.Printf("PathIsDir() for %s failed with %s\n", pathToZip, err.Error())
			return err
		}
		if isDir {
			return nil
		}
		toZipReader, err := os.Open(pathToZip)
		if err != nil {
			//fmt.Printf("os.Open() %s failed with %s\n", pathToZip, err.Error())
			return err
		}
		defer toZipReader.Close()

		zipName := pathToZip[len(dirToZip)+1:] // +1 for '/' in the path
		// per zip.Writer.Create(), name must be slash-separated so on Windows
		// we have to convert backslashes to slashes
		zipName = filepath.ToSlash(zipName)
		inZipWriter, err := zw.Create(zipName)
		if err != nil {
			//fmt.Printf("Error in zipWriter(): %s\n", err.Error())
			return err
		}
		_, err = io.Copy(inZipWriter, toZipReader)
		if err != nil {
			return err
		}
		//fmt.Printf("Added %s to zip file\n", pathToZip)
		return nil
	})
	err2 := zw.Close()
	if err2 != nil {
		return err2
	}
	return err
}

// CreateZipWithDirContent creates a zip file with the content of a directory.
// The names of files inside the zip file are relatitve to dirToZip e.g.
// if dirToZip is foo and there is a file foo/bar.txt, the name in the zip
// will be bar.txt
func CreateZipWithDirContent(zipFilePath, dirToZip string) error {
	if isDir, err := PathIsDir(dirToZip); err != nil || !isDir {
		// TODO: should return an error if err == nil && !isDir
		return err
	}
	zf, err := os.Create(zipFilePath)
	if err != nil {
		//fmt.Printf("Failed to os.Create() %s, %s\n", zipFilePath, err.Error())
		return err
	}
	defer zf.Close()
	return ZipDirToWriter(zf, dirToZip)
}

func ReadZipFileMust(path string) map[string][]byte {
	r, err := zip.OpenReader(path)
	Must(err)
	defer CloseNoError(r)
	res := map[string][]byte{}
	for _, f := range r.File {
		rc, err := f.Open()
		Must(err)
		d, err := io.ReadAll(rc)
		Must(err)
		_ = rc.Close()
		res[f.Name] = d
	}
	return res
}

func zipAddFile(zw *zip.Writer, zipName string, path string) {
	zipName = filepath.ToSlash(zipName)
	d, err := os.ReadFile(path)
	Must(err)
	w, err := zw.Create(zipName)
	Must(err)
	_, err = w.Write(d)
	Must(err)
	fmt.Printf("  added %s from %s\n", zipName, path)
}

func zipDirRecur(zw *zip.Writer, baseDir string, dirToZip string) {
	dir := filepath.Join(baseDir, dirToZip)
	files, err := os.ReadDir(dir)
	Must(err)
	for _, fi := range files {
		if fi.IsDir() {
			zipDirRecur(zw, baseDir, filepath.Join(dirToZip, fi.Name()))
		} else if fi.Type().IsRegular() {
			zipName := filepath.Join(dirToZip, fi.Name())
			path := filepath.Join(baseDir, zipName)
			zipAddFile(zw, zipName, path)
		} else {
			path := filepath.Join(baseDir, fi.Name())
			s := fmt.Sprintf("%s is not a dir or regular file", path)
			panic(s)
		}
	}
}

// toZip is a list of files and directories in baseDir
// Directories are added recursively
func CreateZipFile(dst string, baseDir string, toZip ...string) {
	os.Remove(dst)
	PanicIf(len(toZip) == 0, "must provide toZip args")
	w, err := os.Create(dst)
	Must(err)
	defer CloseNoError(w)
	zw := zip.NewWriter(w)
	Must(err)
	for _, name := range toZip {
		path := filepath.Join(baseDir, name)
		fi, err := os.Stat(path)
		Must(err)
		if fi.IsDir() {
			zipDirRecur(zw, baseDir, name)
		} else if fi.Mode().IsRegular() {
			zipAddFile(zw, name, path)
		} else {
			s := fmt.Sprintf("%s is not a dir or regular file", path)
			panic(s)
		}
	}
	err = zw.Close()
	Must(err)
}

func UnzipDataToDir(zipData []byte, dir string) error {
	writeFile := func(f *zip.File, data []byte) error {
		// names in zip are unix-style, convert to windows-style
		name := filepath.FromSlash(f.Name)
		path := filepath.Join(dir, name)
		err := os.MkdirAll(filepath.Dir(path), 0755)
		if err != nil {
			return err
		}
		return os.WriteFile(path, data, 0644)
	}
	return IterZipData(zipData, writeFile)
}

func IterZipReader(r *zip.Reader, cb func(f *zip.File, data []byte) error) error {
	for _, f := range r.File {
		rc, err := f.Open()
		if err != nil {
			return err
		}
		d, err := io.ReadAll(rc)
		err2 := rc.Close()
		if err != nil {
			return err
		}
		if err2 != nil {
			return err2
		}
		err = cb(f, d)
		if err != nil {
			return err
		}
	}
	return nil
}

func IterZipData(zipData []byte, cb func(f *zip.File, data []byte) error) error {
	dr := bytes.NewReader(zipData)
	r, err := zip.NewReader(dr, int64(len(zipData)))
	if err != nil {
		return err
	}
	return IterZipReader(r, cb)
}

func ReadZipData(zipData []byte) (map[string][]byte, error) {
	res := map[string][]byte{}
	err := IterZipData(zipData, func(f *zip.File, data []byte) error {
		res[f.Name] = data
		return nil
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

func getErr(errs ...error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

func BrCompressData(d []byte, level int) ([]byte, error) {
	var dst bytes.Buffer
	w := brotli.NewWriterLevel(&dst, level)
	_, err := w.Write(d)
	err2 := w.Close()
	if err = getErr(err, err2); err != nil {
		return nil, err
	}
	return dst.Bytes(), nil
}

func BrCompressDataBest(d []byte) ([]byte, error) {
	return BrCompressData(d, brotli.BestCompression)
}

func BrCompressDataDefault(d []byte) ([]byte, error) {
	return BrCompressData(d, brotli.DefaultCompression)
}

func BrCompressFile(dstPath string, path string, level int) error {
	d, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	d2, err := BrCompressData(d, level)
	if err != nil {
		return err
	}
	return os.WriteFile(dstPath, d2, 0644)
}

func BrCompressFileDefault(dstPath string, path string) error {
	return BrCompressFile(dstPath, path, brotli.DefaultCompression)
}

func BrCompressFileBest(dstPath string, path string) error {
	return BrCompressFile(dstPath, path, brotli.BestCompression)
}

func zstdNewWriter(dst io.Writer) (*zstd.Encoder, error) {
	// in my tests:
	// - zstd.SpeedBestCompression is much slower and not much better
	// - default concurrency is GONUMPROCS() but adding concurrency of any value
	//   doesn't consistently speed things up
	return zstd.NewWriter(dst, zstd.WithEncoderLevel(zstd.SpeedBestCompression))
}

func ZstdCompressData(d []byte) ([]byte, error) {
	var dst bytes.Buffer
	w, err := zstdNewWriter(&dst)
	if err != nil {
		return nil, err
	}
	_, err = w.Write(d)
	if err != nil {
		return nil, err
	}
	err = w.Close()
	if err != nil {
		return nil, err
	}
	return dst.Bytes(), nil
}

func ZstdDecompressData(d []byte) ([]byte, error) {
	r := bytes.NewReader(d)
	zr, err := zstd.NewReader(r)
	if err != nil {
		return nil, err
	}
	defer zr.Close()
	return io.ReadAll(zr)
}

func ZstdCompressFile(dst string, src string) error {
	fSrc, err := os.Open(src)
	if err != nil {
		return err
	}
	defer fSrc.Close()
	fDst, err := os.Create(dst)
	if err != nil {
		return err
	}
	zw, err := zstdNewWriter(fDst)
	if err != nil {
		return err
	}
	_, err = io.Copy(zw, fSrc)
	err2 := zw.Close()
	err3 := fDst.Close()

	err = getErr(err, err2, err3)
	if err != nil {
		os.Remove(dst)
		return err
	}
	return nil
}

func ZstdReadFile(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	r, err := zstd.NewReader(f)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(r)
}
