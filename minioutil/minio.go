package minioutil

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"os"
	"path"
	"path/filepath"
	"github.com/kjk/common/atomicfile"
	"github.com/kjk/common/u"
	"strings"

	"github.com/andybalholm/brotli"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type Config struct {
	Access       string
	Secret       string
	Bucket       string
	Endpoint     string
	Region       string
	RequestTrace io.Writer
}

type Client struct {
	Client *minio.Client
	config *Config
	Bucket string
}

func New(config *Config) (*Client, error) {
	if config == nil {
		return nil, errors.New("must provide config")
	}
	c := config
	if c.Access == "" || c.Secret == "" || c.Bucket == "" || c.Endpoint == "" {
		return nil, errors.New("must provide all fields in config")
	}

	mc, err := minio.New(c.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(c.Access, c.Secret, ""),
		Region: config.Region,
		Secure: true,
	})
	if err != nil {
		return nil, err
	}
	if config.RequestTrace != nil {
		mc.TraceOn(config.RequestTrace)
	}
	found, err := mc.BucketExists(ctx(), c.Bucket)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("bucket '%s' doesn't existt", c.Bucket)
	}

	return &Client{
		Client: mc,
		config: config,
		Bucket: config.Bucket,
	}, nil
}

func (c *Client) URLBase() string {
	url := c.Client.EndpointURL()
	return fmt.Sprintf("https://%s.%s/", c.Bucket, url.Host)
}

func (c *Client) URLForPath(remotePath string) string {
	return c.URLBase() + strings.TrimPrefix(remotePath, "/")
}

func (c *Client) Exists(remotePath string) bool {
	_, err := c.Client.StatObject(ctx(), c.Bucket, remotePath, minio.StatObjectOptions{})
	return err == nil
}

func (c *Client) Copy(oldPath, newPath string) (*minio.UploadInfo, error) {
	_, err := c.Client.StatObject(ctx(), c.Bucket, newPath, minio.StatObjectOptions{})
	if err == nil {
		return nil, os.ErrExist
	}
	dstOpts := minio.CopyDestOptions{
		Bucket: c.Bucket,
		Object: newPath,
	}
	srcOpts := minio.CopySrcOptions{
		Bucket: c.Bucket,
		Object: oldPath,
	}
	ui, err := c.Client.CopyObject(ctx(), dstOpts, srcOpts)
	return &ui, err
}

func (c *Client) Rename(oldPath, newPath string) (*minio.UploadInfo, error) {
	_, err := c.Client.StatObject(ctx(), c.Bucket, newPath, minio.StatObjectOptions{})
	if err == nil {
		return nil, errors.New("destination already exists")
	}
	dstOpts := minio.CopyDestOptions{
		Bucket: c.Bucket,
		Object: newPath,
	}
	srcOpts := minio.CopySrcOptions{
		Bucket: c.Bucket,
		Object: oldPath,
	}
	ui, err := c.Client.CopyObject(ctx(), dstOpts, srcOpts)
	if err != nil {
		return nil, err
	}
	err = c.Client.RemoveObject(ctx(), c.Bucket, oldPath, minio.RemoveObjectOptions{})
	if err != nil {
		_ = c.Client.RemoveObject(ctx(), c.Bucket, newPath, minio.RemoveObjectOptions{})
		return nil, err
	}
	return &ui, nil
}

func (c *Client) DownloadFileAtomically(dstPath string, remotePath string) error {
	opts := minio.GetObjectOptions{}
	obj, err := c.Client.GetObject(ctx(), c.Bucket, remotePath, opts)
	if err != nil {
		return err
	}
	defer obj.Close()

	// ensure there's a dir for destination file
	dir := filepath.Dir(dstPath)
	err = os.MkdirAll(dir, 0755)
	if err != nil {
		return err
	}

	f, err := atomicfile.New(dstPath)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(f, obj)
	if err != nil {
		return err
	}
	return f.Close()
}

func (c *Client) UploadFile(remotePath string, path string, public bool) (info minio.UploadInfo, err error) {
	ext := filepath.Ext(remotePath)
	contentType := mime.TypeByExtension(ext)
	opts := minio.PutObjectOptions{
		ContentType: contentType,
	}
	if public {
		setPublicObjectMetadata(&opts)
	}
	return c.Client.FPutObject(ctx(), c.Bucket, remotePath, path, opts)
}

func (c *Client) UploadData(remotePath string, data []byte, public bool) (info minio.UploadInfo, err error) {
	contentType := u.MimeTypeFromFileName(remotePath)
	opts := minio.PutObjectOptions{
		ContentType: contentType,
	}
	if public {
		setPublicObjectMetadata(&opts)
	}
	r := bytes.NewBuffer(data)
	return c.Client.PutObject(ctx(), c.Bucket, remotePath, r, int64(len(data)), opts)
}

func (c *Client) UploadDir(dirRemote string, dirLocal string, public bool) error {
	files, err := os.ReadDir(dirLocal)
	if err != nil {
		return err
	}
	for _, f := range files {
		fname := f.Name()
		pathLocal := filepath.Join(dirLocal, fname)
		pathRemote := path.Join(dirRemote, fname)
		_, err := c.UploadFile(pathRemote, pathLocal, public)
		if err != nil {
			return fmt.Errorf("upload of '%s' as '%s' failed with '%s'", pathLocal, pathRemote, err)
		}
	}
	return nil
}

func (c *Client) ListObjects(prefix string) <-chan minio.ObjectInfo {
	opts := minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: true,
	}
	return c.Client.ListObjects(ctx(), c.Bucket, opts)
}

func (c *Client) Remove(remotePath string) error {
	opts := minio.RemoveObjectOptions{}
	err := c.Client.RemoveObject(ctx(), c.Bucket, remotePath, opts)
	return err
}

func brotliCompress(path string) ([]byte, error) {
	var buf bytes.Buffer
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	w := brotli.NewWriterLevel(&buf, brotli.BestCompression)
	_, err = io.Copy(w, f)
	if err != nil {
		return nil, err
	}
	err = w.Close()
	if err != nil {
		return nil, err
	}
	err = f.Close()
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (c *Client) UploadFileBrotliCompressed(remotePath string, path string, public bool) (info minio.UploadInfo, err error) {
	// TODO: use io.Pipe() to do compression more efficiently
	d, err := brotliCompress(path)
	if err != nil {
		return
	}
	ext := filepath.Ext(remotePath)
	contentType := mime.TypeByExtension(ext)
	opts := minio.PutObjectOptions{
		ContentType: contentType,
	}
	if public {
		setPublicObjectMetadata(&opts)
	}
	r := bytes.NewReader(d)
	fsize := int64(len(d))
	return c.Client.PutObject(ctx(), c.Bucket, remotePath, r, fsize, opts)
}

func ctx() context.Context {
	return context.Background()
}

func setPublicObjectMetadata(opts *minio.PutObjectOptions) {
	if opts.UserMetadata == nil {
		opts.UserMetadata = map[string]string{}
	}
	opts.UserMetadata["x-amz-acl"] = "public-read"
}
