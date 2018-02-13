package apps

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/binary"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"

	"github.com/cozy/afero"
	"github.com/cozy/cozy-stack/pkg/cache"
	"github.com/cozy/cozy-stack/pkg/magic"
	"github.com/cozy/cozy-stack/pkg/utils"
	web_utils "github.com/cozy/cozy-stack/web/utils"
	"github.com/cozy/swift"
	"github.com/jinroh/immcache"
)

const gzipEncoding = "gzip"

// A caching layer is used to cache application assets in the local filesystem
// and avoid roundtrips with the origin asset server.
var diskCache = cache.Register(immcache.NewDiskCache(
	immcache.LRUIndex,
	immcache.DiskCacheOptions{
		BasePath:       os.TempDir(),
		BasePathPrefix: "cozycache",
		DiskSizeMax:    20 << (2 * 10), // 20MB,
	},
))

// FileServer interface defines a way to access and serve the application's
// data files.
type FileServer interface {
	Open(slug, version, file string) (io.ReadCloser, error)
	ServeFileContent(w http.ResponseWriter, req *http.Request,
		slug, version, file string) error
}

type swiftServer struct {
	c         *swift.Connection
	container string
}

type aferoServer struct {
	makePath func(slug, version, file string) string
	fs       afero.Fs
}

type gzipReadCloser struct {
	gr *gzip.Reader
	cl io.Closer
}

type assetMeta struct {
	Etag                  string
	ContentType           string
	ContentEncoding       string
	ContentLength         int64
	OriginalContentLength int64
	FileName              string
}

// NewSwiftFileServer returns provides the apps.FileServer implementation
// using the swift backend as file server.
func NewSwiftFileServer(conn *swift.Connection, appsType AppType) FileServer {
	return &swiftServer{conn, containerName(appsType)}
}

func (s *swiftServer) Open(slug, version, file string) (io.ReadCloser, error) {
	src, meta, err := s.open(slug, version, file)
	if err != nil {
		return nil, err
	}
	if meta.ContentEncoding == gzipEncoding {
		return newGzipReadCloser(src)
	}
	return src, nil
}

func (s *swiftServer) ServeFileContent(w http.ResponseWriter, req *http.Request, slug, version, file string) error {
	src, meta, err := s.open(slug, version, file)
	if err != nil {
		return err
	}
	return serveFileContent(w, req, src, meta)
}

func (s *swiftServer) open(slug, version, file string) (src io.ReadCloser, meta *assetMeta, err error) {
	key := s.container + path.Join(slug, version, file)
	src, err = diskCache.GetOrLoad(key, s)
	if err != nil {
		err = wrapSwiftErr(err)
		return
	}
	return decodeMeta(src)
}

// Load implements the immcache.Loader interface, to use swiftServer as a
// Loader with the cache.GetOrLoad method.
func (s *swiftServer) Load(key string) (size int64, src io.ReadCloser, err error) {
	objName := strings.TrimPrefix(key, s.container)
	f, h, err := s.c.ObjectOpen(s.container, objName, false, nil)
	if err != nil {
		return
	}

	size, err = f.Length()
	if err != nil {
		return
	}

	o := h.ObjectMetadata()
	contentLength, _ := strconv.ParseInt(h["Content-Length"], 10, 64)
	originalContentLength, _ := strconv.ParseInt(o["original-content-length"], 10, 64)
	meta := &assetMeta{
		Etag:                  fmt.Sprintf(`"%s"`, h["Etag"]),
		ContentType:           h["Content-Type"],
		ContentEncoding:       o["content-encoding"],
		ContentLength:         contentLength,
		OriginalContentLength: originalContentLength,
		FileName:              objName,
	}

	return prependMeta(src, size, meta)
}

// NewAferoFileServer returns a simple wrapper of the afero.Fs interface that
// provides the apps.FileServer interface.
//
// You can provide a makePath method to define how the file name should be
// created from the application's slug, version and file name. If not provided,
// the standard VFS concatenation (starting with vfs.WebappsDirName) is used.
func NewAferoFileServer(fs afero.Fs, makePath func(slug, version, file string) string) FileServer {
	if makePath == nil {
		makePath = defaultMakePath
	}
	return &aferoServer{makePath, fs}
}

func (s *aferoServer) Open(slug, version, file string) (io.ReadCloser, error) {
	src, meta, err := s.open(slug, version, file)
	if err != nil {
		return nil, err
	}
	if meta.ContentEncoding == gzipEncoding {
		return newGzipReadCloser(src)
	}
	return src, err
}

func (s *aferoServer) ServeFileContent(w http.ResponseWriter, req *http.Request, slug, version, file string) error {
	src, meta, err := s.open(slug, version, file)
	if err != nil {
		return err
	}
	return serveFileContent(w, req, src, meta)
}

func (s *aferoServer) open(slug, version, file string) (src io.ReadCloser, meta *assetMeta, err error) {
	key := s.makePath(slug, version, file)
	src, err = diskCache.GetOrLoad(key, s)
	if err == nil {
		return decodeMeta(src)
	}
	return
}

func (s *aferoServer) Load(key string) (size int64, src io.ReadCloser, err error) {
	isGzipped := true
	src, err = s.fs.Open(key + ".gz")
	if os.IsNotExist(err) {
		isGzipped = false
		src, err = s.fs.Open(key)
	}
	if err != nil {
		return
	}

	var infos os.FileInfo
	if isGzipped {
		infos, err = s.fs.Stat(key + ".gz")
	} else {
		infos, err = s.fs.Stat(key)
	}
	if err != nil {
		return
	}

	h := sha256.New()
	buf, err := ioutil.ReadAll(io.TeeReader(src, h))
	if errc := src.Close(); err == nil {
		err = errc
	}
	if err != nil {
		return
	}

	src = ioutil.NopCloser(bytes.NewReader(buf))
	etag := fmt.Sprintf(`"%s"`, hex.EncodeToString(h.Sum(nil)[:8]))
	originalSize := int64(len(buf))

	contentType := magic.MIMETypeByExtension(path.Ext(key))
	if contentType == "text/html" {
		contentType = "text/html; charset=utf-8"
	}

	contentEncoding := ""
	if isGzipped {
		contentEncoding = gzipEncoding
	}

	size = infos.Size()
	meta := &assetMeta{
		Etag:                  etag,
		ContentType:           contentType,
		ContentEncoding:       contentEncoding,
		ContentLength:         size,
		OriginalContentLength: originalSize,
		FileName:              key,
	}

	return prependMeta(src, size, meta)
}

func serveFileContent(w http.ResponseWriter, req *http.Request, src io.ReadCloser, meta *assetMeta) (err error) {
	defer func() {
		if src != nil {
			if errc := src.Close(); err == nil {
				err = errc
			}
		}
	}()

	if checkEtag := req.Header.Get("Cache-Control") == ""; checkEtag {
		if web_utils.CheckPreconditions(w, req, meta.Etag) {
			return
		}
		w.Header().Set("Etag", meta.Etag)
	}

	contentLength := meta.ContentLength
	if meta.ContentEncoding == gzipEncoding {
		if acceptGzipEncoding(req) {
			w.Header().Set("Content-Encoding", gzipEncoding)
		} else {
			contentLength = meta.OriginalContentLength
			src, err = newGzipReadCloser(src)
		}
		if err != nil {
			return
		}
	}

	contentType := meta.ContentType
	if contentType == "" {
		contentType = magic.MIMETypeByExtension(path.Ext(meta.FileName))
	}
	if contentType == "text/html" {
		contentType = "text/html; charset=utf-8"
	} else if contentType == "text/xml" && path.Ext(meta.FileName) == ".svg" {
		// override for files with text/xml content because of leading <?xml tag
		contentType = "image/svg+xml"
	}

	web_utils.ServeContent(w, req, contentType, contentLength, src)
	return nil
}

func defaultMakePath(slug, version, file string) string {
	basepath := path.Join("/", slug, version)
	filepath := path.Join("/", path.Clean(file))
	return path.Join(basepath, filepath)
}

func acceptGzipEncoding(req *http.Request) bool {
	return strings.Contains(req.Header.Get("Accept-Encoding"), gzipEncoding)
}

func containerName(appsType AppType) string {
	switch appsType {
	case Webapp:
		return "apps-web"
	case Konnector:
		return "apps-konnectors"
	}
	panic("Unknown AppType")
}

func wrapSwiftErr(err error) error {
	if err == swift.ObjectNotFound || err == swift.ContainerNotFound {
		return os.ErrNotExist
	}
	return err
}

// The Close method of gzip.Reader does not closes the underlying reader. This
// little wrapper does the closing.
func newGzipReadCloser(r io.ReadCloser) (io.ReadCloser, error) {
	gr, err := gzip.NewReader(r)
	if err != nil {
		r.Close()
		return nil, err
	}
	return gzipReadCloser{gr: gr, cl: r}, nil
}

func (g gzipReadCloser) Read(b []byte) (int, error) {
	return g.gr.Read(b)
}

func (g gzipReadCloser) Close() error {
	err1 := g.gr.Close()
	err2 := g.cl.Close()
	if err1 != nil {
		return err1
	}
	if err2 != nil {
		return err2
	}
	return nil
}

func prependMeta(in io.ReadCloser, size int64, meta *assetMeta) (int64, io.ReadCloser, error) {
	var b bytes.Buffer
	headerLenBuf := make([]byte, 4)
	b.Write(headerLenBuf)
	if err := gob.NewEncoder(&b).Encode(meta); err != nil {
		return 0, nil, err
	}
	headerLen := b.Len()
	binary.BigEndian.PutUint32(b.Bytes()[:len(headerLenBuf)], uint32(headerLen))
	out := utils.ReadCloser(io.MultiReader(&b, in), in.Close)
	return size + int64(b.Len()), out, nil
}

func decodeMeta(in io.ReadCloser) (out io.ReadCloser, meta *assetMeta, err error) {
	headerLenBuf := make([]byte, 4)
	if _, err = io.ReadFull(in, headerLenBuf); err == nil {
		headerLen := int64(binary.BigEndian.Uint32(headerLenBuf))
		err = gob.NewDecoder(io.LimitReader(in, headerLen-4)).Decode(&meta)
	}
	if err == nil {
		out = in
	}
	return
}
