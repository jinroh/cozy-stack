package vfsminio

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cozy/cozy-stack/pkg/config"
	"github.com/cozy/cozy-stack/pkg/consts"
	"github.com/cozy/cozy-stack/pkg/couchdb"
	"github.com/cozy/cozy-stack/pkg/lock"
	"github.com/cozy/cozy-stack/pkg/logger"
	"github.com/cozy/cozy-stack/pkg/utils"
	"github.com/cozy/cozy-stack/pkg/vfs"
	multierror "github.com/hashicorp/go-multierror"
	"github.com/minio/minio-go"
	"github.com/sirupsen/logrus"
)

const maxFileSize = 5 << (3 * 10) // 5 GiB

type minioVFS struct {
	vfs.Indexer
	vfs.DiskThresholder
	c          *minio.Client
	location   string
	bucket     string
	dataBucket string
	mu         lock.ErrorRWLocker
	log        *logrus.Entry
}

const (
	bucketPrefixCozy = "cozy-"
	bucketPrefixData = "data-"
)

func New(index vfs.Indexer, disk vfs.DiskThresholder, mu lock.ErrorRWLocker, domain string) (vfs.VFS, error) {
	if domain == "" {
		return nil, fmt.Errorf("vfsminio: specified domain is empty")
	}
	return &minioVFS{
		Indexer:         index,
		DiskThresholder: disk,

		c:          config.GetMinioConnection(),
		location:   "", // XXX
		bucket:     bucketPrefixCozy + domain,
		dataBucket: bucketPrefixData + domain,
		mu:         mu,
		log:        logger.WithDomain(domain),
	}, nil
}

// MakeObjectName build the swift object name for a given file document. It
// creates a virtual subfolder by splitting the document ID, which should be 32
// bytes long, on the 27nth byte. This avoid having a flat hierarchy.
func MakeObjectName(docID string) string {
	return docID
}

func makeDocID(objName string) string {
	return objName
}

func (mfs *minioVFS) InitFs() error {
	if lockerr := mfs.mu.Lock(); lockerr != nil {
		return lockerr
	}
	defer mfs.mu.Unlock()
	if err := mfs.Indexer.InitIndex(); err != nil {
		return err
	}
	if err := mfs.c.MakeBucket(mfs.bucket, mfs.location); err != nil {
		mfs.log.Errorf("[vfsminio] Could not create bucket %s: %s",
			mfs.bucket, err.Error())
		return err
	}
	if err := mfs.c.MakeBucket(mfs.dataBucket, mfs.location); err != nil {
		mfs.log.Errorf("[vfsminio] Could not create bucket %s: %s",
			mfs.dataBucket, err.Error())
		return err
	}
	mfs.log.Infof("[vfsminio] Created bucket %s", mfs.bucket)
	return nil
}

func (mfs *minioVFS) Delete() error {
	// XXX
	return nil
}

func (mfs *minioVFS) CreateDir(doc *vfs.DirDoc) error {
	if lockerr := mfs.mu.Lock(); lockerr != nil {
		return lockerr
	}
	defer mfs.mu.Unlock()
	exists, err := mfs.Indexer.DirChildExists(doc.DirID, doc.DocName)
	if err != nil {
		return err
	}
	if exists {
		return os.ErrExist
	}
	if doc.ID() == "" {
		return mfs.Indexer.CreateDirDoc(doc)
	}
	return mfs.Indexer.CreateNamedDirDoc(doc)
}

func (mfs *minioVFS) CreateFile(newdoc, olddoc *vfs.FileDoc) (vfs.File, error) {
	if lockerr := mfs.mu.Lock(); lockerr != nil {
		return nil, lockerr
	}
	defer mfs.mu.Unlock()

	diskQuota := mfs.DiskQuota()

	var maxsize, newsize, oldsize int64
	newsize = newdoc.ByteSize
	if diskQuota > 0 {
		diskUsage, err := mfs.DiskUsage()
		if err != nil {
			return nil, err
		}
		if olddoc != nil {
			oldsize = olddoc.Size()
		}
		maxsize = diskQuota - diskUsage
		if maxsize > maxFileSize {
			maxsize = maxFileSize
		}
	} else {
		maxsize = maxFileSize
	}
	if maxsize <= 0 || (newsize >= 0 && (newsize-oldsize) > maxsize) {
		return nil, vfs.ErrFileTooBig
	}

	if olddoc != nil {
		newdoc.SetID(olddoc.ID())
		newdoc.SetRev(olddoc.Rev())
		newdoc.CreatedAt = olddoc.CreatedAt
	}

	newpath, err := mfs.Indexer.FilePath(newdoc)
	if err != nil {
		return nil, err
	}
	if strings.HasPrefix(newpath, vfs.TrashDirName+"/") {
		return nil, vfs.ErrParentInTrash
	}

	// Avoid storing negative size in the index.
	if newdoc.ByteSize < 0 {
		newdoc.ByteSize = 0
	}

	if olddoc == nil {
		var exists bool
		exists, err = mfs.Indexer.DirChildExists(newdoc.DirID, newdoc.DocName)
		if err != nil {
			return nil, err
		}
		if exists {
			return nil, os.ErrExist
		}

		// When added to the index, the document is first considered hidden. This
		// flag will only be removed at the end of the upload when all its metadata
		// are known. See the Close() method.
		newdoc.Trashed = true

		if newdoc.ID() == "" {
			err = mfs.Indexer.CreateFileDoc(newdoc)
		} else {
			err = mfs.Indexer.CreateNamedFileDoc(newdoc)
		}
		if err != nil {
			return nil, err
		}
	}

	objName := MakeObjectName(newdoc.DocID)

	pr, pw := io.Pipe()

	go func() {
		objMeta := map[string]string{
			"creation-name": newdoc.Name(),
			"created-at":    newdoc.CreatedAt.Format(time.RFC3339),
			"exec":          strconv.FormatBool(newdoc.Executable),
		}
		objOpts := minio.PutObjectOptions{
			ContentType:  newdoc.Mime,
			UserMetadata: objMeta,
		}
		_, errp := mfs.c.PutObject(mfs.bucket, objName, pr, newsize, objOpts)
		if errp != nil {
			pw.CloseWithError(errp)
		}
	}()

	return &minioFileCreation{
		pw:      pw,
		fs:      mfs,
		w:       0,
		size:    newsize,
		name:    objName,
		meta:    vfs.NewMetaExtractor(newdoc),
		newdoc:  newdoc,
		olddoc:  olddoc,
		maxsize: maxsize,
	}, nil
}

func (mfs *minioVFS) DestroyDirContent(doc *vfs.DirDoc) error {
	if lockerr := mfs.mu.Lock(); lockerr != nil {
		return lockerr
	}
	defer mfs.mu.Unlock()
	return mfs.destroyDirContent(doc)
}

func (mfs *minioVFS) DestroyDirAndContent(doc *vfs.DirDoc) error {
	if lockerr := mfs.mu.Lock(); lockerr != nil {
		return lockerr
	}
	defer mfs.mu.Unlock()
	return mfs.destroyDirAndContent(doc)
}

func (mfs *minioVFS) DestroyFile(doc *vfs.FileDoc) error {
	if lockerr := mfs.mu.Lock(); lockerr != nil {
		return lockerr
	}
	defer mfs.mu.Unlock()
	return mfs.destroyFile(doc)
}

func (mfs *minioVFS) destroyDirContent(doc *vfs.DirDoc) (err error) {
	iter := mfs.DirIterator(doc, nil)
	for {
		d, f, erri := iter.Next()
		if erri == vfs.ErrIteratorDone {
			return
		}
		if erri != nil {
			return erri
		}
		var errd error
		if d != nil {
			errd = mfs.destroyDirAndContent(d)
		} else {
			errd = mfs.destroyFile(f)
		}
		if errd != nil {
			err = multierror.Append(err, errd)
		}
	}
}

func (mfs *minioVFS) destroyDirAndContent(doc *vfs.DirDoc) error {
	if err := mfs.destroyDirContent(doc); err != nil {
		return err
	}
	return mfs.Indexer.DeleteDirDoc(doc)
}

func (mfs *minioVFS) destroyFile(doc *vfs.FileDoc) error {
	return mfs.Indexer.DeleteFileDoc(doc)
}

func (mfs *minioVFS) OpenFile(doc *vfs.FileDoc) (vfs.File, error) {
	if lockerr := mfs.mu.RLock(); lockerr != nil {
		return nil, lockerr
	}
	defer mfs.mu.RUnlock()
	objName := MakeObjectName(doc.DocID)
	f, err := mfs.c.GetObject(mfs.bucket, objName, minio.GetObjectOptions{})
	if err != nil {
		errResponse := minio.ToErrorResponse(err)
		if errResponse.StatusCode == http.StatusNotFound {
			return nil, os.ErrNotExist
		}
		return nil, err
	}
	return minioFileOpen{f}, nil
}

type fsckFile struct {
	file     *vfs.FileDoc
	fullpath string
}

func (mfs *minioVFS) Fsck() ([]*vfs.FsckLog, error) {
	if lockerr := mfs.mu.RLock(); lockerr != nil {
		return nil, lockerr
	}
	defer mfs.mu.RUnlock()

	root, err := mfs.Indexer.DirByID(consts.RootDirID)
	if err != nil {
		return nil, err
	}

	entries := make(map[string]fsckFile, 256)
	err = mfs.fsckWalk(root, entries)
	if err != nil {
		return nil, err
	}

	var logbook []*vfs.FsckLog

	doneCh := make(chan struct{})

	defer close(doneCh)

	objectCh := mfs.c.ListObjects(mfs.bucket, "", false, doneCh)
	for obj := range objectCh {
		if err = obj.Err; err != nil {
			return nil, err
		}
		docID := makeDocID(obj.Key)
		f, ok := entries[docID]
		if !ok {
			var fileDoc *vfs.FileDoc
			var filePath string
			filePath, fileDoc, err = objectToFileDocV2(obj)
			if err != nil {
				return nil, err
			}
			logbook = append(logbook, &vfs.FsckLog{
				Type:     vfs.IndexMissing,
				IsFile:   true,
				FileDoc:  fileDoc,
				Filename: filePath,
			})
		} else {
			var md5sum []byte
			md5sum, err = hex.DecodeString(obj.ETag)
			if err != nil {
				return nil, err
			}
			if !bytes.Equal(md5sum, f.file.MD5Sum) {
				olddoc := f.file
				newdoc := olddoc.Clone().(*vfs.FileDoc)
				newdoc.MD5Sum = md5sum
				logbook = append(logbook, &vfs.FsckLog{
					Type:       vfs.ContentMismatch,
					IsFile:     true,
					FileDoc:    newdoc,
					OldFileDoc: olddoc,
					Filename:   f.fullpath,
				})
			}
			delete(entries, docID)
		}
	}

	// entries should contain only data that does not contain an associated
	// index.
	for docID, f := range entries {
		_, err = mfs.c.StatObject(mfs.bucket, docID, minio.StatObjectOptions{})
		if err != nil {
			errResp := minio.ToErrorResponse(err)
			if errResp.StatusCode == http.StatusNotFound {
				logbook = append(logbook, &vfs.FsckLog{
					Type:     vfs.FileMissing,
					IsFile:   true,
					FileDoc:  f.file,
					Filename: f.fullpath,
				})
			} else if err != nil {
				return nil, err
			}
		}
	}
	sort.Slice(logbook, func(i, j int) bool {
		return logbook[i].Filename < logbook[j].Filename
	})

	return logbook, nil
}

func (mfs *minioVFS) fsckWalk(dir *vfs.DirDoc, entries map[string]fsckFile) error {
	iter := mfs.Indexer.DirIterator(dir, nil)
	for {
		d, f, err := iter.Next()
		if err == vfs.ErrIteratorDone {
			break
		}
		if err != nil {
			return err
		}
		if f != nil {
			fullpath := path.Join(dir.Fullpath, f.DocName)
			entries[f.DocID] = fsckFile{f, fullpath}
		} else if err = mfs.fsckWalk(d, entries); err != nil {
			return err
		}
	}
	return nil
}

// FsckPrune tries to fix the given list on inconsistencies in the VFS
func (mfs *minioVFS) FsckPrune(logbook []*vfs.FsckLog, dryrun bool) {
	for _, entry := range logbook {
		vfs.FsckPrune(mfs, mfs.Indexer, entry, dryrun)
	}
}

// UpdateFileDoc calls the indexer UpdateFileDoc function and adds a few checks
// before actually calling this method:
//   - locks the filesystem for writing
//   - checks in case we have a move operation that the new path is available
//
// @override Indexer.UpdateFileDoc
func (mfs *minioVFS) UpdateFileDoc(olddoc, newdoc *vfs.FileDoc) error {
	if lockerr := mfs.mu.Lock(); lockerr != nil {
		return lockerr
	}
	defer mfs.mu.Unlock()
	if newdoc.DirID != olddoc.DirID || newdoc.DocName != olddoc.DocName {
		exists, err := mfs.Indexer.DirChildExists(newdoc.DirID, newdoc.DocName)
		if err != nil {
			return err
		}
		if exists {
			return os.ErrExist
		}
	}
	return mfs.Indexer.UpdateFileDoc(olddoc, newdoc)
}

// UdpdateDirDoc calls the indexer UdpdateDirDoc function and adds a few checks
// before actually calling this method:
//   - locks the filesystem for writing
//   - checks in case we have a move operation that the new path is available
//
// @override Indexer.UpdateDirDoc
func (mfs *minioVFS) UpdateDirDoc(olddoc, newdoc *vfs.DirDoc) error {
	if lockerr := mfs.mu.Lock(); lockerr != nil {
		return lockerr
	}
	defer mfs.mu.Unlock()
	if newdoc.DirID != olddoc.DirID || newdoc.DocName != olddoc.DocName {
		exists, err := mfs.Indexer.DirChildExists(newdoc.DirID, newdoc.DocName)
		if err != nil {
			return err
		}
		if exists {
			return os.ErrExist
		}
	}
	return mfs.Indexer.UpdateDirDoc(olddoc, newdoc)
}

func (mfs *minioVFS) DirByID(fileID string) (*vfs.DirDoc, error) {
	if lockerr := mfs.mu.RLock(); lockerr != nil {
		return nil, lockerr
	}
	defer mfs.mu.RUnlock()
	return mfs.Indexer.DirByID(fileID)
}

func (mfs *minioVFS) DirByPath(name string) (*vfs.DirDoc, error) {
	if lockerr := mfs.mu.RLock(); lockerr != nil {
		return nil, lockerr
	}
	defer mfs.mu.RUnlock()
	return mfs.Indexer.DirByPath(name)
}

func (mfs *minioVFS) FileByID(fileID string) (*vfs.FileDoc, error) {
	if lockerr := mfs.mu.RLock(); lockerr != nil {
		return nil, lockerr
	}
	defer mfs.mu.RUnlock()
	return mfs.Indexer.FileByID(fileID)
}

func (mfs *minioVFS) FileByPath(name string) (*vfs.FileDoc, error) {
	if lockerr := mfs.mu.RLock(); lockerr != nil {
		return nil, lockerr
	}
	defer mfs.mu.RUnlock()
	return mfs.Indexer.FileByPath(name)
}

func (mfs *minioVFS) FilePath(doc *vfs.FileDoc) (string, error) {
	if lockerr := mfs.mu.RLock(); lockerr != nil {
		return "", lockerr
	}
	defer mfs.mu.RUnlock()
	return mfs.Indexer.FilePath(doc)
}

func (mfs *minioVFS) DirOrFileByID(fileID string) (*vfs.DirDoc, *vfs.FileDoc, error) {
	if lockerr := mfs.mu.RLock(); lockerr != nil {
		return nil, nil, lockerr
	}
	defer mfs.mu.RUnlock()
	return mfs.Indexer.DirOrFileByID(fileID)
}

func (mfs *minioVFS) DirOrFileByPath(name string) (*vfs.DirDoc, *vfs.FileDoc, error) {
	if lockerr := mfs.mu.RLock(); lockerr != nil {
		return nil, nil, lockerr
	}
	defer mfs.mu.RUnlock()
	return mfs.Indexer.DirOrFileByPath(name)
}

type minioFileCreation struct {
	pw      *io.PipeWriter
	w       int64
	size    int64
	fs      *minioVFS
	name    string
	err     error
	meta    *vfs.MetaExtractor
	newdoc  *vfs.FileDoc
	olddoc  *vfs.FileDoc
	maxsize int64
}

func (f *minioFileCreation) Read(p []byte) (int, error) {
	return 0, os.ErrInvalid
}

func (f *minioFileCreation) ReadAt(p []byte, off int64) (int, error) {
	return 0, os.ErrInvalid
}

func (f *minioFileCreation) Seek(offset int64, whence int) (int64, error) {
	return 0, os.ErrInvalid
}

func (f *minioFileCreation) Write(p []byte) (int, error) {
	if f.meta != nil {
		if _, err := (*f.meta).Write(p); err != nil && err != io.ErrClosedPipe {
			(*f.meta).Abort(err)
			f.meta = nil
		}
	}

	n, err := f.pw.Write(p)
	if err != nil {
		f.err = err
		return n, err
	}

	f.w += int64(n)
	if f.maxsize >= 0 && f.w > f.maxsize {
		f.err = vfs.ErrFileTooBig
		return n, f.err
	}

	if f.size >= 0 && f.w > f.size {
		f.err = vfs.ErrContentLengthMismatch
		return n, f.err
	}

	return n, nil
}

func (f *minioFileCreation) Close() (err error) {
	defer func() {
		if err != nil {
			// Deleting the object should be secure since we use X-Versions-Location
			// on the container and the old object should be restored.
			f.fs.c.RemoveObject(f.fs.bucket, f.name) // #nosec

			// If an error has occured that is not due to the index update, we should
			// delete the file from the index.
			_, isCouchErr := couchdb.IsCouchError(err)
			if !isCouchErr && f.olddoc == nil {
				f.fs.Indexer.DeleteFileDoc(f.newdoc) // #nosec
			}
		}
	}()

	if f.err != nil {
		f.pw.CloseWithError(f.err)
	} else {
		f.err = f.pw.Close()
	}
	if err != nil {
		if f.meta != nil {
			(*f.meta).Abort(err)
			f.meta = nil
		}
	}

	newdoc, olddoc, written := f.newdoc, f.olddoc, f.w
	if f.meta != nil {
		if errc := (*f.meta).Close(); errc == nil {
			newdoc.Metadata = (*f.meta).Result()
		}
	}

	if f.err != nil {
		return f.err
	}

	// XXX
	if newdoc.MD5Sum == nil {
		// newdoc.MD5Sum = md5sum
	}

	if f.size < 0 {
		newdoc.ByteSize = written
	}

	if newdoc.ByteSize != written {
		return vfs.ErrContentLengthMismatch
	}

	// The document is already added to the index when closing the file creation
	// handler. When updating the content of the document with the final
	// informations (size, md5, ...) we can reuse the same document as olddoc.
	if olddoc == nil || !olddoc.Trashed {
		newdoc.Trashed = false
	}
	if olddoc == nil {
		olddoc = newdoc
	}
	lockerr := f.fs.mu.Lock()
	if lockerr != nil {
		return lockerr
	}
	defer f.fs.mu.Unlock()
	err = f.fs.Indexer.UpdateFileDoc(olddoc, newdoc)
	// If we reach a conflict error, the document has been modified while
	// uploading the content of the file.
	//
	// TODO: remove dep on couchdb, with a generalized conflict error for
	// UpdateFileDoc/UpdateDirDoc.
	if couchdb.IsConflictError(err) {
		resdoc, err := f.fs.Indexer.FileByID(olddoc.ID())
		if err != nil {
			return err
		}
		resdoc.Metadata = newdoc.Metadata
		resdoc.ByteSize = newdoc.ByteSize
		return f.fs.Indexer.UpdateFileDoc(resdoc, resdoc)
	}
	return
}

type minioFileOpen struct {
	o *minio.Object
}

func (f minioFileOpen) Read(p []byte) (int, error) {
	return f.o.Read(p)
}

func (f minioFileOpen) ReadAt(p []byte, off int64) (int, error) {
	return f.o.ReadAt(p, off)
}

func (f minioFileOpen) Seek(offset int64, whence int) (int64, error) {
	return f.o.Seek(offset, whence)
}

func (f minioFileOpen) Write(p []byte) (int, error) {
	return 0, os.ErrInvalid
}

func (f minioFileOpen) Close() error {
	return f.o.Close()
}

func objectToFileDocV2(object minio.ObjectInfo) (filePath string, fileDoc *vfs.FileDoc, err error) {
	md5sum, err := hex.DecodeString(object.ETag)
	if err != nil {
		return
	}
	name := object.Metadata.Get("creation-name")
	if name == "" {
		name = fmt.Sprintf("Unknown %s", utils.RandomString(10))
	}
	var cdate time.Time
	if v := object.Metadata.Get("created-at"); v != "" {
		cdate, _ = time.Parse(time.RFC3339, v)
	}
	if cdate.IsZero() {
		cdate = time.Now()
	}
	executable, _ := strconv.ParseBool(object.Metadata.Get("exec"))
	mime, class := vfs.ExtractMimeAndClass(object.ContentType)
	filePath = path.Join(vfs.OrphansDirName, name)
	fileDoc, err = vfs.NewFileDoc(
		name,
		"",
		object.Size,
		md5sum,
		mime,
		class,
		cdate,
		executable,
		false,
		nil)
	return
}

var (
	_ vfs.VFS  = &minioVFS{}
	_ vfs.File = &minioFileCreation{}
	_ vfs.File = minioFileOpen{}
)
