package vfs

import (
	"errors"
	"path"
	"sync"

	"github.com/cozy/cozy-stack/pkg/lru"
	"github.com/jinroh/radix"
)

var errNoCache = errors.New("no cache")

var (
	memIndexerByDomain   map[string]*memIndexer
	memIndexerByDomainMu sync.Mutex
)

// memIndexer implements the VFS Cache interface and should be used
// for mono-stack usage, where only one cozy-stack is accessing to the
// VFS at a time.
//
// Internally it provides some optimisations to cache file attributes
// and avoid having multiple useless RTTs with CouchDB.
type memIndexer struct {
	mud  sync.RWMutex // mutex for directories data-structures
	lrud *lru.Cache   // lru cache for directories
	pthd *radix.Tree  // path directory to id map

	muf  sync.RWMutex // mutex for files data-structures
	lruf *lru.Cache   // lru cache for files
	pthf *radix.Tree  // (folderID, name) file pair to id map
}

// NewMemIndexer creates a new Indexer. The maxEntries parameter is
// used to specified the cache size: how many files and directories
// elements are kept in-memory
func NewMemIndexer(domain string, maxEntries int) *memIndexer {
	memIndexerByDomainMu.Lock()
	defer memIndexerByDomainMu.Unlock()
	if m, ok := memIndexerByDomain[domain]; ok {
		return m
	}
	if memIndexerByDomain == nil {
		memIndexerByDomain = make(map[string]*memIndexer)
	}
	m := new(memIndexer)
	m.init(maxEntries)
	memIndexerByDomain[domain] = m
	return m
}

func (m *memIndexer) init(maxEntries int) {
	dirEviction := func(key string, value interface{}) {
		if doc, ok := value.(*DirDoc); ok {
			m.pthd.Remove(doc.Fullpath)
		}
	}

	fileEviction := func(key string, value interface{}) {
		if doc, ok := value.(*FileDoc); ok {
			m.pthf.Remove(genFilePathID(doc.DirID, doc.DocName))
		}
	}

	m.pthd = radix.New()
	m.pthf = radix.New()
	m.lrud = &lru.Cache{MaxEntries: maxEntries, OnEvicted: dirEviction}
	m.lruf = &lru.Cache{MaxEntries: maxEntries, OnEvicted: fileEviction}
}

func (m *memIndexer) InitIndex() error {
	return nil
}

func (m *memIndexer) DiskUsage() (int64, error) {
	return 0, errNoCache
}

func (m *memIndexer) CreateFileDoc(doc *FileDoc) error {
	m.tapFile(doc)
	return nil
}

func (m *memIndexer) UpdateFileDoc(olddoc, newdoc *FileDoc) error {
	m.tapFile(newdoc)
	return nil
}

func (m *memIndexer) DeleteFileDoc(doc *FileDoc) error {
	m.rmFile(doc)
	return nil
}

func (m *memIndexer) CreateDirDoc(doc *DirDoc) error {
	m.tapDir(doc)
	return nil
}

func (m *memIndexer) UpdateDirDoc(olddoc, newdoc *DirDoc) error {
	m.tapDir(newdoc)
	return nil
}

func (m *memIndexer) DeleteDirDoc(doc *DirDoc) error {
	m.rmDir(doc)
	return nil
}

func (m *memIndexer) DirByID(fileID string) (*DirDoc, error) {
	if fileID == "" {
		panic("oups")
	}
	if doc, ok := m.dirCachedByID(fileID); ok {
		return doc, nil
	}
	return nil, errNoCache
}

func (m *memIndexer) DirByPath(name string) (*DirDoc, error) {
	if doc, ok := m.dirCachedByPath(name); ok {
		return doc, nil
	}
	return nil, errNoCache
}

func (m *memIndexer) FileByID(fileID string) (*FileDoc, error) {
	if doc, ok := m.fileCachedByID(fileID); ok {
		return doc, nil
	}
	return nil, errNoCache
}

func (m *memIndexer) FileByPath(name string) (*FileDoc, error) {
	if doc, ok := m.fileCachedByPath(name); ok {
		return doc, nil
	}
	return nil, errNoCache
}

func (m *memIndexer) FilePath(doc *FileDoc) (string, error) {
	return "", errNoCache
}

func (m *memIndexer) DirOrFileByID(fileID string) (*DirDoc, *FileDoc, error) {
	if doc, ok := m.dirCachedByID(fileID); ok {
		return doc, nil, nil
	}
	if doc, ok := m.fileCachedByID(fileID); ok {
		return nil, doc, nil
	}
	return nil, nil, errNoCache
}

func (m *memIndexer) DirOrFileByPath(name string) (*DirDoc, *FileDoc, error) {
	if doc, ok := m.dirCachedByPath(name); ok {
		return doc, nil, nil
	}
	if doc, ok := m.fileCachedByPath(name); ok {
		return nil, doc, nil
	}
	return nil, nil, errNoCache
}

func (m *memIndexer) DirIterator(doc *DirDoc, opts *IteratorOptions) DirIterator {
	return nil
}

func (m *memIndexer) tapDir(doc *DirDoc) {
	m.mud.Lock()
	defer m.mud.Unlock()
	key := doc.DocID

	if old, ok := m.lrud.Get(key); ok {
		olddoc := old.(*DirDoc)
		// if the directory was renamed, we invalidate all its
		// subdirectories from the cache
		if olddoc.Fullpath != doc.Fullpath {
			removed, ok := m.pthd.RemoveBranch(olddoc.Fullpath)
			if ok {
				removed.Foreach(func(id interface{}, _ string) error {
					m.lrud.Remove(id.(string))
					return nil
				})
			}
		}

		// if the directory has a new parent, we also invalidate all its
		// direct children
		if olddoc.DirID != doc.DirID {
			m.muf.Lock()
			removed, ok := m.pthf.RemoveBranch(olddoc.DirID)
			if ok {
				removed.Foreach(func(id interface{}, _ string) error {
					m.lruf.Remove(id.(string))
					return nil
				})
			}
			m.muf.Unlock()
		}
	}

	m.lrud.Add(key, doc)
	m.pthd.Insert(doc.Fullpath, key)
}

func (m *memIndexer) tapFile(doc *FileDoc) {
	m.muf.Lock()
	defer m.muf.Unlock()
	key := doc.DocID
	if old, ok := m.lruf.Get(key); ok {
		olddoc := old.(*FileDoc)
		m.pthf.Remove(genFilePathID(olddoc.DirID, olddoc.DocName))
	}
	m.lruf.Add(key, doc)
	m.pthf.Insert(genFilePathID(doc.DirID, doc.DocName), key)
}

func (m *memIndexer) rmDir(doc *DirDoc) {
	m.mud.Lock()
	defer m.mud.Unlock()
	m.lrud.Remove(doc.DocID)
}

func (m *memIndexer) rmFile(doc *FileDoc) {
	m.muf.Lock()
	defer m.muf.Unlock()
	m.lruf.Remove(doc.DocID)
}

func (m *memIndexer) dirCachedByID(fileID string) (*DirDoc, bool) {
	m.mud.Lock()
	defer m.mud.Unlock()
	if v, ok := m.lrud.Get(fileID); ok {
		return v.(*DirDoc), true
	}
	return nil, false
}

func (m *memIndexer) dirCachedByPath(name string) (*DirDoc, bool) {
	m.mud.Lock()
	defer m.mud.Unlock()
	pid, ok := m.pthd.Get(name)
	if ok {
		v, _ := m.lrud.Get(pid.(string))
		return v.(*DirDoc), true
	}
	return nil, false
}

func (m *memIndexer) fileCachedByID(fileID string) (*FileDoc, bool) {
	m.muf.Lock()
	defer m.muf.Unlock()
	if v, ok := m.lruf.Get(fileID); ok {
		return v.(*FileDoc), true
	}
	return nil, false
}

func (m *memIndexer) fileCachedByPath(name string) (*FileDoc, bool) {
	dirPath := path.Dir(name)
	parent, err := m.DirByPath(dirPath)
	if err != nil {
		return nil, false
	}
	folderID, filename := parent.ID(), path.Base(name)
	return m.fileCachedByFolderID(folderID, filename)
}

func (m *memIndexer) fileCachedByFolderID(folderID, name string) (*FileDoc, bool) {
	m.muf.Lock()
	defer m.muf.Unlock()
	pid, ok := m.pthf.Get(genFilePathID(folderID, name))
	if ok {
		v, _ := m.lruf.Get(pid.(string))
		return v.(*FileDoc), true
	}
	return nil, false
}

func genFilePathID(folderID, name string) string {
	return folderID + "/" + name
}

var _ Indexer = &memIndexer{}
