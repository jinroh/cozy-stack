package vfs

type cachedIndexer struct {
	origin Indexer
	cache  Indexer
}

func NewCachedIndexer(origin, cache Indexer) *cachedIndexer {
	return &cachedIndexer{origin: origin, cache: cache}
}

func (c *cachedIndexer) InitIndex() error {
	if err := c.origin.InitIndex(); err != nil {
		return err
	}
	return c.cache.InitIndex()
}

func (c *cachedIndexer) DiskUsage() (int64, error) {
	if u, err := c.cache.DiskUsage(); err == nil {
		return u, nil
	}
	return c.origin.DiskUsage()
}

func (c *cachedIndexer) CreateFileDoc(doc *FileDoc) error {
	if err := c.origin.CreateFileDoc(doc); err != nil {
		return err
	}
	return c.cache.CreateFileDoc(doc)
}

func (c *cachedIndexer) UpdateFileDoc(olddoc, newdoc *FileDoc) error {
	if err := c.origin.UpdateFileDoc(olddoc, newdoc); err != nil {
		return err
	}
	return c.cache.UpdateFileDoc(olddoc, newdoc)
}

func (c *cachedIndexer) DeleteFileDoc(doc *FileDoc) error {
	if err := c.origin.DeleteFileDoc(doc); err != nil {
		return err
	}
	return c.cache.DeleteFileDoc(doc)
}

func (c *cachedIndexer) CreateDirDoc(doc *DirDoc) error {
	if err := c.origin.CreateDirDoc(doc); err != nil {
		return err
	}
	return c.cache.CreateDirDoc(doc)
}

func (c *cachedIndexer) UpdateDirDoc(olddoc, newdoc *DirDoc) error {
	if err := c.origin.UpdateDirDoc(olddoc, newdoc); err != nil {
		return err
	}
	return c.cache.UpdateDirDoc(olddoc, newdoc)
}

func (c *cachedIndexer) DeleteDirDoc(doc *DirDoc) error {
	if err := c.origin.DeleteDirDoc(doc); err != nil {
		return err
	}
	return c.cache.DeleteDirDoc(doc)
}

func (c *cachedIndexer) DirByID(fileID string) (*DirDoc, error) {
	if v, err := c.cache.DirByID(fileID); err == nil {
		return v, nil
	}
	return c.origin.DirByID(fileID)
}

func (c *cachedIndexer) DirByPath(name string) (*DirDoc, error) {
	if v, err := c.cache.DirByPath(name); err == nil {
		return v, nil
	}
	return c.origin.DirByPath(name)
}

func (c *cachedIndexer) FileByID(fileID string) (*FileDoc, error) {
	if v, err := c.cache.FileByID(fileID); err == nil {
		return v, nil
	}
	return c.origin.FileByID(fileID)
}

func (c *cachedIndexer) FileByPath(name string) (*FileDoc, error) {
	if v, err := c.cache.FileByPath(name); err == nil {
		return v, nil
	}
	return c.origin.FileByPath(name)
}

func (c *cachedIndexer) FilePath(doc *FileDoc) (string, error) {
	if v, err := c.cache.FilePath(doc); err == nil {
		return v, nil
	}
	return c.origin.FilePath(doc)
}

func (c *cachedIndexer) DirOrFileByID(fileID string) (*DirDoc, *FileDoc, error) {
	if d, f, err := c.cache.DirOrFileByID(fileID); err == nil {
		return d, f, nil
	}
	return c.origin.DirOrFileByID(fileID)
}

func (c *cachedIndexer) DirOrFileByPath(name string) (*DirDoc, *FileDoc, error) {
	if d, f, err := c.cache.DirOrFileByPath(name); err == nil {
		return d, f, nil
	}
	return c.origin.DirOrFileByPath(name)
}

func (c *cachedIndexer) DirIterator(doc *DirDoc, opts *IteratorOptions) DirIterator {
	return c.origin.DirIterator(doc, opts)
}
