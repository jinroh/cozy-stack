package move

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/base32"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/cozy/cozy-stack/pkg/consts"
	"github.com/cozy/cozy-stack/pkg/couchdb"
	"github.com/cozy/cozy-stack/pkg/crypto"
	"github.com/cozy/cozy-stack/pkg/instance"
	"github.com/cozy/cozy-stack/pkg/vfs"
)

const (
	albumsFile     = "albums.json"
	referencesFile = "references.json"
)

// Reference between albumid and filepath
type Reference struct {
	Albumid  string `json:"albumid"`
	Filepath string `json:"filepath"`
}

func writeFile(tw *tar.Writer, doc *vfs.FileDoc, name string, fs vfs.VFS) error {
	file, err := fs.OpenFile(doc)
	if err != nil {
		return err
	}
	defer file.Close()
	hdr := &tar.Header{
		Name:       "files/" + name,
		Mode:       0640,
		Size:       doc.Size(),
		ModTime:    doc.ModTime(),
		AccessTime: doc.CreatedAt,
		ChangeTime: doc.UpdatedAt,
		Typeflag:   tar.TypeReg,
	}
	if doc.Executable {
		hdr.Mode = 0750
	}
	if err = tw.WriteHeader(hdr); err != nil {
		return err
	}
	_, err = io.Copy(tw, file)
	return err
}

func createDir(tw *tar.Writer, dir *vfs.DirDoc, name string) error {
	hdr := &tar.Header{
		Name:     "files/" + name,
		Mode:     0755,
		Size:     dir.Size(),
		ModTime:  dir.ModTime(),
		Typeflag: tar.TypeDir,
	}
	return tw.WriteHeader(hdr)
}

func albums(tw *tar.Writer, instance *instance.Instance) error {
	doctype := consts.PhotosAlbums

	var results []map[string]interface{}
	{
		rows := couchdb.GetAllDocs(instance, doctype)
		for {
			var v map[string]interface{}
			done, err := rows.Next()
			if couchdb.IsNoDatabaseError(err) {
				return nil
			}
			if err != nil {
				return err
			}
			if done {
				break
			}
			if err = rows.ScanDoc(&v); err != nil {
				return err
			}
			results = append(results, v)
		}
	}

	hdrDir := &tar.Header{
		Name:     "albums",
		Mode:     0755,
		Typeflag: tar.TypeDir,
	}
	if err := tw.WriteHeader(hdrDir); err != nil {
		return err
	}

	var content bytes.Buffer
	size := 0
	for _, val := range results {
		b, err := json.Marshal(val)
		if err != nil {
			return err
		}
		b = append(b, '\n')
		size += len(b)
		if _, err = content.Write(b); err != nil {
			return err
		}
	}

	hdrAlbum := &tar.Header{
		Name:       "albums/" + albumsFile,
		Mode:       0644,
		Size:       int64(size),
		AccessTime: time.Now(),
		ChangeTime: time.Now(),
		Typeflag:   tar.TypeReg,
	}
	if err := tw.WriteHeader(hdrAlbum); err != nil {
		return err
	}
	if _, err := content.WriteTo(tw); err != nil {
		return err
	}

	var buf bytes.Buffer
	size = 0
	{
		fs := instance.VFS()
		req := &couchdb.ViewRequest{
			StartKey: []string{consts.PhotosAlbums},
			EndKey:   []string{consts.PhotosAlbums, couchdb.MaxString},
		}
		rows := couchdb.ExecView(instance, consts.FilesReferencedByView, req)
		for {
			done, err := rows.Next()
			if err != nil {
				return err
			}
			if done {
				break
			}
			var key []string
			if err = rows.ScanKey(&key); err != nil {
				return err
			}
			id := key[1]
			doc, err := fs.FileByID(rows.ID())
			if err != nil {
				return err
			}
			path, err := fs.FilePath(doc)
			if err != nil {
				return err
			}
			ref := Reference{
				Albumid:  id,
				Filepath: path,
			}
			b, err := json.Marshal(ref)
			if err != nil {
				return err
			}
			b = append(b, '\n')
			size += len(b)
			if _, err = buf.Write(b); err != nil {
				return err
			}
		}
	}

	hdrRef := &tar.Header{
		Name:       "albums/" + referencesFile,
		Mode:       0644,
		Size:       int64(size),
		AccessTime: time.Now(),
		ChangeTime: time.Now(),
		Typeflag:   tar.TypeReg,
	}
	if err := tw.WriteHeader(hdrRef); err != nil {
		return err
	}
	_, err := buf.WriteTo(tw)
	return err
}

func export(tw *tar.Writer, instance *instance.Instance) error {
	fs := instance.VFS()
	err := vfs.Walk(fs, "/", func(name string, dir *vfs.DirDoc, file *vfs.FileDoc, err error) error {
		if err != nil {
			return err
		}

		if dir != nil {
			if err := createDir(tw, dir, name); err != nil {
				return err
			}
		}

		if file != nil {
			if err := writeFile(tw, file, name, fs); err != nil {
				return err
			}

		}

		return nil
	})
	if err != nil {
		return err
	}
	return albums(tw, instance)
}

// Export is used to create a tarball with files and photos from an instance
func Export(instance *instance.Instance) (filename string, err error) {
	domain := instance.Domain
	tab := crypto.GenerateRandomBytes(20)
	id := base32.StdEncoding.EncodeToString(tab)
	filename = fmt.Sprintf("%s-%s.tar.gz", domain, id)

	w, err := os.Create(filename)
	if err != nil {
		return
	}
	defer w.Close()

	gw := gzip.NewWriter(w)
	tw := tar.NewWriter(gw)
	err = export(tw, instance)
	defer func() {
		if errc := tw.Close(); err == nil && errc != nil {
			err = errc
		}
		if errc := gw.Close(); err == nil && errc != nil {
			err = errc
		}
	}()
	return
}
