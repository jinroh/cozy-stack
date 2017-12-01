package move

import (
	"archive/tar"
	"compress/gzip"
	"io"
	"os"
	"testing"
	"time"

	"github.com/cozy/cozy-stack/pkg/config"
	"github.com/cozy/cozy-stack/pkg/consts"
	"github.com/cozy/cozy-stack/pkg/couchdb"
	"github.com/cozy/cozy-stack/pkg/instance"
	"github.com/cozy/cozy-stack/pkg/vfs"
	"github.com/cozy/cozy-stack/tests/testutils"
	"github.com/stretchr/testify/assert"
)

var inst *instance.Instance
var filename string

var testdb couchdb.Database

func TestTardir(t *testing.T) {
	fs := inst.VFS()

	fd, err := os.Open("../../tests/fixtures/logos.zip")
	assert.NoError(t, err)
	defer fd.Close()
	zip, err := vfs.NewFileDoc("logos.zip", consts.RootDirID, -1, nil, "application/zip", "application", time.Now(), false, false, nil)
	assert.NoError(t, err)
	file, err := fs.CreateFile(zip, nil)
	assert.NoError(t, err)
	_, err = io.Copy(file, fd)
	assert.NoError(t, err)
	assert.NoError(t, file.Close())

	_, err = fs.OpenFile(zip)
	assert.NoError(t, err)

	//album
	testJsondoc := &couchdb.JSONDoc{
		Type: consts.PhotosAlbums,
	}
	testJsondoc.M = make(map[string]interface{})
	m := testJsondoc.ToMapWithType()
	m["name"] = "albumTest"
	delete(testJsondoc.M, "_type")
	err = couchdb.CreateDoc(testdb, testJsondoc)
	assert.NoError(t, err)
	assert.NotEmpty(t, testJsondoc.Rev(), testJsondoc.ID())

	testAlbumref := &couchdb.DocReference{
		ID:   testJsondoc.ID(),
		Type: testJsondoc.DocType(),
	}

	fd, err = os.Open("../../tests/fixtures/wet-cozy_20160910__©M4Dz.jpg")
	assert.NoError(t, err)
	defer fd.Close()

	image, err := vfs.NewFileDoc("wet-cozy_20160910__©M4Dz.jpg", consts.RootDirID, -1, nil, "application/image", "application", time.Now(), false, false, nil)
	assert.NoError(t, err)
	photo, err := fs.CreateFile(image, nil)
	assert.NoError(t, err)
	_, err = io.Copy(photo, fd)
	assert.NoError(t, err)
	assert.NoError(t, photo.Close())

	_, err = fs.OpenFile(image)
	assert.NoError(t, err)

	image.AddReferencedBy(*testAlbumref)
	err = couchdb.UpdateDoc(testdb, image)
	assert.NoError(t, err)

	filename, err = Export(inst)
	assert.NoError(t, err)

	r, err := os.Open(filename)
	assert.NoError(t, err)
	defer r.Close()

	gr, err := gzip.NewReader(r)
	assert.NoError(t, err)
	defer gr.Close()

	tr := tar.NewReader(gr)

	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		assert.NoError(t, err)
		if hdr.Name == "files/logos.zip" {
			assert.Equal(t, int64(2814), hdr.Size)
		}
		if hdr.Name == "albums/" {
			for {
				hdr, err := tr.Next()
				if err == io.EOF {
					break
				}
				assert.NoError(t, err)
				if hdr.Name == "albums.json" {
					assert.NotNil(t, hdr.Size)
				}
				if hdr.Name == "references.json" {
					assert.NotNil(t, hdr.Size)
				}
			}
		}

	}

}

func TestImport(t *testing.T) {
	fs := inst.VFS()

	r, err := os.Open(filename)
	assert.NoError(t, err)
	defer r.Close()

	dst, err := vfs.Mkdir(fs, "/destination", nil)
	assert.NoError(t, err)

	err = untar(r, dst, inst)
	assert.NoError(t, err)

	logo, err := fs.FileByPath("/destination/logos.zip")
	assert.NoError(t, err)
	assert.Equal(t, int64(2814), logo.Size())

	photo, err := fs.FileByPath("/destination/wet-cozy_20160910__©M4Dz.jpg")
	assert.NoError(t, err)
	assert.NotNil(t, photo.ReferencedBy)

	rows := couchdb.GetAllDocs(testdb, consts.PhotosAlbums)
	for {
		var val map[string]interface{}
		var done bool
		done, err = rows.Next()
		assert.NoError(t, err)
		if done {
			break
		}
		assert.NoError(t, err)
		err = rows.ScanDoc(&val)
		assert.NoError(t, err)
		if val["_id"] == photo.ReferencedBy[0].ID {
			assert.Equal(t, "albumTest", val["name"])
		}
	}

	err = os.Remove(filename)
	assert.NoError(t, err)
}

func TestMain(m *testing.M) {
	config.UseTestFile()
	testutils.NeedCouchdb()

	setup := testutils.NewSetup(m, "export_test")
	inst = setup.GetTestInstance()
	testdb = couchdb.NewDatabase(inst.Domain)

	os.Exit(setup.Run())
}
