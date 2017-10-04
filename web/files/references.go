package files

import (
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/cozy/cozy-stack/pkg/consts"
	"github.com/cozy/cozy-stack/pkg/couchdb"
	"github.com/cozy/cozy-stack/pkg/vfs"
	"github.com/cozy/cozy-stack/web/jsonapi"
	"github.com/cozy/cozy-stack/web/middlewares"
	"github.com/cozy/cozy-stack/web/permissions"
	"github.com/labstack/echo"
)

const maxRefLimit = 100

// ListReferencesHandler list all files referenced by a doc
// GET /data/:type/:id/relationships/references
// Beware, this is actually used in the web/data Routes
func ListReferencesHandler(c echo.Context) error {
	instance := middlewares.GetInstance(c)
	doctype := c.Get("doctype").(string)
	id := c.Param("docid")
	include := c.QueryParam("include")
	includeDocs := include == "files"

	if err := permissions.AllowTypeAndID(c, permissions.GET, doctype, id); err != nil {
		return err
	}

	cursor, err := jsonapi.ExtractPaginationCursor(c, maxRefLimit)
	if err != nil {
		return err
	}

	key := []string{doctype, id}
	reqCount := &couchdb.ViewRequest{Key: key, Reduce: true}

	rows := couchdb.ExecView(instance, consts.FilesReferencedByView, reqCount)
	if err != nil {
		return err
	}
	done, err := rows.Next()
	if err != nil {
		return err
	}
	count := 0
	if !done {
		if err = rows.ScanValue(&count); err != nil {
			return err
		}
	}

	sort := c.QueryParam("sort")
	descending := strings.HasPrefix(sort, "-")
	start := key
	end := []string{key[0], key[1], couchdb.MaxString}
	if descending {
		start, end = end, start
	}
	var view *couchdb.View
	switch sort {
	case "", "id", "-id":
		view = consts.FilesReferencedByView
	case "datetime", "-datetime":
		view = consts.ReferencedBySortedByDatetimeView
	default:
		return jsonapi.BadRequest(errors.New("Invalid sort parameter"))
	}

	req := &couchdb.ViewRequest{
		StartKey:    start,
		EndKey:      end,
		IncludeDocs: includeDocs,
		Reduce:      false,
		Descending:  descending,
	}

	var refs []couchdb.DocReference
	var objs []jsonapi.Object
	rows = couchdb.ExecView(instance, view, req)
	rows.WithCursor(cursor)
	for {
		done, err := rows.Next()
		if err != nil {
			return err
		}
		if done {
			break
		}

		refs = append(refs, couchdb.DocReference{
			ID:   rows.ID(),
			Type: consts.Files,
		})

		if includeDocs {
			var dof vfs.DirOrFileDoc
			if err = rows.ScanDoc(&dof); err != nil {
				return err
			}
			var obj jsonapi.Object
			d, f := dof.Refine()
			if d != nil {
				obj = newDir(d)
			} else {
				obj = newFile(f, instance)
			}
			objs = append(objs, obj)
		}
	}

	newCursor := rows.NextCursor()
	var links = &jsonapi.LinksList{}
	if newCursor.HasMore() {
		links.Next = fmt.Sprintf("%s?%s", c.Request().URL.Path, newCursor.ToQueryParams().Encode())
	}

	return jsonapi.DataRelations(c, http.StatusOK, refs, count, links, objs)
}

// AddReferencesHandler add some files references to a doc
// POST /data/:type/:id/relationships/references
// Beware, this is actually used in the web/data Routes
func AddReferencesHandler(c echo.Context) error {
	instance := middlewares.GetInstance(c)
	doctype := c.Get("doctype").(string)
	id := c.Param("docid")

	references, err := jsonapi.BindRelations(c.Request())
	if err != nil {
		return wrapVfsError(err)
	}

	docRef := couchdb.DocReference{
		Type: doctype,
		ID:   id,
	}

	if err := permissions.AllowTypeAndID(c, permissions.PUT, doctype, id); err != nil {
		return err
	}

	for _, fRef := range references {
		dir, file, err := instance.VFS().DirOrFileByID(fRef.ID)
		if err != nil {
			return wrapVfsError(err)
		}
		if file == nil {
			dir.AddReferencedBy(docRef)
			err = couchdb.UpdateDoc(instance, dir)
		} else {
			file.AddReferencedBy(docRef)
			err = couchdb.UpdateDoc(instance, file)
		}
		if err != nil {
			return wrapVfsError(err)
		}
	}

	return c.NoContent(204)
}

// RemoveReferencesHandler remove some files references from a doc
// DELETE /data/:type/:id/relationships/references
// Beware, this is actually used in the web/data Routes
func RemoveReferencesHandler(c echo.Context) error {
	instance := middlewares.GetInstance(c)
	doctype := c.Get("doctype").(string)
	id := c.Param("docid")

	references, err := jsonapi.BindRelations(c.Request())
	if err != nil {
		return wrapVfsError(err)
	}

	docRef := couchdb.DocReference{
		Type: doctype,
		ID:   id,
	}

	if err := permissions.AllowTypeAndID(c, permissions.DELETE, doctype, id); err != nil {
		return err
	}

	for _, fRef := range references {
		dir, file, err := instance.VFS().DirOrFileByID(fRef.ID)
		if err != nil {
			return wrapVfsError(err)
		}
		if file == nil {
			dir.RemoveReferencedBy(docRef)
			err = couchdb.UpdateDoc(instance, dir)
		} else {
			file.RemoveReferencedBy(docRef)
			err = couchdb.UpdateDoc(instance, file)
		}
		if err != nil {
			return wrapVfsError(err)
		}
	}

	return c.NoContent(204)
}
