// Package data provide simple CRUD operation on couchdb doc
package data

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"

	"github.com/cozy/cozy-stack/pkg/consts"
	"github.com/cozy/cozy-stack/pkg/couchdb"
	"github.com/cozy/cozy-stack/web/files"
	"github.com/cozy/cozy-stack/web/jsonapi"
	"github.com/cozy/cozy-stack/web/middlewares"
	"github.com/cozy/cozy-stack/web/permissions"
	"github.com/labstack/echo"
)

func paramIsTrue(c echo.Context, param string) bool {
	return c.QueryParam(param) == "true"
}

// ValidDoctype validates the doctype and sets it in the context of the request.
func ValidDoctype(next echo.HandlerFunc) echo.HandlerFunc {
	// TODO extends me to verify characters allowed in db name.
	return func(c echo.Context) error {
		doctype := c.Param("doctype")
		if doctype == "" {
			return jsonapi.NewError(http.StatusBadRequest, "Invalid doctype '%s'", doctype)
		}
		c.Set("doctype", doctype)

		docidraw := c.Param("docid")
		docid, err := url.QueryUnescape(docidraw)
		if err != nil {
			return jsonapi.NewError(http.StatusBadRequest, "Invalid docid '%s'", docid)
		}
		c.Set("docid", docid)

		return next(c)
	}
}

func fixErrorNoDatabaseIsWrongDoctype(err error) error {
	if couchdb.IsNoDatabaseError(err) {
		err.(*couchdb.Error).Reason = "wrong_doctype"
	}
	return err
}

func allDoctypes(c echo.Context) error {
	instance := middlewares.GetInstance(c)

	if err := permissions.AllowWholeType(c, permissions.GET, consts.Doctypes); err != nil {
		return err
	}

	types, err := couchdb.AllDoctypes(instance)
	if err != nil {
		return err
	}
	var doctypes []string
	for _, typ := range types {
		if CheckReadable(typ) == nil {
			doctypes = append(doctypes, typ)
		}
	}
	return c.JSON(http.StatusOK, doctypes)
}

// GetDoc get a doc by its type and id
func getDoc(c echo.Context) error {
	instance := middlewares.GetInstance(c)
	doctype := c.Get("doctype").(string)
	docid := c.Get("docid").(string)

	if err := CheckReadable(doctype); err != nil {
		return err
	}

	if docid == "" {
		return dbStatus(c)
	}

	if paramIsTrue(c, "revs") {
		return proxy(c, docid)
	}

	var out couchdb.JSONDoc
	err := couchdb.GetDoc(instance, doctype, docid, &out)
	if err != nil {
		return fixErrorNoDatabaseIsWrongDoctype(err)
	}

	out.Type = doctype

	if err := permissions.Allow(c, permissions.GET, &out); err != nil {
		return err
	}

	return c.JSON(http.StatusOK, out.ToMapWithType())
}

// CreateDoc create doc from the json passed as body
func createDoc(c echo.Context) error {
	doctype := c.Get("doctype").(string)
	instance := middlewares.GetInstance(c)

	doc := couchdb.JSONDoc{Type: doctype}
	if err := json.NewDecoder(c.Request().Body).Decode(&doc.M); err != nil {
		return jsonapi.NewError(http.StatusBadRequest, err)
	}

	if err := CheckWritable(doctype); err != nil {
		return err
	}

	if err := permissions.Allow(c, permissions.POST, &doc); err != nil {
		return err
	}

	if err := couchdb.CreateDoc(instance, doc); err != nil {
		return err
	}

	return c.JSON(http.StatusCreated, echo.Map{
		"ok":   true,
		"id":   doc.ID(),
		"rev":  doc.Rev(),
		"type": doc.DocType(),
		"data": doc.ToMapWithType(),
	})
}

func createNamedDoc(c echo.Context, doc couchdb.JSONDoc) error {
	instance := middlewares.GetInstance(c)

	err := permissions.Allow(c, permissions.POST, &doc)
	if err != nil {
		return err
	}

	err = couchdb.CreateNamedDocWithDB(instance, doc)
	if err != nil {
		return fixErrorNoDatabaseIsWrongDoctype(err)
	}

	return c.JSON(http.StatusOK, echo.Map{
		"ok":   true,
		"id":   doc.ID(),
		"rev":  doc.Rev(),
		"type": doc.DocType(),
		"data": doc.ToMapWithType(),
	})
}

// UpdateDoc updates the document given in the request or creates a new one with
// the given id.
func UpdateDoc(c echo.Context) error {
	instance := middlewares.GetInstance(c)

	var doc couchdb.JSONDoc
	if err := json.NewDecoder(c.Request().Body).Decode(&doc); err != nil {
		return jsonapi.NewError(http.StatusBadRequest, err)
	}

	doc.Type = c.Param("doctype")

	if err := CheckWritable(doc.Type); err != nil {
		return err
	}

	if (doc.ID() == "") != (doc.Rev() == "") {
		return jsonapi.NewError(http.StatusBadRequest,
			"You must either provide an _id and _rev in document (update) or neither (create with fixed id).")
	}

	if doc.ID() != "" && doc.ID() != c.Get("docid").(string) {
		return jsonapi.NewError(http.StatusBadRequest, "document _id doesnt match url")
	}

	if doc.ID() == "" {
		doc.SetID(c.Get("docid").(string))
		return createNamedDoc(c, doc)
	}

	errWhole := permissions.AllowWholeType(c, permissions.PUT, doc.DocType())
	if errWhole != nil {

		// we cant apply to whole type, let's fetch old doc and see if it applies there
		var old couchdb.JSONDoc
		errFetch := couchdb.GetDoc(instance, doc.DocType(), doc.ID(), &old)
		if errFetch != nil {
			return errFetch
		}
		old.Type = doc.DocType()
		// check if permissions set allows manipulating old doc
		errOld := permissions.Allow(c, permissions.PUT, &old)
		if errOld != nil {
			return errOld
		}

		// also check if permissions set allows manipulating new doc
		errNew := permissions.Allow(c, permissions.PUT, &doc)
		if errNew != nil {
			return errNew
		}
	}

	errUpdate := couchdb.UpdateDoc(instance, doc)
	if errUpdate != nil {
		return fixErrorNoDatabaseIsWrongDoctype(errUpdate)
	}

	return c.JSON(http.StatusOK, echo.Map{
		"ok":   true,
		"id":   doc.ID(),
		"rev":  doc.Rev(),
		"type": doc.DocType(),
		"data": doc.ToMapWithType(),
	})
}

// DeleteDoc deletes the provided document from its database.
func DeleteDoc(c echo.Context) error {
	instance := middlewares.GetInstance(c)
	doctype := c.Get("doctype").(string)
	docid := c.Get("docid").(string)
	revHeader := c.Request().Header.Get("If-Match")
	revQuery := c.QueryParam("rev")
	rev := ""

	if revHeader != "" && revQuery != "" && revQuery != revHeader {
		return jsonapi.NewError(http.StatusBadRequest,
			"If-Match Header and rev query parameters mismatch")
	} else if revHeader != "" {
		rev = revHeader
	} else if revQuery != "" {
		rev = revQuery
	} else {
		return jsonapi.NewError(http.StatusBadRequest, "delete without revision")
	}

	if err := CheckWritable(doctype); err != nil {
		return err
	}

	var doc couchdb.JSONDoc
	err := couchdb.GetDoc(instance, doctype, docid, &doc)
	if err != nil {
		return err
	}
	doc.Type = doctype
	doc.SetRev(rev)

	err = permissions.Allow(c, permissions.DELETE, &doc)
	if err != nil {
		return err
	}

	err = couchdb.DeleteDoc(instance, &doc)
	if err != nil {
		return fixErrorNoDatabaseIsWrongDoctype(err)
	}

	return c.JSON(http.StatusOK, echo.Map{
		"ok":      true,
		"id":      doc.ID(),
		"rev":     doc.Rev(),
		"type":    doc.DocType(),
		"deleted": true,
	})

}

func defineIndex(c echo.Context) error {
	instance := middlewares.GetInstance(c)
	doctype := c.Get("doctype").(string)

	var definitionRequest map[string]interface{}
	if err := json.NewDecoder(c.Request().Body).Decode(&definitionRequest); err != nil {
		return jsonapi.NewError(http.StatusBadRequest, err)
	}

	if err := CheckReadable(doctype); err != nil {
		return err
	}

	if err := permissions.AllowWholeType(c, permissions.GET, doctype); err != nil {
		return err
	}

	var result map[string]interface{}
	err := couchdb.DefineIndexRaw(instance, doctype, definitionRequest, &result)
	if couchdb.IsNoDatabaseError(err) {
		if err = couchdb.CreateDB(instance, doctype); err == nil || couchdb.IsFileExists(err) {
			err = couchdb.DefineIndexRaw(instance, doctype, definitionRequest, &result)
		}
	}
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, result)
}

const maxMangoLimit = 100

func findDocuments(c echo.Context) error {
	instance := middlewares.GetInstance(c)
	doctype := c.Get("doctype").(string)

	var findRequest map[string]interface{}
	if err := json.NewDecoder(c.Request().Body).Decode(&findRequest); err != nil {
		return jsonapi.NewError(http.StatusBadRequest, err)
	}

	if err := CheckReadable(doctype); err != nil {
		return err
	}

	if err := permissions.AllowWholeType(c, permissions.GET, doctype); err != nil {
		return err
	}

	limit, hasLimit := findRequest["limit"].(float64)
	if !hasLimit || limit > maxMangoLimit {
		limit = 100
	}

	// add 1 so we know if there is more.
	findRequest["limit"] = limit + 1

	var docs []json.RawMessage
	rows := couchdb.FindDocsRaw(instance, doctype, findRequest)
	for i := 0; i < int(limit); i++ {
		done, err := rows.Next()
		if err != nil {
			return err
		}
		if done {
			break
		}
		var j json.RawMessage
		if err = rows.ScanDoc(&j); err != nil {
			return err
		}
		docs = append(docs, j)
	}
	next, _ := rows.Next()

	out := echo.Map{
		"docs":  docs,
		"limit": limit,
		"next":  next,
	}

	return c.JSON(http.StatusOK, out)
}

var allowedChangesParams = map[string]bool{
	"feed":         true,
	"style":        true,
	"since":        true,
	"limit":        true,
	"timeout":      true,
	"include_docs": true,
	"heartbeat":    true, // Pouchdb sends heartbeet even for non-continuous
	"_nonce":       true, // Pouchdb sends a request hash to avoid agressive caching by some browsers
}

func changesFeed(c echo.Context) error {
	instance := middlewares.GetInstance(c)
	var doctype = c.Get("doctype").(string)

	// Drop a clear error for parameters not supported by stack
	for key := range c.QueryParams() {
		if !allowedChangesParams[key] {
			return jsonapi.NewError(http.StatusBadRequest, "Unsuported query parameter '%s'", key)
		}
	}

	feed, err := couchdb.ValidChangesMode(c.QueryParam("feed"))
	if err != nil {
		return jsonapi.NewError(http.StatusBadRequest, err)
	}

	feedStyle, err := couchdb.ValidChangesStyle(c.QueryParam("style"))
	if err != nil {
		return jsonapi.NewError(http.StatusBadRequest, err)
	}

	limitString := c.QueryParam("limit")
	limit := 0
	if limitString != "" {
		if limit, err = strconv.Atoi(limitString); err != nil {
			return jsonapi.NewError(http.StatusBadRequest, "Invalid limit value '%s'", err.Error())
		}
	}

	includeDocs := paramIsTrue(c, "include_docs")

	if err = permissions.AllowWholeType(c, permissions.GET, doctype); err != nil {
		return err
	}

	results, err := couchdb.GetChanges(instance, &couchdb.ChangesRequest{
		DocType:     doctype,
		Feed:        feed,
		Style:       feedStyle,
		Since:       c.QueryParam("since"),
		Limit:       limit,
		IncludeDocs: includeDocs,
	})

	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, results)
}

func allDocs(c echo.Context) error {
	doctype := c.Get("doctype").(string)

	if err := CheckReadable(doctype); err != nil {
		return err
	}

	if err := permissions.AllowWholeType(c, permissions.GET, doctype); err != nil {
		return err
	}

	return proxy(c, "_all_docs")
}

// mostly just to prevent couchdb crash on replications
func dataAPIWelcome(c echo.Context) error {
	return c.JSON(http.StatusOK, echo.Map{
		"message": "welcome to a cozy API",
	})
}

func couchdbStyleErrorHandler(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {
		err := next(c)
		if err == nil {
			return nil
		}

		if ce, ok := err.(*couchdb.Error); ok {
			return c.JSON(ce.StatusCode, ce.JSON())
		}

		if he, ok := err.(*echo.HTTPError); ok {
			return c.JSON(he.Code, echo.Map{"error": he.Error()})
		}

		if je, ok := err.(*jsonapi.Error); ok {
			return c.JSON(je.Status, echo.Map{"error": je.Title})
		}

		return c.JSON(http.StatusInternalServerError, echo.Map{
			"error": err.Error(),
		})
	}
}

// Routes sets the routing for the data service
func Routes(router *echo.Group) {
	router.Use(couchdbStyleErrorHandler)

	// API Routes that don't depend on a doctype
	router.GET("/", dataAPIWelcome)
	router.GET("/_all_doctypes", allDoctypes)

	group := router.Group("/:doctype", ValidDoctype)

	replicationRoutes(group)

	// API Routes under /:doctype
	group.GET("/:docid", getDoc)
	group.PUT("/:docid", UpdateDoc)
	group.DELETE("/:docid", DeleteDoc)
	group.GET("/:docid/relationships/references", files.ListReferencesHandler)
	group.POST("/:docid/relationships/references", files.AddReferencesHandler)
	group.DELETE("/:docid/relationships/references", files.RemoveReferencesHandler)
	group.POST("/", createDoc)
	group.GET("/_all_docs", allDocs)
	group.POST("/_all_docs", allDocs)
	group.POST("/_index", defineIndex)
	group.POST("/_find", findDocuments)
	// group.DELETE("/:docid", DeleteDoc)
}
