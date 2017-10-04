package couchdb

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/cozy/cozy-stack/pkg/config"
	"github.com/cozy/cozy-stack/pkg/couchdb/mango"
	"github.com/cozy/cozy-stack/pkg/logger"
	"github.com/cozy/cozy-stack/pkg/realtime"
	"github.com/google/go-querystring/query"
)

var couchdbClient = &http.Client{
	Timeout: 10 * time.Second,
}

// MaxString is the unicode character "\uFFFF", useful in query as
// a upperbound for string.
const MaxString = mango.MaxString

// SelectorReferencedBy is the string constant for the references in a JSON
// document.
const SelectorReferencedBy = "referenced_by"

// Doc is the interface that encapsulate a couchdb document, of any
// serializable type. This interface defines method to set and get the
// ID of the document.
type Doc interface {
	ID() string
	Rev() string
	DocType() string
	Clone() Doc

	SetID(id string)
	SetRev(rev string)
}

// Database is the type passed to every function in couchdb package
// for now it is just a string with the database prefix.
type Database interface {
	Prefix() string
}

// prefixedDB implements the Database interface
type prefixedDB struct {
	prefix string
}

// Prefix implements the Database interface on prefixedDB
func (sdb *prefixedDB) Prefix() string { return sdb.prefix }

// NewDatabase returns a Database from a prefix, useful for test
func NewDatabase(domain string) Database {
	return &prefixedDB{prefix: domain}
}

func rtevent(db Database, verb string, doc, oldDoc Doc) {
	domain := db.Prefix()
	if err := runHooks(domain, verb, doc, oldDoc); err != nil {
		logger.WithDomain(db.Prefix()).Errorf("error in hooks on %s %s %v\n", verb, doc.DocType(), err)
	}

	e := &realtime.Event{
		Verb:   verb,
		Doc:    doc.Clone(),
		OldDoc: oldDoc,
		Domain: domain,
	}
	go realtime.GetHub().Publish(e)
}

// GlobalDB is the prefix used for stack-scoped db
var GlobalDB = NewDatabase("global")

// GlobalSecretsDB is the the prefix used for db which hold
// a cozy stack secrets.
var GlobalSecretsDB = NewDatabase("secrets")

// View is the map/reduce thing in CouchDB
type View struct {
	Name    string `json:"-"`
	Doctype string `json:"-"`
	Map     string `json:"map"`
	Reduce  string `json:"reduce,omitempty"`
}

// JSONDoc is a map representing a simple json object that implements
// the Doc interface.
type JSONDoc struct {
	M    map[string]interface{}
	Type string
}

// ID returns the identifier field of the document
//   "io.cozy.event/123abc123" == doc.ID()
func (j JSONDoc) ID() string {
	id, ok := j.M["_id"].(string)
	if ok {
		return id
	}
	return ""
}

// Rev returns the revision field of the document
//   "3-1234def1234" == doc.Rev()
func (j JSONDoc) Rev() string {
	rev, ok := j.M["_rev"].(string)
	if ok {
		return rev
	}
	return ""
}

// DocType returns the document type of the document
//   "io.cozy.event" == doc.Doctype()
func (j JSONDoc) DocType() string {
	return j.Type
}

// SetID is used to set the identifier of the document
func (j JSONDoc) SetID(id string) {
	if id == "" {
		delete(j.M, "_id")
	} else {
		j.M["_id"] = id
	}
}

// SetRev is used to set the revision of the document
func (j JSONDoc) SetRev(rev string) {
	if rev == "" {
		delete(j.M, "_rev")
	} else {
		j.M["_rev"] = rev
	}
}

// Clone is used to create a copy of the document
func (j JSONDoc) Clone() Doc {
	cloned := JSONDoc{Type: j.Type}
	cloned.M = make(map[string]interface{})
	for k, v := range j.M {
		cloned.M[k] = v
	}
	return cloned
}

// MarshalJSON implements json.Marshaller by proxying to internal map
func (j JSONDoc) MarshalJSON() ([]byte, error) {
	return json.Marshal(j.M)
}

// UnmarshalJSON implements json.Unmarshaller by proxying to internal map
func (j *JSONDoc) UnmarshalJSON(bytes []byte) error {
	err := json.Unmarshal(bytes, &j.M)
	if err != nil {
		return err
	}
	doctype, ok := j.M["_type"].(string)
	if ok {
		j.Type = doctype
	}
	delete(j.M, "_type")
	return nil
}

// ToMapWithType returns the JSONDoc internal map including its DocType
// its used in request response.
func (j *JSONDoc) ToMapWithType() map[string]interface{} {
	j.M["_type"] = j.DocType()
	return j.M
}

// Get returns the value of one of the db fields
func (j JSONDoc) Get(key string) interface{} {
	return j.M[key]
}

// Valid implements permissions.Validable on JSONDoc.
//
// The `referenced_by` selector is a special case: the `values` field of such
// rule has the format "doctype/id" and it cannot directly be compared to the
// same field of a JSONDoc since, in the latter, the format is:
// "referenced_by": [
//     {"type": "doctype1", "id": "id1"},
//     {"type": "doctype2", "id": "id2"},
// ]
func (j JSONDoc) Valid(field, value string) bool {
	if field == SelectorReferencedBy {
		rawReferences := j.Get(field)
		references, ok := rawReferences.([]interface{})
		if !ok {
			return false
		}

		values := strings.Split(value, "/")
		if len(values) != 2 {
			return false
		}
		valueType, valueID := values[0], values[1]

		for _, ref := range references {
			reference := ref.(map[string]interface{})
			if valueType == reference["type"].(string) &&
				valueID == reference["id"].(string) {
				return true
			}
		}

		return false
	}

	return fmt.Sprintf("%v", j.Get(field)) == value
}

var dbNameEscaper = strings.NewReplacer(
	".", "-",
	":", "-",
)

var dbNameUnescaper = strings.NewReplacer(
	"-", ".",
)

func unescapeCouchdbName(name string) string {
	return dbNameUnescaper.Replace(name)
}

func escapeCouchdbName(name string) string {
	return dbNameEscaper.Replace(strings.ToLower(name))
}

func dbNameHasPrefix(dbname, dbprefix string) (bool, string) {
	dbprefix = escapeCouchdbName(dbprefix + "/")
	if !strings.HasPrefix(dbname, dbprefix) {
		return false, ""
	}
	return true, strings.Replace(dbname, dbprefix, "", 1)
}

func docPath(id string) string {
	return "/" + url.PathEscape(id)
}

func makeDBName(db Database, doctype string) string {
	return url.PathEscape(escapeCouchdbName(db.Prefix() + "/" + doctype))
}

func doctypeRequest(db Database, method, doctype, path string, req interface{}, out interface{}) error {
	return makeRequest(db, method, "/"+makeDBName(db, doctype)+path, req, out)
}

func makeRequest(db Database, method, fullPath string, reqbody interface{}, out interface{}) error {
	var reqjson []byte
	var err error

	if reqbody != nil {
		reqjson, err = json.Marshal(reqbody)
		if err != nil {
			return err
		}
	}

	prefix := db.Prefix()
	if prefix == "" {
		return fmt.Errorf("You need to provide a valid database")
	}

	log := logger.WithDomain(prefix)
	if logger.IsDebug(log) {
		log.Debugf("request: %s %s %s", method, fullPath, string(bytes.TrimSpace(reqjson)))
	}

	u := config.CouchURL(fullPath)
	req, err := http.NewRequest(method, u, bytes.NewReader(reqjson))
	if err != nil {
		return newRequestError(err)
	}
	req.Header.Add("Accept", "application/json")
	if reqbody != nil {
		req.Header.Add("Content-Type", "application/json")
	}

	auth := config.GetConfig().CouchDB.Auth
	if auth != nil {
		if p, ok := auth.Password(); ok {
			req.SetBasicAuth(auth.Username(), p)
		}
	}

	start := time.Now()
	resp, err := couchdbClient.Do(req)
	if err != nil {
		err = newConnectionError(err)
		log.Error(err.Error())
		return err
	}
	defer resp.Body.Close()

	elapsed := time.Since(start)
	if elapsed.Seconds() >= 5 {
		log.Warnf("slow request on %s %s (%s)", method, fullPath, elapsed)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var body []byte
		body, err = ioutil.ReadAll(resp.Body)
		if err != nil {
			err = newIOReadError(err)
		} else {
			err = newCouchdbError(resp.StatusCode, body)
		}
		log.Debug(err.Error())
		return err
	}
	if out == nil {
		return nil
	}

	if logger.IsDebug(log) {
		var data []byte
		data, err = ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		log.Debugf("response: %s", string(bytes.TrimSpace(data)))
		err = json.Unmarshal(data, &out)
	} else {
		err = json.NewDecoder(resp.Body).Decode(&out)
	}

	return err
}

// DBStatus responds with informations on the database: size, number of
// documents, sequence numbers, etc.
func DBStatus(db Database, doctype string, out interface{}) error {
	return doctypeRequest(db, http.MethodGet, doctype, "", nil, out)
}

// AllDoctypes returns a list of all the doctypes that have a database
// on a given instance
func AllDoctypes(db Database) ([]string, error) {
	var dbs []string
	if err := makeRequest(db, http.MethodGet, "/_all_dbs", nil, &dbs); err != nil {
		return nil, err
	}
	prefix := escapeCouchdbName(db.Prefix())
	var doctypes []string
	for _, dbname := range dbs {
		parts := strings.Split(dbname, "/")
		if len(parts) == 2 && parts[0] == prefix {
			doctype := unescapeCouchdbName(parts[1])
			doctypes = append(doctypes, doctype)
		}
	}
	return doctypes, nil
}

// GetDoc fetch a document by its docType and ID, out is filled with
// the document by json.Unmarshal-ing
func GetDoc(db Database, doctype, id string, out Doc) error {
	var err error
	id, err = validateDocID(id)
	if err != nil {
		return err
	}
	if id == "" {
		return fmt.Errorf("Missing ID for GetDoc")
	}
	return doctypeRequest(db, http.MethodGet, doctype, docPath(id), nil, out)
}

// CreateDB creates the necessary database for a doctype
func CreateDB(db Database, doctype string) error {
	return doctypeRequest(db, http.MethodPut, doctype, "", nil, nil)
}

// DeleteDB destroy the database for a doctype
func DeleteDB(db Database, doctype string) error {
	return doctypeRequest(db, http.MethodDelete, doctype, "", nil, nil)
}

// DeleteAllDBs will remove all the couchdb doctype databases for
// a couchdb.DB.
func DeleteAllDBs(db Database) error {
	var dbsList []string
	err := makeRequest(db, http.MethodGet, "/_all_dbs", nil, &dbsList)
	if err != nil {
		return err
	}

	for _, doctypedb := range dbsList {
		hasPrefix, doctype := dbNameHasPrefix(doctypedb, db.Prefix())
		if !hasPrefix {
			continue
		}
		if err = DeleteDB(db, doctype); err != nil {
			return err
		}
	}

	return nil
}

// ResetDB destroy and recreate the database for a doctype
func ResetDB(db Database, doctype string) error {
	err := DeleteDB(db, doctype)
	if err != nil && !IsNoDatabaseError(err) {
		return err
	}
	return CreateDB(db, doctype)
}

// DeleteDoc deletes a struct implementing the couchb.Doc interface
// If the document's current rev does not match the one passed,
// a CouchdbError(409 conflict) will be returned.
// The document's SetRev will be called with tombstone revision
func DeleteDoc(db Database, doc Doc) error {
	id, err := validateDocID(doc.ID())
	if err != nil {
		return err
	}
	if id == "" {
		return fmt.Errorf("Missing ID for DeleteDoc")
	}

	var res updateResponse
	path := docPath(id) + "?rev=" + url.QueryEscape(doc.Rev())
	err = doctypeRequest(db, http.MethodDelete, doc.DocType(), path, nil, &res)
	if err != nil {
		return err
	}
	doc.SetRev(res.Rev)
	rtevent(db, realtime.EventDelete, doc, nil)
	return nil
}

// Reseter is a interface for reseting a cloned doc
type Reseter interface {
	Reset()
}

// UpdateDoc update a document. The document ID and Rev should be filled.
// The doc SetRev function will be called with the new rev.
func UpdateDoc(db Database, doc Doc) error {
	id, err := validateDocID(doc.ID())
	if err != nil {
		return err
	}
	doctype := doc.DocType()
	if id == "" || doc.Rev() == "" || doctype == "" {
		return fmt.Errorf("UpdateDoc doc argument should have doctype, id and rev")
	}

	// The old doc is requested to be emitted throught rtevent.
	// This is useful to keep track of the modifications for the triggers.
	oldDoc := doc.Clone()
	if r, ok := oldDoc.(Reseter); ok {
		r.Reset()
	}
	path := docPath(id)
	err = doctypeRequest(db, http.MethodGet, doctype, path, nil, oldDoc)
	if err != nil {
		return err
	}
	var res updateResponse
	err = doctypeRequest(db, http.MethodPut, doctype, path, doc, &res)
	if err != nil {
		return err
	}
	doc.SetRev(res.Rev)
	rtevent(db, realtime.EventUpdate, doc, oldDoc)
	return nil
}

// BulkUpdateDocs is used to update several docs in one call, as a bulk.
func BulkUpdateDocs(db Database, doctype string, docs []interface{}) error {
	body := struct {
		Docs []interface{} `json:"docs"`
	}{docs}
	var res []updateResponse
	if err := doctypeRequest(db, http.MethodPost, doctype, "/_bulk_docs", body, &res); err != nil {
		return err
	}
	if len(res) != len(docs) {
		return errors.New("BulkUpdateDoc receive an unexpected number of responses")
	}
	for i, doc := range docs {
		if d, ok := doc.(Doc); ok {
			d.SetRev(res[i].Rev)
			rtevent(db, realtime.EventUpdate, d, nil)
		}
	}
	return nil
}

// CreateNamedDoc persist a document with an ID.
// if the document already exist, it will return a 409 error.
// The document ID should be fillled.
// The doc SetRev function will be called with the new rev.
func CreateNamedDoc(db Database, doc Doc) error {
	id, err := validateDocID(doc.ID())
	if err != nil {
		return err
	}
	doctype := doc.DocType()
	if doc.Rev() != "" || id == "" || doctype == "" {
		return fmt.Errorf("CreateNamedDoc should have type and id but no rev")
	}
	var res updateResponse
	err = doctypeRequest(db, http.MethodPut, doctype, docPath(id), doc, &res)
	if err != nil {
		return err
	}
	doc.SetRev(res.Rev)
	rtevent(db, realtime.EventCreate, doc, nil)
	return nil
}

// CreateNamedDocWithDB is equivalent to CreateNamedDoc but creates the database
// if it does not exist
func CreateNamedDocWithDB(db Database, doc Doc) error {
	err := CreateNamedDoc(db, doc)
	if IsNoDatabaseError(err) {
		err = CreateDB(db, doc.DocType())
		if err != nil {
			return err
		}
		return CreateNamedDoc(db, doc)
	}
	return err
}

// Upsert create the doc or update it if it already exists.
func Upsert(db Database, doc Doc) error {
	id, err := validateDocID(doc.ID())
	if err != nil {
		return err
	}

	var old JSONDoc
	err = GetDoc(db, doc.DocType(), id, &old)
	if IsNoDatabaseError(err) {
		err = CreateDB(db, doc.DocType())
		if err != nil {
			return err
		}
		return CreateNamedDoc(db, doc)
	}
	if IsNotFoundError(err) {
		return CreateNamedDoc(db, doc)
	}
	if err != nil {
		return err
	}

	doc.SetRev(old.Rev())
	return UpdateDoc(db, doc)
}

func createDocOrDb(db Database, doc Doc, response interface{}) error {
	doctype := doc.DocType()
	err := doctypeRequest(db, http.MethodPost, doctype, "", doc, response)
	if err == nil || !IsNoDatabaseError(err) {
		return err
	}
	err = CreateDB(db, doctype)
	if err == nil || IsFileExists(err) {
		err = doctypeRequest(db, http.MethodPost, doctype, "", doc, response)
	}
	return err
}

// CreateDoc is used to persist the given document in the couchdb
// database. The document's SetRev and SetID function will be called
// with the document's new ID and Rev.
// This function creates a database if this is the first document of its type
func CreateDoc(db Database, doc Doc) error {
	var res *updateResponse

	if doc.ID() != "" {
		return newDefinedIDError()
	}

	err := createDocOrDb(db, doc, &res)
	if err != nil {
		return err
	} else if !res.Ok {
		return fmt.Errorf("CouchDB replied with 200 ok=false")
	}

	doc.SetID(res.ID)
	doc.SetRev(res.Rev)
	rtevent(db, realtime.EventCreate, doc, nil)
	return nil
}

// DefineViews creates a design doc with some views
func DefineViews(db Database, views []*View) error {
	type viewDesignDoc struct {
		ID    string           `json:"_id,omitempty"`
		Rev   string           `json:"_rev,omitempty"`
		Lang  string           `json:"language"`
		Views map[string]*View `json:"views"`
	}

	for _, v := range views {
		id := "_design/" + v.Name
		path := "/_design/" + url.PathEscape(v.Name)
		doc := &viewDesignDoc{
			ID:    id,
			Lang:  "javascript",
			Views: map[string]*View{v.Name: v},
		}
		err := doctypeRequest(db, http.MethodPut, v.Doctype, path, &doc, nil)
		if IsNoDatabaseError(err) {
			err = CreateDB(db, v.Doctype)
			if err != nil {
				return err
			}
			err = doctypeRequest(db, http.MethodPut, v.Doctype, path, &doc, nil)
		}
		if IsConflictError(err) {
			var old viewDesignDoc
			err = doctypeRequest(db, http.MethodGet, v.Doctype, path, nil, &old)
			if err != nil {
				return err
			}
			doc.Rev = old.Rev
			err = doctypeRequest(db, http.MethodPut, v.Doctype, path, &doc, nil)
		}

		if err != nil {
			return err
		}
	}
	return nil
}

// DefineIndex define the index on the doctype database
// see query package on how to define an index
func DefineIndex(db Database, index *mango.Index) error {
	return DefineIndexRaw(db, index.Doctype, index.Request, nil)
}

// DefineIndexRaw defines a index
func DefineIndexRaw(db Database, doctype string, index interface{}, result interface{}) error {
	path := "/_index"
	err := doctypeRequest(db, http.MethodPost, doctype, path, index, result)
	if IsNoDatabaseError(err) {
		if err = CreateDB(db, doctype); err != nil {
			return err
		}
		err = doctypeRequest(db, http.MethodPost, doctype, path, index, result)
	}
	return err
}

// DefineIndexes defines a list of indexes
func DefineIndexes(db Database, indexes []*mango.Index) error {
	for _, index := range indexes {
		if err := DefineIndex(db, index); err != nil {
			return err
		}
	}
	return nil
}

// ExecView executes the specified view function
func ExecView(db Database, view *View, req *ViewRequest) *ViewRows {
	return &ViewRows{
		index: -1,
		db:    db,
		view:  view,
		req:   req,
	}
}

type ViewRows struct {
	index  int
	rows   []*row
	db     Database
	view   *View
	req    *ViewRequest
	cursor Cursor
}

func (r *ViewRows) WithCursor(c Cursor) {
	r.cursor = c
	r.cursor.applyTo(r.req)
}

func (r *ViewRows) NextCursor() Cursor {
	return r.cursor
}

func (r *ViewRows) Next() (bool, error) {
	if r.index == -1 {
		path := fmt.Sprintf("/_design/%s/_view/%s", r.view.Name, r.view.Name)
		var res struct {
			Total int    `json:"total_rows"`
			Rows  []*row `json:"rows"`
		}
		err := doctypeRequest(r.db, http.MethodPost, r.view.Doctype, path, r.req, &res)
		if err != nil {
			return false, err
		}
		if r.cursor != nil {
			r.cursor = r.cursor.updateFrom(res.Rows)
		}
		r.rows = res.Rows
	}
	r.index++
	return r.index >= len(r.rows), nil
}

func (r *ViewRows) NextAndScanDoc(v interface{}) (bool, error) {
	done, err := r.Next()
	if err != nil {
		return false, err
	}
	if done {
		return false, nil
	}
	if err = r.ScanDoc(v); err != nil {
		return false, err
	}
	return true, nil
}

func (r *ViewRows) ScanDoc(v interface{}) error {
	return json.Unmarshal(r.rows[r.index].Doc, &v)
}

func (r *ViewRows) ScanValue(v interface{}) error {
	return json.Unmarshal(r.rows[r.index].Value, &v)
}

func (r *ViewRows) ScanKey(v interface{}) error {
	return json.Unmarshal(r.rows[r.index].Key, &v)
}

// ID returns the ID of the current row.
func (r *ViewRows) ID() string {
	return r.rows[r.index].ID
}

// FindDocs returns all documents matching the passed FindRequest
// documents will be unmarshalled in the provided results slice.
func FindDocs(db Database, doctype string, opts *FindRequest) *FindDocsRows {
	if opts.Limit == 0 {
		opts.Limit = 100
	}
	return FindDocsRaw(db, doctype, opts)
}

// FindDocsRaw find documents
func FindDocsRaw(db Database, doctype string, opts interface{}) *FindDocsRows {
	return &FindDocsRows{
		index:   -1,
		db:      db,
		doctype: doctype,
		opts:    opts,
	}
}

type FindDocsRows struct {
	index   int
	docs    []json.RawMessage
	db      Database
	doctype string
	opts    interface{}
}

func (r *FindDocsRows) Next() (bool, error) {
	if r.index == -1 {
		var res struct {
			Warning string            `json:"warning"`
			Docs    []json.RawMessage `json:"docs"`
		}
		req := r.opts
		err := doctypeRequest(r.db, http.MethodPost, r.doctype, "/_find", req, &res)
		if err != nil {
			return false, err
		}
		// Developer should not rely on unoptimized index.
		if res.Warning != "" {
			return false, unoptimalError()
		}
		r.docs = res.Docs[:]
	}
	r.index++
	return r.index >= len(r.docs), nil
}

func (r *FindDocsRows) ScanDoc(v interface{}) error {
	return json.Unmarshal(r.docs[r.index], &v)
}

func (r *FindDocsRows) NextAndScanDoc(v interface{}) (ok bool, err error) {
	done, err := r.Next()
	if err != nil {
		return false, err
	}
	if done {
		return false, nil
	}
	if err = r.ScanDoc(v); err != nil {
		return false, err
	}
	return true, nil
}

// CountAllDocs returns the number of documents of the given doctype.
func CountAllDocs(db Database, doctype string) (int, error) {
	var res struct {
		TotalRows int `json:"total_rows"`
	}
	path := "/_all_docs?limit=0"
	if err := doctypeRequest(db, http.MethodGet, doctype, path, nil, &res); err != nil {
		return 0, err
	}
	return res.TotalRows, nil
}

// GetAllDocs is used to go through the documents of the specified doctype on
// the given database.
func GetAllDocs(db Database, doctype string, options ...*AllDocsOptions) *AllDocsRows {
	desc := false
	limit := 100
	if len(options) > 0 {
		opts := options[0]
		desc = opts.Descending
		if opts.Limit > 0 {
			limit = opts.Limit
		}
	}
	return &AllDocsRows{
		index:   -1,
		db:      db,
		doctype: doctype,
		limit:   limit,
		desc:    desc,
		fetch:   true,
	}
}

// AllDocsRows can be used to iterate over the rows of a database associated
// with a doctype.
type AllDocsRows struct {
	index    int
	rows     []*row
	db       Database
	limit    int
	desc     bool
	fetch    bool
	doctype  string
	startKey string
}

// Next can be used to iterate on the next row. It returns a boolean stating
// whether or not the iterator still has values.
func (r *AllDocsRows) Next() (done bool, err error) {
	r.index++

	if r.index >= len(r.rows) {
		if !r.fetch {
			return true, nil
		}

		r.index = 0

		for {
			skip := 0
			if r.startKey != "" {
				skip = 1
			}
			req := &struct {
				Descending    bool   `url:"descending,omitempty"`
				Limit         int    `url:"limit,omitempty"`
				Skip          int    `url:"skip,omitempty"`
				StartKeyDocID string `url:"startkey_docid,omitempty"`
			}{
				Descending:    r.desc,
				Limit:         r.limit,
				Skip:          skip,
				StartKeyDocID: r.startKey,
			}

			var v url.Values
			v, err = query.Values(req)
			if err != nil {
				return false, err
			}

			v.Add("include_docs", "true")

			var res struct {
				Offset    int    `json:"offset"`
				TotalRows int    `json:"total_rows"`
				Rows      []*row `json:"rows"`
			}
			path := "/_all_docs?" + v.Encode()
			if err = doctypeRequest(r.db, http.MethodGet, r.doctype, path, nil, &res); err != nil {
				return false, err
			}
			if len(res.Rows) == 0 {
				return true, nil
			}
			r.fetch = len(res.Rows) == r.limit
			r.startKey = res.Rows[len(res.Rows)-1].ID
			r.rows = res.Rows

			// filter out _design documents
			var designOffset int
			for i := 0; i < len(r.rows); i++ {
				if !strings.HasPrefix(r.rows[i].ID, "_design") {
					designOffset = i
					break
				}
			}
			if designOffset > 0 {
				r.rows = r.rows[designOffset:]
			}

			if len(r.rows) > 0 {
				break
			}
		}
	}

	return false, nil
}

// ID returns the ID of the current row.
func (r *AllDocsRows) ID() string {
	return r.rows[r.index].ID
}

// ScanDoc will unmarshal the content of the document on the current row.
func (r *AllDocsRows) ScanDoc(v interface{}) error {
	return json.Unmarshal(r.rows[r.index].Doc, &v)
}

func validateDocID(id string) (string, error) {
	if len(id) > 0 && id[0] == '_' {
		return "", newBadIDError(id)
	}
	return id, nil
}

type row struct {
	ID    string          `json:"id"`
	Doc   json.RawMessage `json:"doc"`
	Value json.RawMessage `json:"value"`
	Key   json.RawMessage `json:"key"`
}

type updateResponse struct {
	ID  string `json:"id"`
	Rev string `json:"rev"`
	Ok  bool   `json:"ok"`
}

// FindRequest is used to build a find request
type FindRequest struct {
	Selector mango.Filter  `json:"selector"`
	UseIndex string        `json:"use_index,omitempty"`
	Limit    int           `json:"limit,omitempty"`
	Skip     int           `json:"skip,omitempty"`
	Sort     *mango.SortBy `json:"sort,omitempty"`
	Fields   []string      `json:"fields,omitempty"`
}

// AllDocsOptions is used as options for GetAllDocs method
type AllDocsOptions struct {
	Descending bool
	Limit      int
}

// ViewRequest are all params that can be passed to a view
// It can be encoded either as a POST-json or a GET-url.
type ViewRequest struct {
	Key      interface{}   `json:"key,omitempty"`
	Keys     []interface{} `json:"keys,omitempty"`
	StartKey interface{}   `json:"start_key,omitempty"`
	EndKey   interface{}   `json:"end_key,omitempty"`

	StartKeyDocID string `json:"startkey_docid,omitempty"`
	EndKeyDocID   string `json:"endkey_docid,omitempty"`

	Limit        int  `json:"limit"`
	Skip         int  `json:"skip"`
	Descending   bool `json:"descending"`
	IncludeDocs  bool `json:"include_docs"`
	InclusiveEnd bool `json:"inclusive_end"`
	Reduce       bool `json:"reduce"`
	Group        bool `json:"group"`
	GroupLevel   int  `json:"group_level"`
}
