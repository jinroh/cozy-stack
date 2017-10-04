package permissions

import (
	"fmt"
	"time"

	"github.com/cozy/cozy-stack/pkg/consts"
	"github.com/cozy/cozy-stack/pkg/couchdb"
	"github.com/cozy/cozy-stack/pkg/couchdb/mango"
)

// Permission is a storable object containing a set of rules and
// several codes
type Permission struct {
	PID         string            `json:"_id,omitempty"`
	PRev        string            `json:"_rev,omitempty"`
	Type        string            `json:"type,omitempty"`
	SourceID    string            `json:"source_id,omitempty"`
	Permissions Set               `json:"permissions,omitempty"`
	ExpiresAt   int               `json:"expires_at,omitempty"`
	Codes       map[string]string `json:"codes,omitempty"`
}

const (
	// TypeRegister is the value of Permission.Type for the temporary permissions
	// allowed by registerToken
	TypeRegister = "register"

	// TypeWebapp if the value of Permission.Type for an application
	TypeWebapp = "app"

	// TypeKonnector if the value of Permission.Type for an application
	TypeKonnector = "konnector"

	// TypeShareByLink if the value of Permission.Type for a share (by link) permission doc
	TypeShareByLink = "share"

	// TypeOauth if the value of Permission.Type for a oauth permission doc
	TypeOauth = "oauth"

	// TypeCLI if the value of Permission.Type for a command-line permission doc
	TypeCLI = "cli"
)

// ID implements jsonapi.Doc
func (p *Permission) ID() string { return p.PID }

// Rev implements jsonapi.Doc
func (p *Permission) Rev() string { return p.PRev }

// DocType implements jsonapi.Doc
func (p *Permission) DocType() string { return consts.Permissions }

// Clone implements couchdb.Doc
func (p *Permission) Clone() couchdb.Doc {
	cloned := *p
	cloned.Codes = make(map[string]string)
	for k, v := range p.Codes {
		cloned.Codes[k] = v
	}
	return &cloned
}

// SetID implements jsonapi.Doc
func (p *Permission) SetID(id string) { p.PID = id }

// SetRev implements jsonapi.Doc
func (p *Permission) SetRev(rev string) { p.PRev = rev }

// AddRules add some rules to the permission doc
func (p *Permission) AddRules(rules ...Rule) {
	newperms := append(p.Permissions, rules...)
	p.Permissions = newperms
}

// RemoveRule remove a rule from the permission doc
func (p *Permission) RemoveRule(rule Rule) {
	newperms := p.Permissions[:0]
	for _, r := range p.Permissions {
		if r.Title != rule.Title {
			newperms = append(newperms, r)
		}
	}
	p.Permissions = newperms
}

// PatchCodes replace the permission docs codes
func (p *Permission) PatchCodes(codes map[string]string) {
	p.Codes = codes
}

// Revoke destroy a Permission
func (p *Permission) Revoke(db couchdb.Database) error {
	return couchdb.DeleteDoc(db, p)
}

// ParentOf check if child has been created by p
func (p *Permission) ParentOf(child *Permission) bool {

	canBeParent := p.Type == TypeWebapp || p.Type == TypeOauth

	return child.Type == TypeShareByLink && canBeParent &&
		child.SourceID == p.SourceID
}

// GetByID fetch a permission by its ID
func GetByID(db couchdb.Database, id string) (*Permission, error) {
	var perm Permission
	err := couchdb.GetDoc(db, consts.Permissions, id, &perm)
	return &perm, err
}

// GetForRegisterToken create a non-persisted permissions doc with hard coded
// registerToken permissions set
func GetForRegisterToken() *Permission {
	return &Permission{
		Type: TypeRegister,
		Permissions: Set{
			Rule{
				Verbs:  Verbs(GET),
				Type:   consts.Settings,
				Values: []string{consts.InstanceSettingsID},
			},
		},
	}
}

// GetForOauth create a non-persisted permissions doc from a oauth token scopes
func GetForOauth(claims *Claims) (*Permission, error) {
	set, err := UnmarshalScopeString(claims.Scope)
	if err != nil {
		return nil, err
	}
	pdoc := &Permission{
		Type:        TypeOauth,
		Permissions: set,
		SourceID:    claims.Subject,
	}
	return pdoc, nil
}

// GetForCLI create a non-persisted permissions doc for the command-line
func GetForCLI(claims *Claims) (*Permission, error) {
	set, err := UnmarshalScopeString(claims.Scope)
	if err != nil {
		return nil, err
	}
	pdoc := &Permission{
		Type:        TypeCLI,
		Permissions: set,
	}
	return pdoc, nil
}

// GetForWebapp retrieves the Permission doc for a given webapp
func GetForWebapp(db couchdb.Database, slug string) (*Permission, error) {
	return getForApp(db, TypeWebapp, consts.Apps, slug)
}

// GetForKonnector retrieves the Permission doc for a given konnector
func GetForKonnector(db couchdb.Database, slug string) (*Permission, error) {
	return getForApp(db, TypeKonnector, consts.Konnectors, slug)
}

func getForApp(db couchdb.Database, permType, docType, slug string) (*Permission, error) {
	opts := &couchdb.FindRequest{
		UseIndex: "by-source-and-type",
		Selector: mango.And(
			mango.Equal("type", permType),
			mango.Equal("source_id", docType+"/"+slug),
		),
		Limit: 1,
	}
	var res *Permission
	rows := couchdb.FindDocs(db, consts.Permissions, opts)
	ok, err := rows.NextAndScanDoc(&res)
	if err != nil {
		// FIXME https://issues.apache.org/jira/browse/COUCHDB-3336
		// With a cluster of couchdb, we can have a race condition where we
		// query an index before it has been updated for an app that has
		// just been created.
		time.Sleep(1 * time.Second)
		rows = couchdb.FindDocs(db, consts.Permissions, opts)
		ok, err = rows.NextAndScanDoc(&res)
		if err != nil {
			return nil, err
		}
	}
	if !ok {
		return nil, fmt.Errorf("no permission doc for %v", slug)
	}
	return res, nil
}

// GetForShareCode retrieves the Permission doc for a given sharing code
func GetForShareCode(db couchdb.Database, tokenCode string) (*Permission, error) {
	rows := couchdb.ExecView(db, consts.PermissionsShareByCView, &couchdb.ViewRequest{
		Key:         tokenCode,
		IncludeDocs: true,
		Limit:       1,
	})
	var doc *Permission
	ok, err := rows.NextAndScanDoc(&doc)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("no permission doc for token %v", tokenCode)
	}
	return doc, nil
}

// CreateWebappSet creates a Permission doc for an app
func CreateWebappSet(db couchdb.Database, slug string, set Set) (*Permission, error) {
	existing, _ := GetForWebapp(db, slug)
	if existing != nil {
		return nil, fmt.Errorf("There is already a permission doc for %v", slug)
	}
	return createAppSet(db, TypeWebapp, consts.Apps, slug, set)
}

// CreateKonnectorSet creates a Permission doc for a konnector
func CreateKonnectorSet(db couchdb.Database, slug string, set Set) (*Permission, error) {
	existing, _ := GetForKonnector(db, slug)
	if existing != nil {
		return nil, fmt.Errorf("There is already a permission doc for %v", slug)
	}
	return createAppSet(db, TypeKonnector, consts.Konnectors, slug, set)
}

func createAppSet(db couchdb.Database, typ, docType, slug string, set Set) (*Permission, error) {
	doc := &Permission{
		Type:        typ,
		SourceID:    docType + "/" + slug,
		Permissions: set,
	}
	err := couchdb.CreateDoc(db, doc)
	if err != nil {
		return nil, err
	}
	return doc, nil
}

// UpdateWebappSet creates a Permission doc for an app
func UpdateWebappSet(db couchdb.Database, slug string, set Set) (*Permission, error) {
	doc, err := GetForWebapp(db, slug)
	if err != nil {
		return nil, err
	}
	return updateAppSet(db, doc, TypeWebapp, consts.Apps, slug, set)
}

// UpdateKonnectorSet creates a Permission doc for a konnector
func UpdateKonnectorSet(db couchdb.Database, slug string, set Set) (*Permission, error) {
	doc, err := GetForKonnector(db, slug)
	if err != nil {
		return nil, err
	}
	return updateAppSet(db, doc, TypeKonnector, consts.Konnectors, slug, set)
}

func updateAppSet(db couchdb.Database, doc *Permission, typ, docType, slug string, set Set) (*Permission, error) {
	doc.Permissions = set
	err := couchdb.UpdateDoc(db, doc)
	if err != nil {
		return nil, err
	}
	return doc, nil
}

// CreateShareSet creates a Permission doc for sharing
func CreateShareSet(db couchdb.Database, parent *Permission, codes map[string]string, set Set) (*Permission, error) {
	if parent.Type != TypeWebapp && parent.Type != TypeKonnector && parent.Type != TypeOauth {
		return nil, ErrOnlyAppCanCreateSubSet
	}

	if !set.IsSubSetOf(parent.Permissions) {
		return nil, ErrNotSubset
	}

	// SourceID stays the same, allow quick destruction of all children permissions
	doc := &Permission{
		Type:        TypeShareByLink,
		SourceID:    parent.SourceID,
		Permissions: set,
		Codes:       codes,
	}

	err := couchdb.CreateDoc(db, doc)
	if err != nil {
		return nil, err
	}

	return doc, nil
}

// DeleteShareSet revokes all the code in a permission set
func DeleteShareSet(db couchdb.Database, permID string) error {

	var doc *Permission
	err := couchdb.GetDoc(db, consts.Permissions, permID, doc)
	if err != nil {
		return err
	}

	return couchdb.DeleteDoc(db, doc)
}

// ForceWebapp creates or updates a Permission doc for a given webapp
func ForceWebapp(db couchdb.Database, slug string, set Set) error {
	existing, _ := GetForWebapp(db, slug)
	doc := &Permission{
		Type:        TypeWebapp,
		SourceID:    consts.Apps + "/" + slug,
		Permissions: set,
	}
	if existing == nil {
		return couchdb.CreateDoc(db, doc)
	}

	doc.SetID(existing.ID())
	doc.SetRev(existing.Rev())
	return couchdb.UpdateDoc(db, doc)
}

// DestroyWebapp remove all Permission docs for a given app
func DestroyWebapp(db couchdb.Database, slug string) error {
	return destroyApp(db, consts.Apps, slug)
}

// DestroyKonnector remove all Permission docs for a given konnector
func DestroyKonnector(db couchdb.Database, slug string) error {
	return destroyApp(db, consts.Konnectors, slug)
}

func destroyApp(db couchdb.Database, docType, slug string) error {
	var res []*Permission
	opts := &couchdb.FindRequest{
		UseIndex: "by-source-and-type",
		Selector: mango.And(
			mango.Equal("source_id", docType+"/"+slug),
			mango.Exists("type"),
		),
	}
	rows := couchdb.FindDocs(db, consts.Permissions, opts)
	for {
		done, err := rows.Next()
		if err != nil {
			return err
		}
		if done {
			break
		}
		var p *Permission
		if err = rows.ScanDoc(&p); err != nil {
			return err
		}
		res = append(res, p)
	}
	for _, p := range res {
		err := couchdb.DeleteDoc(db, p)
		if err != nil {
			return err
		}
	}
	return nil
}

// GetPermissionsForIDs gets permissions for several IDs
// returns for every id the combined allowed verbset
func GetPermissionsForIDs(db couchdb.Database, doctype string, ids []string) (map[string]*VerbSet, error) {
	keys := make([]interface{}, len(ids))
	for i, id := range ids {
		keys[i] = []string{doctype, "_id", id}
	}

	rows := couchdb.ExecView(db, consts.PermissionsShareByDocView, &couchdb.ViewRequest{
		Keys: keys,
	})

	result := make(map[string]*VerbSet)
	for {
		done, err := rows.Next()
		if err != nil {
			return nil, err
		}
		if done {
			break
		}
		var key []string
		var val *VerbSet
		if err = rows.ScanKey(&key); err != nil {
			return nil, err
		}
		if err = rows.ScanValue(&val); err != nil {
			return nil, err
		}
		if _, ok := result[key[2]]; ok {
			result[key[2]].Merge(val)
		} else {
			result[key[2]] = val
		}
	}

	return result, nil
}

// GetPermissionsByType gets all share permissions for a given doctype.
// The passed Cursor will be modified in place
func GetPermissionsByType(db couchdb.Database, doctype string, cursor couchdb.Cursor) ([]*Permission, couchdb.Cursor, error) {
	var req = &couchdb.ViewRequest{
		StartKey:    []string{doctype},
		EndKey:      []string{doctype, couchdb.MaxString},
		IncludeDocs: true,
	}

	var result []*Permission
	rows := couchdb.ExecView(db, consts.PermissionsShareByDocView, req)
	rows.WithCursor(cursor)
	for {
		done, err := rows.Next()
		if err != nil {
			return nil, nil, err
		}
		if done {
			break
		}
		var pdoc *Permission
		if err := rows.ScanDoc(&pdoc); err != nil {
			return nil, nil, err
		}
		result = append(result, pdoc)
	}

	return result, rows.NextCursor(), nil
}

// GetSharedWithMePermissionsByDoctype retrieves the permissions in all
// the sharings that apply to the given doctype, where the user is a recipient
// (i.e. owner is false).
//
// The cursor will be modified in place.
func GetSharedWithMePermissionsByDoctype(db couchdb.Database, doctype string, cursor couchdb.Cursor) ([]*Permission, couchdb.Cursor, error) {
	return getSharedWithPermissionsByDoctype(db, doctype, cursor, false)
}

// GetSharedWithOthersPermissionsByDoctype retrieves the permissions in all the
// sharings that apply to the given doctype, where the user is the sharer (i.e.
// owner is true).
//
// The cursor will be modified in place.
func GetSharedWithOthersPermissionsByDoctype(db couchdb.Database, doctype string, cursor couchdb.Cursor) ([]*Permission, couchdb.Cursor, error) {
	return getSharedWithPermissionsByDoctype(db, doctype, cursor, true)
}

func getSharedWithPermissionsByDoctype(db couchdb.Database, doctype string, cursor couchdb.Cursor, owner bool) ([]*Permission, couchdb.Cursor, error) {
	var req = &couchdb.ViewRequest{
		StartKey:    []interface{}{doctype, owner},
		EndKey:      []interface{}{doctype, owner, couchdb.MaxString},
		IncludeDocs: false,
	}

	var result []*Permission
	rows := couchdb.ExecView(db, consts.SharedWithPermissionsView, req)
	rows.WithCursor(cursor)
	for {
		done, err := rows.Next()
		if err != nil {
			return nil, nil, err
		}
		if done {
			break
		}

		// The rows have the following format:
		// "id": "_id", "key": [type, owner, sharing_id], "value": [rule]
		// see consts/views.go and the view "SharedWithPermissionView"
		var keys []string
		if err = rows.ScanKey(&keys); err != nil || len(keys) < 2 {
			return nil, nil, err
		}

		var rule Rule
		var val struct {
			Description string   `json:"description"`
			Selector    string   `json:"selector"`
			Type        string   `json:"type"`
			Values      []string `json:"values"`
			Verbs       []string `json:"verbs"`
		}
		if err = rows.ScanValue(&val); err != nil {
			return nil, nil, err
		}
		rule.Description = val.Description
		rule.Selector = val.Selector
		rule.Type = val.Type
		rule.Values = val.Values

		// Since we didn't include the Sharing document (it contains all the
		// permissions, possibly more than what where are interested in),
		// we have to manually parse the rule.
		if len(val.Verbs) == 0 {
			rule.Verbs = ALL
		} else {
			rule.Verbs = VerbSet{}
			for _, verbStr := range val.Verbs {
				var verb Verb
				switch verbStr {
				case "GET":
					verb = GET
				case "POST":
					verb = POST
				case "PUT":
					verb = PUT
				case "PATCH":
					verb = PATCH
				case "DELETE":
					verb = DELETE
				default:
					continue
				}
				rule.Verbs.Merge(&VerbSet{verb: struct{}{}})
			}
		}

		result = append(result, &Permission{
			Type:        consts.Sharings,
			SourceID:    keys[2],
			Permissions: Set{rule},
		})
	}

	return result, rows.NextCursor(), nil
}
