package oauth

import (
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/cozy/cozy-stack/pkg/consts"
	"github.com/cozy/cozy-stack/pkg/couchdb"
	"github.com/cozy/cozy-stack/pkg/couchdb/mango"
	"github.com/cozy/cozy-stack/pkg/crypto"
	"github.com/cozy/cozy-stack/pkg/instance"
	"github.com/cozy/cozy-stack/pkg/permissions"

	jwt "gopkg.in/dgrijalva/jwt-go.v3"
)

// ClientSecretLen is the number of random bytes used for generating the client secret
const ClientSecretLen = 24 // #nosec

var internalError = &ClientRegistrationError{
	Code:  http.StatusInternalServerError,
	Error: "internal_server_error",
}

// Client is a struct for OAuth2 client. Most of the fields are described in
// the OAuth 2.0 Dynamic Client Registration Protocol. The exception is
// `client_kind`, and it is an optional field.
// See https://tools.ietf.org/html/rfc7591
//
// CouchID and ClientID are the same. They are just two ways to serialize to
// JSON, one for CouchDB and the other for the Dynamic Client Registration
// Protocol.
type Client struct {
	CouchID  string `json:"_id,omitempty"`  // Generated by CouchDB
	CouchRev string `json:"_rev,omitempty"` // Generated by CouchDB

	ClientID          string `json:"client_id,omitempty"`                 // Same as CouchID
	ClientSecret      string `json:"client_secret,omitempty"`             // Generated by the server
	SecretExpiresAt   int    `json:"client_secret_expires_at"`            // Forced by the server to 0 (no expiration)
	RegistrationToken string `json:"registration_access_token,omitempty"` // Generated by the server

	RedirectURIs    []string `json:"redirect_uris"`              // Declared by the client (mandatory)
	GrantTypes      []string `json:"grant_types"`                // Forced by the server to ["authorization_code", "refresh_token"]
	ResponseTypes   []string `json:"response_types"`             // Forced by the server to ["code"]
	ClientName      string   `json:"client_name"`                // Declared by the client (mandatory)
	ClientKind      string   `json:"client_kind,omitempty"`      // Declared by the client (optional, can be "desktop", "mobile", "browser", etc.)
	ClientURI       string   `json:"client_uri,omitempty"`       // Declared by the client (optional)
	LogoURI         string   `json:"logo_uri,omitempty"`         // Declared by the client (optional)
	PolicyURI       string   `json:"policy_uri,omitempty"`       // Declared by the client (optional)
	SoftwareID      string   `json:"software_id"`                // Declared by the client (mandatory)
	SoftwareVersion string   `json:"software_version,omitempty"` // Declared by the client (optional)

	// XXX omitempty does not work for time.Time, thus the interface{} type
	SynchronizedAt interface{} `json:"synchronized_at,omitempty"` // Date of the last synchronization, updated by /settings/synchronized
}

// ID returns the client qualified identifier
func (c *Client) ID() string { return c.CouchID }

// Rev returns the client revision
func (c *Client) Rev() string { return c.CouchRev }

// DocType returns the client document type
func (c *Client) DocType() string { return consts.OAuthClients }

// Clone implements couchdb.Doc
func (c *Client) Clone() couchdb.Doc {
	cloned := *c
	cloned.RedirectURIs = make([]string, len(c.RedirectURIs))
	copy(cloned.RedirectURIs, c.RedirectURIs)

	cloned.GrantTypes = make([]string, len(c.GrantTypes))
	copy(cloned.GrantTypes, c.GrantTypes)

	cloned.ResponseTypes = make([]string, len(c.ResponseTypes))
	copy(cloned.ResponseTypes, c.ResponseTypes)
	return &cloned
}

// SetID changes the client qualified identifier
func (c *Client) SetID(id string) { c.CouchID = id }

// SetRev changes the client revision
func (c *Client) SetRev(rev string) { c.CouchRev = rev }

// TransformIDAndRev makes the translation from the JSON of CouchDB to the
// one used in the dynamic client registration protocol
func (c *Client) TransformIDAndRev() {
	c.ClientID = c.CouchID
	c.CouchID = ""
	c.CouchRev = ""
}

// GetAll loads all the clients from the database, without the secrets
func GetAll(i *instance.Instance) ([]*Client, error) {
	var clients []*Client
	rows := couchdb.GetAllDocs(i, consts.OAuthClients)
	for {
		var c *Client
		done, err := rows.Next()
		if err != nil {
			return nil, err
		}
		if done {
			break
		}
		if err = rows.ScanDoc(&c); err != nil {
			return nil, err
		}
		c.ClientSecret = ""
		clients = append(clients, c)
	}
	return clients, nil
}

// FindClient loads a client from the database
func FindClient(i *instance.Instance, id string) (Client, error) {
	var c Client
	if err := couchdb.GetDoc(i, consts.OAuthClients, id, &c); err != nil {
		i.Logger().Errorf("[oauth] Failed to find the client %s: %s", id, err)
		return c, err
	}
	return c, nil
}

// ClientRegistrationError is a Client Registration Error Response, as described
// in the Client Dynamic Registration Protocol
// See https://tools.ietf.org/html/rfc7591#section-3.2.2 for errors
type ClientRegistrationError struct {
	Code        int    `json:"-"`
	Error       string `json:"error"`
	Description string `json:"error_description,omitempty"`
}

func (c *Client) checkMandatoryFields(i *instance.Instance) *ClientRegistrationError {
	if len(c.RedirectURIs) == 0 {
		return &ClientRegistrationError{
			Code:        http.StatusBadRequest,
			Error:       "invalid_redirect_uri",
			Description: "redirect_uris is mandatory",
		}
	}
	for _, redirectURI := range c.RedirectURIs {
		u, err := url.Parse(redirectURI)
		if err != nil ||
			u.Host == i.Domain ||
			u.Fragment != "" {
			return &ClientRegistrationError{
				Code:        http.StatusBadRequest,
				Error:       "invalid_redirect_uri",
				Description: fmt.Sprintf("%s is invalid", redirectURI),
			}
		}
	}
	if c.ClientName == "" {
		return &ClientRegistrationError{
			Code:        http.StatusBadRequest,
			Error:       "invalid_client_metadata",
			Description: "client_name is mandatory",
		}
	}
	if c.SoftwareID == "" {
		return &ClientRegistrationError{
			Code:        http.StatusBadRequest,
			Error:       "invalid_client_metadata",
			Description: "software_id is mandatory",
		}
	}

	return nil
}

// Create is a function that sets some fields, and then save it in Couch.
func (c *Client) Create(i *instance.Instance) *ClientRegistrationError {
	if err := c.checkMandatoryFields(i); err != nil {
		return err
	}

	{
		// Find the correct suffix to apply to the client name in case it is already
		// used.
		suffix := ""
		prefix := c.ClientName + "-"
		found := false
		opts := &couchdb.FindRequest{
			UseIndex: "by-client-name",
			Selector: mango.StartWith("client_name", c.ClientName),
		}
		rows := couchdb.FindDocs(i, consts.OAuthClients, opts)
		n := 1
		for {
			done, err := rows.Next()
			if done || couchdb.IsNoDatabaseError(err) {
				break
			}
			if err != nil {
				return internalError
			}
			var r *Client
			if err = rows.ScanDoc(&r); err != nil {
				return internalError
			}
			name := r.ClientName
			if name == c.ClientName {
				found = true
				continue
			}
			if !strings.HasPrefix(name, prefix) {
				continue
			}
			var m int
			m, err = strconv.Atoi(name[len(prefix):])
			if err == nil && m > n {
				n = m
			}
		}
		if found {
			suffix = strconv.Itoa(n + 1)
		}
		if suffix != "" {
			c.ClientName = c.ClientName + "-" + suffix
		}
	}

	c.CouchID = ""
	c.CouchRev = ""
	c.ClientID = ""
	secret := crypto.GenerateRandomBytes(ClientSecretLen)
	c.ClientSecret = string(crypto.Base64Encode(secret))
	c.SecretExpiresAt = 0
	c.RegistrationToken = ""
	c.GrantTypes = []string{"authorization_code", "refresh_token"}
	c.ResponseTypes = []string{"code"}

	err := couchdb.CreateDoc(i, c)
	if err != nil {
		return internalError
	}

	c.RegistrationToken, err = crypto.NewJWT(i.OAuthSecret, jwt.StandardClaims{
		Audience: permissions.RegistrationTokenAudience,
		Issuer:   i.Domain,
		IssuedAt: time.Now().Unix(),
		Subject:  c.CouchID,
	})
	if err != nil {
		i.Logger().Errorf("[oauth] Failed to create the registration access token: %s", err)
		return internalError
	}

	c.TransformIDAndRev()
	return nil
}

// Update will update the client metadata
func (c *Client) Update(i *instance.Instance, old *Client) *ClientRegistrationError {
	if c.ClientID != old.CouchID {
		return &ClientRegistrationError{
			Code:        http.StatusBadRequest,
			Error:       "invalid_client_id",
			Description: "client_id is mandatory",
		}
	}

	if err := c.checkMandatoryFields(i); err != nil {
		return err
	}

	switch c.ClientSecret {
	case "":
		c.ClientSecret = old.ClientSecret
	case old.ClientSecret:
		secret := crypto.GenerateRandomBytes(ClientSecretLen)
		c.ClientSecret = string(crypto.Base64Encode(secret))
	default:
		return &ClientRegistrationError{
			Code:        http.StatusBadRequest,
			Error:       "invalid_client_secret",
			Description: "client_secret is invalid",
		}
	}

	c.CouchID = old.CouchID
	c.CouchRev = old.CouchRev
	c.ClientName = old.ClientName
	c.ClientID = ""
	c.SecretExpiresAt = 0
	c.RegistrationToken = ""
	c.GrantTypes = []string{"authorization_code", "refresh_token"}
	c.ResponseTypes = []string{"code"}

	if err := couchdb.UpdateDoc(i, c); err != nil {
		return internalError
	}

	c.TransformIDAndRev()
	return nil
}

// Delete is a function that unregister a client
func (c *Client) Delete(i *instance.Instance) *ClientRegistrationError {
	if err := couchdb.DeleteDoc(i, c); err != nil {
		return internalError
	}
	return nil
}

// AcceptRedirectURI returns true if the given URI matches the registered
// redirect_uris
func (c *Client) AcceptRedirectURI(u string) bool {
	for _, uri := range c.RedirectURIs {
		if u == uri {
			return true
		}
	}
	return false
}

// CreateJWT returns a new JSON Web Token for the given instance and audience
func (c *Client) CreateJWT(i *instance.Instance, audience, scope string) (string, error) {
	token, err := crypto.NewJWT(i.OAuthSecret, permissions.Claims{
		StandardClaims: jwt.StandardClaims{
			Audience: audience,
			Issuer:   i.Domain,
			IssuedAt: crypto.Timestamp(),
			Subject:  c.CouchID,
		},
		Scope: scope,
	})
	if err != nil {
		i.Logger().Errorf("[oauth] Failed to create the %s token: %s", audience, err)
	}
	return token, err
}

// ValidToken checks that the JWT is valid and returns the associate claims
// It is expected to be used for registration token and refresh token, and
// it doesn't check when they were issued as they don't expire.
func (c *Client) ValidToken(i *instance.Instance, audience, token string) (permissions.Claims, bool) {
	claims := permissions.Claims{}
	if token == "" {
		return claims, false
	}
	keyFunc := func(token *jwt.Token) (interface{}, error) {
		return i.OAuthSecret, nil
	}
	if err := crypto.ParseJWT(token, keyFunc, &claims); err != nil {
		i.Logger().Errorf("[oauth] Failed to verify the %s token: %s", audience, err)
		return claims, false
	}
	// Note: the refresh and registration tokens don't expire, no need to check its issue date
	if claims.Audience != audience {
		i.Logger().Errorf("[oauth] Unexpected audience for %s token: %s", audience, claims.Audience)
		return claims, false
	}
	if claims.Issuer != i.Domain {
		i.Logger().Errorf("[oauth] Expected %s issuer for %s token, but was: %s", audience, i.Domain, claims.Issuer)
		return claims, false
	}
	if claims.Subject != c.CouchID {
		i.Logger().Errorf("[oauth] Expected %s subject for %s token, but was: %s", audience, c.CouchID, claims.Subject)
		return claims, false
	}
	return claims, true
}

var (
	_ couchdb.Doc = &Client{}
)
