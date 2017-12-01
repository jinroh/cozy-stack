package intents

import (
	"github.com/cozy/cozy-stack/pkg/apps"
	"github.com/cozy/cozy-stack/pkg/consts"
	"github.com/cozy/cozy-stack/pkg/couchdb"
	"github.com/cozy/cozy-stack/pkg/instance"
)

// Service is a struct for an app that can serve an intent
type Service struct {
	Slug string `json:"slug"`
	Href string `json:"href"`
}

// Intent is a struct for a call from a client-side app to have another app do
// something for it
type Intent struct {
	IID         string    `json:"_id,omitempty"`
	IRev        string    `json:"_rev,omitempty"`
	Action      string    `json:"action"`
	Type        string    `json:"type"`
	Permissions []string  `json:"permissions"`
	Client      string    `json:"client"`
	Services    []Service `json:"services"`
}

// ID is used to implement the couchdb.Doc interface
func (in *Intent) ID() string { return in.IID }

// Rev is used to implement the couchdb.Doc interface
func (in *Intent) Rev() string { return in.IRev }

// DocType is used to implement the couchdb.Doc interface
func (in *Intent) DocType() string { return consts.Intents }

// Clone implements couchdb.Doc
func (in *Intent) Clone() couchdb.Doc {
	cloned := *in
	cloned.Permissions = make([]string, len(in.Permissions))
	copy(cloned.Permissions, in.Permissions)
	cloned.Services = make([]Service, len(in.Services))
	copy(cloned.Services, in.Services)
	return &cloned
}

// SetID is used to implement the couchdb.Doc interface
func (in *Intent) SetID(id string) { in.IID = id }

// SetRev is used to implement the couchdb.Doc interface
func (in *Intent) SetRev(rev string) { in.IRev = rev }

// Save will persist the intent in CouchDB
func (in *Intent) Save(instance *instance.Instance) error {
	if in.ID() != "" {
		return couchdb.UpdateDoc(instance, in)
	}
	return couchdb.CreateDoc(instance, in)
}

// GenerateHref creates the href where the service can be called for an intent
func (in *Intent) GenerateHref(instance *instance.Instance, slug, target string) string {
	u := instance.SubDomain(slug)
	if len(target) > 0 {
		u.Path = target
	}
	u.RawQuery = "intent=" + in.ID()
	return u.String()
}

// FillServices looks at all the application that can answer this intent
// and save them in the services field
func (in *Intent) FillServices(instance *instance.Instance) error {
	webapps, err := apps.ListWebapps(instance)
	if err != nil {
		return err
	}
	for _, man := range webapps {
		if intent := man.FindIntent(in.Action, in.Type); intent != nil {
			href := in.GenerateHref(instance, man.Slug(), intent.Href)
			service := Service{Slug: man.Slug(), Href: href}
			in.Services = append(in.Services, service)
		}
	}
	return nil
}
