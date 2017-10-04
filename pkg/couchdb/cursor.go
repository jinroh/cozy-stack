package couchdb

import (
	"encoding/json"
	"net/url"
	"strconv"
)

// A Cursor holds a reference to a page in a couchdb View
type Cursor interface {
	HasMore() bool
	ToQueryParams() url.Values
	applyTo(req *ViewRequest)
	updateFrom([]*row) Cursor
}

type (
	skipCursor struct {
		done  bool // True if there is no more result after last fetch
		Limit int  // Maximum number of items retrieved from a request
		Skip  int  // Skip is the number of elements to start from
	}

	keyCursor struct {
		done      bool        // True if there is no more result after last fetch
		Limit     int         // Maximum number of items retrieved from a request
		NextKey   interface{} // NextKey & NextDocID contains a reference to the document
		NextDocID string      // right after the last fetched one
	}
)

// NewKeyCursor returns a new key based Cursor pointing to
// the given start_key & startkey_docid
func NewKeyCursor(limit int, key interface{}, id string) Cursor {
	return &keyCursor{
		Limit:     limit,
		NextKey:   key,
		NextDocID: id,
	}
}

// NewSkipCursor returns a new skip based Cursor pointing to
// the page after skip items
func NewSkipCursor(limit, skip int) Cursor {
	return &skipCursor{
		Limit: limit,
		Skip:  skip,
	}
}

func (c *skipCursor) applyTo(req *ViewRequest) {
	if c.Skip != 0 {
		req.Skip = c.Skip
	}
	if c.Limit != 0 {
		req.Limit = c.Limit + 1
	}
}

func (c *skipCursor) updateFrom(rows []*row) Cursor {
	l := len(rows)
	newCursor := &skipCursor{}
	newCursor.done = l <= c.Limit
	newCursor.Skip = c.Skip + c.Limit
	return newCursor
}

func (c *skipCursor) HasMore() bool { return !c.done }

func (c *skipCursor) ToQueryParams() url.Values {
	if !c.HasMore() {
		return url.Values{}
	}
	return url.Values{
		"page[limit]": {strconv.Itoa(c.Limit)},
		"page[skip]":  {strconv.Itoa(c.Skip)},
	}
}

func (c *keyCursor) applyTo(req *ViewRequest) {
	if c.NextKey != "" && c.NextKey != nil {
		if req.Key != nil && req.StartKey == nil {
			req.StartKey = req.Key
			req.EndKey = req.Key
			req.InclusiveEnd = true
			req.Key = nil
		}
		req.StartKey = c.NextKey
		if c.NextDocID != "" {
			req.StartKeyDocID = c.NextDocID
		}
	}
	if c.Limit != 0 {
		req.Limit = c.Limit + 1
	}
}

func (c *keyCursor) updateFrom(rows []*row) Cursor {
	l := len(rows)
	done := l <= c.Limit
	newCursor := &keyCursor{}
	newCursor.Limit = c.Limit
	newCursor.done = done
	if done || l == 0 {
		newCursor.NextKey = nil
		newCursor.NextDocID = ""
	} else {
		newCursor.NextKey = rows[l-1].Key
		newCursor.NextDocID = rows[l-1].ID
	}
	return newCursor
}

func (c *keyCursor) HasMore() bool { return !c.done }

func (c *keyCursor) ToQueryParams() url.Values {
	if !c.HasMore() {
		return url.Values{}
	}
	cursor, err := json.Marshal([]interface{}{c.NextKey, c.NextDocID})
	if err != nil {
		return url.Values{}
	}
	return url.Values{
		"page[limit]":  {strconv.Itoa(c.Limit)},
		"page[cursor]": {string(cursor)},
	}
}
