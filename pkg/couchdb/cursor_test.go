package couchdb

import (
	"encoding/json"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStartKeyCursor(t *testing.T) {

	req1 := &ViewRequest{
		Key: []string{"A", "B"},
	}

	c1 := NewKeyCursor(10, []string{"A", "B"}, "last-result-id")

	c1.applyTo(req1)
	assert.Nil(t, req1.Key)
	assert.Equal(t, []string{"A", "B"}, req1.StartKey)
	assert.Equal(t, "last-result-id", req1.StartKeyDocID)
	assert.Equal(t, 11, req1.Limit)

	c2 := NewKeyCursor(3, nil, "")

	rows := []*row{
		{Key: json.RawMessage([]byte(`["A", "B"]`)), ID: "resultA"},
		{Key: json.RawMessage([]byte(`["A", "B"]`)), ID: "resultB"},
		{Key: json.RawMessage([]byte(`["A", "B"]`)), ID: "resultC"},
		{Key: json.RawMessage([]byte(`["A", "B"]`)), ID: "resultD"},
	}

	c3 := c2.updateFrom(rows)
	assert.EqualValues(t, url.Values{
		"page[limit]":  []string{"3"},
		"page[cursor]": []string{`[["A","B"],"resultD"]`},
	}, c3.ToQueryParams())
}
