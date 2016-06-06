package web

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExtractUID(t *testing.T) {

	// test : expected
	paths := map[string]string{
		"/":                                     "",
		"/1.5/abc":                              "",
		"/somepath/123":                         "",
		"/somepath/123/storage/collectionname/": "",
		"123": "",
		"":    "",

		"/1.5/123":                         "123",
		"/1.5/123/info":                    "123",
		"/1.5/123/info/collections":        "123",
		"/1.5/123/storage/collectionname":  "123",
		"/1.5/123/storage/collectionname/": "123",
	}

	for path, expected := range paths {
		assert.Equal(t, expected, extractUID(path))
	}
}
