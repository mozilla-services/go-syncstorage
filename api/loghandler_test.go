package api

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func BenchmarkFindUID(b *testing.B) {
	for i := 0; i < b.N; i++ {
		// test with 10 characters
		findUID("/1.5/0123456789/")
	}
}

func TestFindUID(t *testing.T) {
	assert := assert.New(t)

	tests := map[string]string{
		// successes
		"/1.5/12345/": "12345",

		"/1.5/12345/info/collections":       "12345",
		"/1.5/12345/info/collection_usage":  "12345",
		"/1.5/12345/info/collection_counts": "12345",
		"/1.5/12345/info/quota":             "12345",

		"/1.5/12345/storage/bookmarks":    "12345",
		"/1.5/12345/storage/bookmarks/b1": "12345",

		// failures
		"": "",

		"/1.5/123":    "",
		"/1.5/":       "",
		"/1.5/abc/":   "",
		"/1.5/12abc/": "",
	}

	for path, expect := range tests {
		assert.Equal(expect, findUID(path))
	}
}
