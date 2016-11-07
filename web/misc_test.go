package web

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mozilla-services/go-syncstorage/syncstorage"
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

func TestAcceptHeaderOk(t *testing.T) {

	// test headers are acceptable
	acceptable := []string{
		"application/json",
		"application/newlines",
	}

	for _, contentType := range acceptable {
		w := httptest.NewRecorder()
		req, _ := http.NewRequest("GET", "/", nil)
		req.Header.Set("Accept", contentType)
		assert.True(t, AcceptHeaderOk(w, req), contentType)
	}

	// test default rewrites
	defaults := []string{
		"",
		"*/*",
		"application/*",
		"*/json",

		// https://github.com/mostlygeek/go-syncstorage/issues/85
		"text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
	}

	for _, contentType := range defaults {
		w := httptest.NewRecorder()
		req, _ := http.NewRequest("GET", "/", nil)
		req.Header.Set("Accept", contentType)
		assert.True(t, AcceptHeaderOk(w, req), contentType)
		assert.Equal(t, req.Header.Get("Accept"), "application/json")
	}

	// test 406 StatusNotAcceptable
	notAcceptable := []string{
		"x/yy",
		"text/html",
		"application/xhtml+xml",
		"application/xml",
		"text/html,application/xhtml+xml,application/xml;q=0.9",
	}

	for _, contentType := range notAcceptable {
		w := httptest.NewRecorder()
		req, _ := http.NewRequest("GET", "/", nil)
		req.Header.Set("Accept", contentType)
		assert.False(t, AcceptHeaderOk(w, req), contentType)
		assert.Equal(t, http.StatusNotAcceptable, w.Code)
	}

}

func TestJSONError(t *testing.T) {
	assert := assert.New(t)

	{
		w := httptest.NewRecorder()
		JSONError(w, "testing", http.StatusBadRequest)
		assert.Equal(http.StatusBadRequest, w.Code)
		assert.Equal(`{"err":"testing"}`, w.Body.String())
	}

	{ // make sure string is properly encoded
		w := httptest.NewRecorder()
		message := ` this " is a " tough
		  string for json. ''
		`
		JSONError(w, message, http.StatusNotAcceptable)
		assert.Equal(http.StatusNotAcceptable, w.Code)
		assert.Equal(`{"err":" this \" is a \" tough\n\t\t  string for json. ''\n\t\t"}`, w.Body.String())
	}
}

func TestGetMediaType(t *testing.T) {
	assert := assert.New(t)
	assert.Equal("text/plain", getMediaType("text/plain"))
	assert.Equal("application/json", getMediaType("application/json"))
	assert.Equal("application/json", getMediaType("application/json; a=b ; c=d"))
	assert.Equal("", getMediaType("this is invalid:"))
}

func BenchmarkNewLine(b *testing.B) {
	writer := httptest.NewRecorder()

	bso := syncstorage.BSO{
		Id:       "BSO_id",
		Modified: 1000020,
		Payload: `Just some whatever ordinary playload. This just needs to be
		          of a small length to test things out`,
		SortIndex: 11,
	}

	data := make([]syncstorage.BSO, 100, 100)
	for i := 0; i < len(data); i++ {
		data[i] = bso // copy it a few times
	}

	for i := 0; i < b.N; i++ {
		NewLine(writer, nil, http.StatusOK, data)
		writer.Body.Reset() // clean it out
	}
}
