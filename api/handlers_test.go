package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestJSON(t *testing.T) {
	assert := assert.New(t)
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/", nil)
	req.Header.Set("Accept", "application/json")

	js := []byte(`[{"A":"one", "B":1}, {"A":"two", "B":2}]`)

	var val []struct {
		A string
		B int
	}

	err := json.Unmarshal(js, &val)
	if assert.NoError(err) {
		JSON(w, req, val)
		assert.Equal("application/json", w.HeaderMap.Get("Content-Type"))
		assert.Equal(`[{"A":"one","B":1},{"A":"two","B":2}]`, w.Body.String())
	}

}

func TestNewLine(t *testing.T) {
	assert := assert.New(t)

	// some test data
	var val []struct {
		A string
		B int
	}
	js := []byte(`[{"A":"one", "B":1}, {"A":"two", "B":2}, {"A":"three", "B":3}]`)
	err := json.Unmarshal(js, &val)
	if !assert.NoError(err) {
		return
	}

	// single object
	{
		w := httptest.NewRecorder()
		req, _ := http.NewRequest("GET", "/", nil)
		req.Header.Set("Accept", "application/newlines")
		NewLine(w, req, val[0])
		assert.Equal("application/newlines", w.HeaderMap.Get("Content-Type"))
		expected := `{"A":"one","B":1}` + "\n"
		assert.Equal(expected, w.Body.String())
	}

	// multi-newline
	{
		w := httptest.NewRecorder()
		req, _ := http.NewRequest("GET", "/", nil)
		req.Header.Set("Accept", "application/newlines")
		NewLine(w, req, val)
		assert.Equal("application/newlines", w.HeaderMap.Get("Content-Type"))
		expected := `{"A":"one","B":1}
{"A":"two","B":2}
{"A":"three","B":3}
`
		assert.Equal(expected, w.Body.String())
	}
}

func TestJsonNewline(t *testing.T) {
	assert := assert.New(t)

	// some test data
	var val []struct {
		A string
		B int
	}
	js := []byte(`[{"A":"one", "B":1}, {"A":"two", "B":2}, {"A":"three", "B":3}]`)
	err := json.Unmarshal(js, &val)
	if !assert.NoError(err) {
		return
	}

	// JSON response ok?
	{
		w := httptest.NewRecorder()
		req, _ := http.NewRequest("GET", "/", nil)
		req.Header.Set("Accept", "application/json")
		JsonNewline(w, req, val)
		assert.Equal("application/json", w.HeaderMap.Get("Content-Type"))
		assert.Equal(`[{"A":"one","B":1},{"A":"two","B":2},{"A":"three","B":3}]`, w.Body.String())
	}

	// Newline ok?
	{
		w := httptest.NewRecorder()
		req, _ := http.NewRequest("GET", "/", nil)
		req.Header.Set("Accept", "application/newlines")
		JsonNewline(w, req, val)
		assert.Equal("application/newlines", w.HeaderMap.Get("Content-Type"))
		expected := `{"A":"one","B":1}
{"A":"two","B":2}
{"A":"three","B":3}
`
		assert.Equal(expected, w.Body.String())
	}
}
