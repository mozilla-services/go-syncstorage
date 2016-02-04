package api

import (
	"bytes"
	"crypto/sha256"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/mostlygeek/go-syncstorage/syncstorage"
	"github.com/mostlygeek/go-syncstorage/token"
	"github.com/mozilla-services/hawk-go"
	"github.com/stretchr/testify/assert"
)

// tokenrequest generates an *http.Request with Hawk authentication
func hawkrequest(
	method,
	urlStr string,
	token token.Token,
) (*http.Request, *hawk.Auth) {
	return hawkrequestwithbody(method, urlStr, token, "", nil)
}

func hawkrequestwithbody(
	method,
	urlStr string,
	token token.Token,
	contentType string,
	body io.Reader,
) (*http.Request, *hawk.Auth) {
	var (
		content []byte
		err     error
	)

	// make a copy of the data in body
	// if we need to to generate a payload hash later
	if body != nil {
		content, err = ioutil.ReadAll(body)
		if err != nil {
			panic(err)
		}
		body = bytes.NewReader(content)
	}

	req, err := http.NewRequest(method, urlStr, body)
	if err != nil {
		panic(err)
	}

	creds := &hawk.Credentials{
		ID:   token.Token,
		Key:  token.DerivedSecret,
		Hash: sha256.New,
	}

	auth := hawk.NewRequestAuth(req, creds, 0)

	// add in the payload hash
	if len(content) > 0 {
		auth.Hash = auth.PayloadHash(contentType).Sum(content)
	}

	req.Header.Add("Authorization", auth.RequestHeader())
	return req, auth
}

func TestHawkAuthHeader(t *testing.T) {
	assert := assert.New(t)

	c := makeTestContext()
	c.DisableHawk = false

	var uid uint64 = 12345
	node := "https://syncnode-12345.services.mozilla.com"
	payload := token.TokenPayload{
		Uid:     uid,
		Node:    node,
		Expires: float64(syncstorage.Now()+60) / 1000,
		Salt:    "pacific",
	}

	token, err := token.NewToken([]byte(c.Secrets[0]), payload)
	if !assert.NoError(err) {
		return
	}

	url := "http://t/1.5/" + strconv.FormatUint(uid, 10) + "/info/collections"
	req, _ := hawkrequest("GET", url, token)

	resp := httptest.NewRecorder()

	router := NewRouterFromContext(c)
	router.ServeHTTP(resp, req)
	assert.Equal(http.StatusOK, resp.Code)
}
