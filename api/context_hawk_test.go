package api

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
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
	return hawkrequestbody(method, urlStr, token, "", nil)
}

func hawkcontext() *Context {
	// context_test.go
	c := makeTestContext()
	c.DisableHawk = false

	return c
}

func hawkrequestbody(
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
		h := auth.PayloadHash(contentType)
		h.Sum(content)
		auth.SetHash(h)
		req.Header.Set("Content-Type", contentType)
	}

	req.Header.Add("Authorization", auth.RequestHeader())
	req.Header.Add("Accept", "application/json")
	return req, auth
}

func testtoken(secret string, uid uint64) token.Token {
	node := "https://syncnode-12345.services.mozilla.com"
	payload := token.TokenPayload{
		Uid:     uid,
		Node:    node,
		Expires: float64(syncstorage.Now()+60) / 1000,
		Salt:    "pacific",
	}

	tok, err := token.NewToken([]byte(secret), payload)
	if err != nil {
		panic(err)
	}

	return tok
}

func TestHawkAuthGET(t *testing.T) {
	t.Parallel()
	context := hawkcontext()

	var uid uint64 = 12345

	tok := testtoken(context.Secrets[0], uid)
	req, _ := hawkrequest("GET", syncurl(uid, "info/collections"), tok)
	resp := sendrequest(req, context)
	assert.Equal(t, http.StatusOK, resp.Code)
}

func TestHawkMultiSecrets(t *testing.T) {
	t.Parallel()
	context := hawkcontext()

	var uid uint64 = 12345

	for _, secret := range context.Secrets {
		tok := testtoken(secret, uid)
		req, _ := hawkrequest("GET", syncurl(uid, "info/collections"), tok)
		resp := sendrequest(req, context)
		if assert.Equal(t, http.StatusOK, resp.Code) {
			return
		}
	}
}

func TestHawkAuthPOST(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)
	context := hawkcontext()

	var uid uint64 = 12345

	tok := testtoken(context.Secrets[0], uid)

	body := bytes.NewBufferString(`[
		{"id":"bso1", "payload": "initial payload", "sortindex": 1, "ttl": 2100000},
		{"id":"bso2", "payload": "initial payload", "sortindex": 1, "ttl": 2100000},
		{"id":"bso3", "payload": "initial payload", "sortindex": 1, "ttl": 2100000}
	]`)

	req, _ := hawkrequestbody("POST", syncurl(uid, "storage/bookmarks"), tok, "application/json", body)
	resp := sendrequest(req, context)
	if !assert.Equal(http.StatusOK, resp.Code, resp.Body.String()) {
		return
	}

	var results PostResults
	jsbody := resp.Body.Bytes()
	err := json.Unmarshal(jsbody, &results)
	if !assert.NoError(err) {
		return
	}
	assert.Len(results.Success, 3)
	assert.Len(results.Failed, 0)

	// look in the DB directly to make sure
	// data was written correctly
	uidstr := strconv.FormatUint(uid, 10)
	cId, _ := context.Dispatch.GetCollectionId(uidstr, "bookmarks")
	for _, bId := range []string{"bso1", "bso2", "bso3"} {
		bso, _ := context.Dispatch.GetBSO(uidstr, cId, bId)
		assert.Equal("initial payload", bso.Payload)
		assert.Equal(1, bso.SortIndex)
	}
}

// TODO test Hawk auth failures
