package web

import (
	"bytes"
	"crypto/sha256"
	"io"
	"io/ioutil"
	"net/http"
	"testing"
	"time"

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

	var uid uint64 = 12345

	hawkH := NewHawkHandler(EchoHandler, []string{"sekret"})
	tok := testtoken(hawkH.secrets[0], uid)

	req, _ := hawkrequest("GET", syncurl(uid, "info/collections"), tok)
	resp := sendrequest(req, hawkH)
	assert.Equal(t, http.StatusOK, resp.Code)
}

func TestHawkMultiSecrets(t *testing.T) {
	t.Parallel()

	var uid uint64 = 12345
	hawkH := NewHawkHandler(EchoHandler, []string{"one", "two", "three"})

	for _, secret := range hawkH.secrets {
		tok := testtoken(secret, uid)
		req, _ := hawkrequest("GET", syncurl(uid, "info/collections"), tok)
		resp := sendrequest(req, hawkH)
		if assert.Equal(t, http.StatusOK, resp.Code) {
			return
		}
	}
}

// TestHawkAuthPOST tests if the payload validation
func TestHawkAuthPOST(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)

	var uid uint64 = 12345
	hawkH := NewHawkHandler(EchoHandler, []string{"sekret"})

	tok := testtoken(hawkH.secrets[0], uid)

	payload := "JUST A BUNCH OF DATA"
	body := bytes.NewBufferString(payload)

	req, _ := hawkrequestbody("POST", "/anything", tok, "text/plain", body)
	resp := sendrequest(req, hawkH)
	assert.Equal(http.StatusOK, resp.Code)

	// make sure the payload made it through unscathed to EchoHandler
	assert.Equal(payload, resp.Body.String())
}

func TestHawkNonceCheckFunc(t *testing.T) {
	assert := assert.New(t)
	hawkH := NewHawkHandler(EchoHandler, []string{"sekret"})

	// check ts within 1min
	creds0 := &hawk.Credentials{ID: "hello"}
	ts1 := time.Now().Add(61 * time.Second)
	assert.False(hawkH.hawkNonceNotFound("t0", ts1, creds0))
	ts2 := time.Now().Add(-61 * time.Second)
	assert.False(hawkH.hawkNonceNotFound("t1", ts2, creds0))

	// check replay
	creds1 := &hawk.Credentials{ID: "bacon"}
	ts := time.Now()
	assert.True(hawkH.hawkNonceNotFound("t2", ts, creds1))
	assert.False(hawkH.hawkNonceNotFound("t2", ts, creds1))
}

func TestHawkAuthNonceRequest(t *testing.T) {
	hawkH := NewHawkHandler(EchoHandler, []string{"sekret"})

	var uid uint64 = 12345

	tok := testtoken(hawkH.secrets[0], uid)
	req, _ := hawkrequest("GET", syncurl(uid, "info/collections"), tok)
	resp := sendrequest(req, hawkH)
	assert.Equal(t, http.StatusOK, resp.Code)

	// send it again
	resp2 := sendrequest(req, hawkH)
	assert.NotEqual(t, http.StatusOK, resp2.Code)
}

func BenchmarkHawkAuth(b *testing.B) {
	hawkH := NewHawkHandler(EchoHandler, []string{"sekret"})
	for i := 0; i < b.N; i++ {
		tok := testtoken(hawkH.secrets[0], uint64(i))
		req, _ := hawkrequest("GET", "/", tok)
		sendrequest(req, hawkH)
	}
}
