package web

import (
	"bytes"
	"crypto/sha256"
	"io"
	"io/ioutil"
	"mime"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/mozilla-services/go-syncstorage/syncstorage"
	"github.com/mozilla-services/go-syncstorage/token"
	"github.com/stretchr/testify/assert"
	"go.mozilla.org/hawk"
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
		mediaType, _, _ := mime.ParseMediaType(contentType)
		h := auth.PayloadHash(mediaType)
		h.Write(content)
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
		Uid:      uid,
		Node:     node,
		Expires:  float64(syncstorage.Now()+60) / 1000,
		Salt:     "pacific",
		FxaUID:   "fxa_" + strconv.FormatUint(uid, 10),
		DeviceId: "device_" + strconv.FormatUint(uid, 10),
	}

	tok, err := token.NewToken([]byte(secret), payload)
	if err != nil {
		panic(err)
	}

	return tok
}

func TestHawkUidMismatchFails(t *testing.T) {
	var uid uint64 = 12345

	hawkH := NewHawkHandler(EchoHandler, []string{"sekret"})
	tok := testtoken(hawkH.secrets[0], uid)

	// provide a different UID in the sync url
	req, _ := hawkrequest("GET", syncurl("67890", "info/collections"), tok)
	resp := sendrequest(req, hawkH)
	assert.Equal(t, http.StatusUnauthorized, resp.Code)
}

// TestHawkNoAuthorizationError401 tests that the server sends a 401 status when
// there is no Authorization header. This is legacy behaviour see bug
// https://bugzilla.mozilla.org/show_bug.cgi?id=1318799
func TestHawkNoAuthorizationError401(t *testing.T) {
	var uid uint64 = 12345

	hawkH := NewHawkHandler(EchoHandler, []string{"sekret"})

	// send an authenticated request
	resp := request("GET", syncurl(uid, "info/collections"), nil, hawkH)
	assert.Equal(t, http.StatusUnauthorized, resp.Code)
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

func TestHawkAuthGET(t *testing.T) {

	var uid uint64 = 12345

	hawkH := NewHawkHandler(EchoHandler, []string{"sekret"})
	tok := testtoken(hawkH.secrets[0], uid)

	req, _ := hawkrequest("GET", syncurl(uid, "info/collections"), tok)
	resp := sendrequest(req, hawkH)
	assert.Equal(t, http.StatusOK, resp.Code)
}

// TestHawkAuthPOST tests if the payload validation
func TestHawkAuthPOST(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)

	var uid uint64 = 12345
	hawkH := NewHawkHandler(EchoHandler, []string{"sekret"})

	tok := testtoken(hawkH.secrets[0], uid)

	payload := "Thank you for flying Hawk"
	body := bytes.NewBufferString(payload)

	req, _ := hawkrequestbody("POST", syncurl(uid, "storage/collections/boom"), tok, "text/plain;charset=utf-8", body)

	assert.Contains(req.Header.Get("Authorization"),
		`hash="Yi9LfIIFRtBEPt74PVmbTF/xVAwPn7ub15ePICfgnuY="`,
		"payload hash value invalid",
	)

	resp := sendrequest(req, hawkH)
	assert.Equal(http.StatusOK, resp.Code)

	// make sure the payload made it through unscathed to EchoHandler
	assert.Equal(payload, resp.Body.String())
}

func TestHawkNonceCheckFunc(t *testing.T) {
	assert := assert.New(t)
	hawkH := NewHawkHandler(EchoHandler, []string{"sekret"})

	// check replay
	creds1 := &hawk.Credentials{ID: "bacon"}
	ts := time.Now()
	assert.True(hawkH.hawkNonceNotFound("t2", ts, creds1))
	assert.False(hawkH.hawkNonceNotFound("t2", ts, creds1))
}

func TestHawkBloomRotate(t *testing.T) {
	assert := assert.New(t)
	hawkH := NewHawkHandler(EchoHandler, []string{"sekret"})

	// use a very short halflife to not wait so long
	// by default it is 30 seconds, to allow for 1 minute validity
	// of hawk nonces
	halfLife := 10 * time.Millisecond

	hawkH.bloomHalflife = halfLife
	creds := &hawk.Credentials{ID: "bacon"}

	// test several rotations through
	for i := 0; i < 3; i++ {
		ts := time.Now()

		assert.True(hawkH.hawkNonceNotFound("nonce", ts, creds), "Expected nonce not to be found")

		time.Sleep(halfLife + time.Millisecond) // force a rotation
		assert.False(hawkH.hawkNonceNotFound("nonce", ts, creds), "Expected nonce to be found")

		time.Sleep(halfLife + time.Millisecond) // force another rotation
		assert.True(hawkH.hawkNonceNotFound("nonce", ts, creds), "Expected nonce to be gone")

		assert.False(hawkH.hawkNonceNotFound("nonce", ts, creds), "Expected nonce to be found")
	}
}

func BenchmarkHawkNonceNotFound(b *testing.B) {
	hawkH := NewHawkHandler(EchoHandler, []string{"sekret"})
	creds := &hawk.Credentials{ID: "bacon"}
	ts := time.Now()

	for i := 0; i < b.N; i++ {
		hawkH.hawkNonceNotFound("nonce", ts, creds)
	}
}

func TestHawkReplayNonce(t *testing.T) {
	assert := assert.New(t)
	hawkH := NewHawkHandler(EchoHandler, []string{"sekret"})

	var uid uint64 = 12345

	tok := testtoken(hawkH.secrets[0], uid)
	req1, _ := hawkrequest("GET", syncurl(uid, "info/collections"), tok)
	resp1 := sendrequest(req1, hawkH)
	assert.Equal(http.StatusOK, resp1.Code)

	resp2 := sendrequest(req1, hawkH)
	assert.Equal(http.StatusForbidden, resp2.Code)
	assert.Contains(resp2.Body.String(), "Hawk: Replay nonce=")

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
