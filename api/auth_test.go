package api

import (
	"encoding/base64"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/mostlygeek/go-syncstorage/syncstorage"
	"github.com/mostlygeek/go-syncstorage/token"
	"github.com/stretchr/testify/assert"
)

func makeTestTokenPayload(uid uint64, node string, expires int64) token.TokenPayload {
	return token.TokenPayload{
		Uid:     uid,
		Node:    node,
		Expires: float64(time.Now().Unix() + expires),
	}
}

func TestCredentialStore(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)

	secrets := []string{"a"}
	credStore := &CredentialsStore{secrets: secrets}

	var uid uint64 = 1
	node := "mynode"
	var expires int64 = 10000 // seconds

	payload := makeTestTokenPayload(uid, node, expires)
	token, err := token.NewToken([]byte(secrets[0]), payload)

	creds, err := credStore.CredentialsForKeyIdentifier(token.Token)

	if !assert.NoError(err) {
		return
	}

	assert.NotNil(creds)

	// the token's derived secret
	assert.Equal(token.DerivedSecret, base64.URLEncoding.EncodeToString(creds.Key().Secret))
	assert.Equal(token.Token, creds.Key().Identifier)
}

func TestHawkHeaderFromToken(t *testing.T) {
	assert := assert.New(t)

	tokenSecret := "preshared secret with tokenserver"

	var uid uint64 = 12345
	node := "https://syncnode-12345.services.mozilla.com"
	payload := token.TokenPayload{
		Uid:     uid,
		Node:    node,
		Expires: 1452807004.454294,
		Salt:    "pacific",
	}

	token, err := token.NewToken([]byte(tokenSecret), payload)
	if !assert.NoError(err) {
		return
	}

	r, _ := http.NewRequest("POST", node, strings.NewReader("Thank you for flying Hawk"))
	r.Header.Add("Content-Type", "text/plain")

	var timestamp int64 = 1353832234
	nonce := "j4h3g2"
	ext := "some-app-ext-data"

	hawkHeader, err := HawkHeaderFromToken(r, token, timestamp, nonce, ext)
	if !assert.NoError(err) {
		return
	}

	expected := `Hawk id="eyJTYWx0IjoicGFjaWZpYyIsIlVpZCI6MTIzNDUsIk5vZGUiOiJodHRwczovL3N5bmNub2RlLTEyMzQ1LnNlcnZpY2VzLm1vemlsbGEuY29tIiwiRXhwaXJlcyI6MS40NTI4MDcwMDQ0NTQyOTRlKzA5ffXYBBcOrCUhxwe_UgaLr6kpgYLXlvu62AH0SO-A4bIy", ts="1353832234", nonce="j4h3g2", ext="some-app-ext-data", mac="8T4yEo9iQKfFpNROtdOT//VskWTN9QHqQAoJtutdo1A=", hash="Yi9LfIIFRtBEPt74PVmbTF/xVAwPn7ub15ePICfgnuY="`

	assert.Equal(expected, hawkHeader)
}

// TestHawkAuthentication makes sure we can make a request all the way through
func TestHawkAuthentication(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)

	// Make an HTTP api context to use with Hawk Enabled
	secrets := []string{"token-secret"}
	dir, _ := ioutil.TempDir(os.TempDir(), "sync_storage_api_test")
	dispatch, err := syncstorage.NewDispatch(4, dir, syncstorage.TwoLevelPath, 10)
	if !assert.NoError(err) {
		return
	}
	context, err := NewContext(secrets, dispatch)
	if !assert.NoError(err) {
		return
	}

	// Generate a token to use. make the expiry way in the future...
	var uid uint64 = 12345
	node := "http://syncnode-12345.services.mozilla.com"
	expires, _ := time.Parse("2006-01-02 15:04:05", "2099-11-20 12:00:00") // *WAYY in the future*
	payload := token.TokenPayload{
		Uid:     uid,
		Node:    node,
		Expires: float64(expires.Unix()) + 0.123,
		Salt:    "pacific",
	}

	token, err := token.NewToken([]byte(secrets[0]), payload)
	if !assert.NoError(err) {
		return
	}

	// Generate the Hawk credenials
	nodePath := node + "/1.5/" + strconv.FormatUint(uid, 10) + "/echo-uid"
	req, _ := http.NewRequest("GET", nodePath, nil)
	timestamp := time.Now().Unix()
	nonce := "j4h3g2"
	ext := ""
	hawkHeader, err := HawkHeaderFromToken(req, token, timestamp, nonce, ext)

	if !assert.NoError(err) {
		return
	}

	req.Header.Add("Authorization", hawkHeader)

	// send it through
	resp := httptest.NewRecorder()
	router := NewRouterFromContext(context)
	router.ServeHTTP(resp, req)

	assert.Equal(http.StatusOK, resp.Code)
	assert.Equal(strconv.FormatUint(uid, 10), resp.Body.String())
}
