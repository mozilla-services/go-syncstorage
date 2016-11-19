package web

import (
	"bytes"
	"crypto/sha256"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/mozilla-services/go-syncstorage/token"
	"github.com/pkg/errors"
	"github.com/willf/bloom"
	"go.mozilla.org/hawk"
)

var (
	EmptyData       = []byte{}
	ErrTokenInvalid = errors.New("Token is invalid")
	ErrTokenExpired = errors.New("Token is expired")
)

type HawkHandler struct {
	handler http.Handler

	// bloom filters for nonce checking
	bloomPrev *bloom.BloomFilter
	bloomNow  *bloom.BloomFilter

	// use to control rotation of bloom filters
	bloomHalflife time.Duration
	lastRotate    time.Time
	bloomLock     sync.Mutex

	secrets []string
}

func NewHawkHandler(handler http.Handler, secrets []string) *HawkHandler {
	// the m value for the bloom filter is likely larger than
	// we need. It figures 60,000 requests/minute * 50 = 3,000,000 bits
	// or ~2.8MB. The code rotates between two of them so about 5.6MB
	// of memory for nonce checking.

	m := uint(1000 * 60 * 50)
	return &HawkHandler{
		handler:       handler,
		secrets:       secrets,
		bloomPrev:     bloom.New(m, 5),
		bloomNow:      bloom.New(m, 5),
		bloomHalflife: 30 * time.Second,
		lastRotate:    time.Now(),
	}
}

func (h *HawkHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {

	// Step 0: Create a session context. Added since sendRequestProblem
	// stores errors to pass around in session.ErrorResult and if we 4xx
	// in HawkHandler the error won't be reported by the LoggingHandler
	var session *Session
	if ctxSession, ok := SessionFromContext(r.Context()); !ok {
		session = &Session{}
		// replace the context
		r = r.WithContext(NewSessionContext(r.Context(), session))
	} else {
		session = ctxSession
	}

	// Step 1: Ensure the Hawk header is OK. Use ParseRequestHeader
	// so the token does not have to be parsed twice to extract
	// the UID from it.
	//
	// Important: Hawk errors results in StatusBadRequest (HTTP 400). A StatusUnauthorized (HTTP 401)
	// causes clients to fetch new tokens from the tokenserver. In practice most hawk errors
	// can not be resolved with a new token, e.g: time skew too high, nonce replay, etc.
	// there's no sense putting unnecessary load on the token service.
	auth, err := hawk.NewAuthFromRequest(r, nil, h.hawkNonceNotFound)
	if err != nil {
		if e, ok := err.(hawk.AuthFormatError); ok {
			sendRequestProblem(w, r, http.StatusForbidden,
				errors.Errorf("Hawk: Malformed hawk header, field: %s, err: %s", e.Field, e.Err))
		} else if authError, ok := err.(hawk.AuthError); ok {
			w.Header().Set("WWW-Authenticate", "Hawk")
			switch authError {
			case hawk.ErrReplay: // log the replay'd nonce
				authInfo, _ := hawk.ParseRequestHeader(r.Header.Get("Authorization"))
				sendRequestProblem(w, r, http.StatusForbidden,
					errors.Errorf("Hawk: Replay nonce=%s", authInfo.Nonce))
			case hawk.ErrNoAuth:
				// send a 401 for no Authorization header issues to force clients to
				// fetch a new token. See https://bugzilla.mozilla.org/show_bug.cgi?id=1318799
				// reasons.
				sendRequestProblem(w, r, http.StatusUnauthorized, errors.Wrap(err, "Hawk: AuthError"))
			default:
				sendRequestProblem(w, r, http.StatusForbidden, errors.Wrap(err, "Hawk: AuthError"))
			}
		} else {
			sendRequestProblem(w, r, http.StatusForbidden, errors.Wrap(err, "Hawk: Unknown Error"))
		}
		return
	}

	// Step 2: Extract the Token
	var (
		parsedToken token.Token
		tokenError  error = ErrTokenInvalid
	)

	for _, secret := range h.secrets {
		parsedToken, tokenError = token.ParseToken([]byte(secret), auth.Credentials.ID)
		if tokenError == nil { // found the right secret
			break
		}
	}

	if tokenError != nil {
		sendRequestProblem(w, r, http.StatusUnauthorized, errors.Wrap(tokenError, "Hawk: Invalid token"))
		return
	} else {
		// required to these manually so the auth.Valid()
		// check has all the information it needs later
		auth.Credentials.Key = parsedToken.DerivedSecret
		auth.Credentials.Hash = sha256.New
	}

	// Step 3: Make sure it's valid...
	if err := auth.Valid(); err != nil {
		w.Header().Set("WWW-Authenticate", "Hawk")

		// special case, want to see how far client clocks are off
		if err == hawk.ErrTimestampSkew {
			skew := auth.ActualTimestamp.Sub(auth.Timestamp)
			sendRequestProblem(w, r, http.StatusForbidden, errors.Errorf("Hawk: timestamp skew too large %0.3f", skew.Seconds()))
		} else {
			sendRequestProblem(w, r, http.StatusForbidden, errors.Wrap(err, "Hawk: auth invalid"))
		}
		return
	}

	// Step 4: Make sure token UID matches path UID for sync paths
	if strings.HasPrefix(r.URL.Path, "/1.5/") {
		tokenUid := parsedToken.Payload.UidString()
		pathUID := extractUID(r.URL.Path)
		if tokenUid != pathUID {
			// Ref: https://bugzilla.mozilla.org/show_bug.cgi?id=1304137
			// a strange series of events can cause clients to use a token that doesn't
			// match the URL. Sending a 401 should cause clients to abort, fetch a new token
			// and regenerate the correct URL
			sendRequestProblem(w, r, http.StatusUnauthorized,
				errors.Errorf("Hawk: UID in URL (%s) != Token UID (%s)", pathUID, tokenUid))
			return
		}
	}

	// Step 5: Validate the payload hash if it exists
	if auth.Hash != nil {
		if r.Header.Get("Content-Type") == "" {
			sendRequestProblem(w, r, http.StatusBadRequest,
				errors.New("Hawk: Content-Type required"))
			return
		}

		// read and replace io.Reader
		content, err := ioutil.ReadAll(r.Body)
		if err != nil {
			sendRequestProblem(w, r, http.StatusBadRequest,
				errors.Wrap(err, "Hawk: Could not read request body"))
			return
		}

		r.Body = ioutil.NopCloser(bytes.NewReader(content))
		pHash := auth.PayloadHash(r.Header.Get("Content-Type"))
		pHash.Sum(content)
		if !auth.ValidHash(pHash) {
			w.Header().Set("WWW-Authenticate", "Hawk")
			sendRequestProblem(w, r, http.StatusForbidden,
				errors.New("Hawk: payload hash invalid"))
			return
		}
	}

	// Step 6: Update the session token and pass it on
	session.Token = parsedToken.Payload
	h.handler.ServeHTTP(w, r)

}

func (h *HawkHandler) hawkNonceNotFound(nonce string, t time.Time, creds *hawk.Credentials) bool {
	// From the Docs:
	//   The nonce is generated by the client, and is a string unique across all
	//   requests with the same timestamp and key identifier combination.
	var key string
	if creds != nil {
		key = nonce + t.String() + creds.ID
	} else {
		key = nonce + t.String()
	}

	// rotate the blooms?
	h.bloomLock.Lock()
	now := time.Now()
	if now.Sub(h.lastRotate) > h.bloomHalflife {
		h.bloomNow, h.bloomPrev = h.bloomPrev, h.bloomNow // switcheroo
		h.bloomNow.ClearAll()
		h.lastRotate = now
	}
	h.bloomLock.Unlock()

	if h.bloomNow.TestString(key) || h.bloomPrev.TestString(key) {
		return false
	}

	h.bloomNow.AddString(key)
	return true
}
