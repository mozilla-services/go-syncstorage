package api

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	"github.com/mostlygeek/go-syncstorage/token"
	"github.com/mozilla-services/hawk-go"
)

// hawk checks HAWK authentication headers and returns an unauthorized response
// if they are invalid otherwise passes call to syncApiHandler
func (c *Context) hawk(h syncApiHandler) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		// have the ability to disable hawk auth for testing purposes
		// when hawk is disabled we need to pull the uid from the
		// url params... when hawk is enabled the uid comes from the Token sent
		// by the tokenserver
		if c.DisableHawk {
			vars := mux.Vars(r)
			if uid, ok := vars["uid"]; !ok {
				http.Error(w, "do not have a uid to work with", http.StatusBadRequest)
			} else {
				authDebug("Hawk disabled. Using uid: %s", uid)
				h(w, r, uid)
			}

			return
		}

		// Step 1: Ensure the Hawk header is OK. Use ParseRequestHeader
		// so the token does not have to be parsed twice to extract
		// the UID from it
		auth, err := hawk.NewAuthFromRequest(r, nil, nil)
		if err != nil {
			if e, ok := err.(hawk.AuthFormatError); ok {
				http.Error(w,
					fmt.Sprintf("Malformed hawk header, field: %s, err: %s", e.Field, e.Err),
					http.StatusBadRequest)
			} else {
				w.Header().Set("WWW-Authenticate", "Hawk")
				http.Error(w, err.Error(), http.StatusUnauthorized)
			}
			return
		}

		// Step 2: Extract the Token
		var (
			parsedToken token.Token
			tokenError  error = ErrTokenInvalid
		)

		for _, secret := range c.Secrets {
			parsedToken, tokenError = token.ParseToken([]byte(secret), auth.Credentials.ID)
			if err != nil { // wrong secret..
				continue
			}
		}

		if tokenError != nil {
			authDebug("tokenError: %s", tokenError.Error())
			http.Error(w,
				fmt.Sprintf("Invalid token: %s", tokenError.Error()),
				http.StatusBadRequest)
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
			http.Error(w, err.Error(), http.StatusUnauthorized)
			return
		}

		// Step 4: Validate the payload hash if it exists
		if auth.Hash != nil {
			if r.Header.Get("Content-Type") == "" {
				http.Error(w, "Content-Type missing", http.StatusBadRequest)
				return
			}

			// read and replace io.Reader
			content, err := ioutil.ReadAll(r.Body)
			if err != nil {
				http.Error(w, "Could not read request body", http.StatusInternalServerError)
				return
			}

			r.Body = ioutil.NopCloser(bytes.NewReader(content))
			pHash := auth.PayloadHash(r.Header.Get("Content-Type"))
			pHash.Sum(content)
			if !auth.ValidHash(pHash) {
				w.Header().Set("WWW-Authenticate", "Hawk")
				http.Error(w, "Hawk error, payload hash invalid", http.StatusUnauthorized)
				return
			}
		}

		// Step 5: *woot*
		h(w, r, strconv.FormatUint(parsedToken.Payload.Uid, 10))
	})
}
