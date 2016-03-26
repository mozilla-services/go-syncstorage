package api

// This file contains a bunch of utility funcs for generating
// output and various handlers to reduce boiler plate and
// inject functionality

import (
	"encoding/json"
	"fmt"
	"net/http"

	log "github.com/Sirupsen/logrus"
)

type syncApiHandler func(http.ResponseWriter, *http.Request, string)

// acceptOK checks that the request has an Accept header that is either
// application/json or application/newlines
func acceptOK(h http.HandlerFunc) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		accept := r.Header.Get("Accept")

		// no Accept defaults to JSON
		if accept == "" {
			r.Header.Set("Accept", "application/json")
			h(w, r)
			return
		}

		if accept != "application/json" && accept != "application/newlines" {
			http.Error(w, http.StatusText(http.StatusNotAcceptable), http.StatusNotAcceptable)
		} else {
			h(w, r)
		}
	})
}

// InternalError produces an HTTP 500 error, basically means a bug in the system
func InternalError(w http.ResponseWriter, r *http.Request, err error) {
	log.WithFields(log.Fields{
		"err":    err.Error(),
		"method": r.Method,
		"path":   r.URL.Path,
	}).Errorf("HTTP Error: %s", err.Error())
	http.Error(w,
		http.StatusText(http.StatusInternalServerError),
		http.StatusInternalServerError)
}

// JsonNewline returns data as newline separated or as a single
// json array
func JsonNewline(w http.ResponseWriter, r *http.Request, val interface{}) {
	if r.Header.Get("Accept") == "application/newlines" {
		NewLine(w, r, val)
	} else {
		JSON(w, r, val)
	}
}

// NewLine prints out new line \n separated JSON objects instead of a
// single JSON array of objects
func NewLine(w http.ResponseWriter, r *http.Request, val interface{}) {

	var vals []json.RawMessage
	// make sure we can convert all of it to JSON before
	// trying to make it all newline JSON
	js, err := json.Marshal(val)
	if err != nil {
		InternalError(w, r, err)
		return
	}

	w.Header().Set("Content-Type", "application/newlines")

	// array of objects?
	newlineChar := []byte("\n")
	err = json.Unmarshal(js, &vals)
	if err != nil { // not an array
		w.Write(js)
		w.Write(newlineChar)
	} else {
		for _, raw := range vals {
			w.Write(raw)
			w.Write(newlineChar)
		}

	}
}

func JSON(w http.ResponseWriter, r *http.Request, val interface{}) {
	js, err := json.Marshal(val)
	if err != nil {
		InternalError(w, r, err)
	} else {
		w.Header().Set("Content-Type", "application/json")
		w.Write(js)
	}
}

// OKResponse writes a 200 response with a simple string body
func OKResponse(w http.ResponseWriter, s string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, s)
}

// Legacy Weave Error Handlers
const (
	// old legacy stuff, used to keep compatibility with python/old clients
	// https://github.com/mozilla-services/server-syncstorage/blob/fd3c8b90278cb9944cb224964af6e6dae19c9263/syncstorage/tweens.py#L17-L21

	WEAVE_UNKNOWN_ERROR  = "0"
	WEAVE_ILLEGAL_METH   = "1"  // Illegal method/protocol
	WEAVE_MALFORMED_JSON = "6"  // Json parse failure
	WEAVE_INVALID_WBO    = "8"  // Invalid Weave Basic Object
	WEAVE_OVER_QUOTA     = "14" // User over quota
)

func WeaveInvalidWBOError(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusBadRequest)
	w.Write([]byte(WEAVE_INVALID_WBO))
}
