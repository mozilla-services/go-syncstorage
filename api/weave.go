package api

import (
	"net/http"

	"github.com/mostlygeek/go-syncstorage/syncstorage"
)

// Legacy Weave Error handling
// There are a bunch of legacy weave stuff around and it's just collected
// here for posterity and to not make me sad from looking at
// it all the time

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

// WeaveHandler is a convenient and messy place to capture
// sync 1.5, and legacy weave specific functionality.
// TODO will have to implement http.Hijack()
func WeaveHandler(h http.Handler) http.Handler { return &WeaveWrapperHandler{h} }

type WeaveWrapperHandler struct {
	handler http.Handler
}

func (weave *WeaveWrapperHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {

	wrapper := &weaveWriter{w: w}
	defer wrapper.addXWeaveTimestamp()
	weave.handler.ServeHTTP(wrapper, req)
}

// weaveWriter intercepts the Write() and WriteHeader() calls
// and injects / rewrites responses
type weaveWriter struct {
	http.ResponseWriter

	// original writer
	w http.ResponseWriter

	wroteTS       bool
	wroteHeader   bool
	disableWrites bool // do allow Writes
}

// addXWeaveTimestamp will add the X-Weave-Timestamp as late as possible
// and matching the rules of the sync 1.5 protocol.
func (w *weaveWriter) addXWeaveTimestamp() {
	if w.wroteTS {
		return
	}

	if lm := w.w.Header().Get("X-Last-Modified"); lm != "" {
		w.w.Header().Set("X-Weave-Timestamp", lm)
	} else {
		w.w.Header().Set("X-Weave-Timestamp",
			syncstorage.ModifiedToString(syncstorage.Now()))
	}

	w.wroteTS = true
}

// implement http.ResponseWriter
func (w *weaveWriter) Header() http.Header { return w.w.Header() }
func (w *weaveWriter) Write(b []byte) (int, error) {
	// must be called, before any data writes can be done
	w.addXWeaveTimestamp()

	if w.disableWrites {
		return 0, nil
	}

	return w.w.Write(b)
}

func (w *weaveWriter) WriteHeader(statusCode int) {
	if w.wroteHeader {
		return
	}

	w.wroteHeader = true
	w.addXWeaveTimestamp()

	// Capture 404's and rewrite them to WEAVE_UNKNOWN_ERROR
	// Matches python server's behaviour: https://git.io/vVvTt
	// for passing test_that_404_responses_have_a_json_body python
	// functional test
	if statusCode == http.StatusNotFound && w.Header().Get("Content-Type") != "application/json" {
		w.w.Header().Set("Content-Type", "application/json")
		w.w.WriteHeader(statusCode)
		w.w.Write([]byte(WEAVE_UNKNOWN_ERROR))
		w.disableWrites = true
		return
	}

	w.w.WriteHeader(statusCode)
	return
}
