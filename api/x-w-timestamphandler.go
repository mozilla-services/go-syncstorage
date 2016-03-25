package api

import (
	"net/http"

	"github.com/mostlygeek/go-syncstorage/syncstorage"
)

// the X-Weave-Timestamp should be added to all requests
// it indicates the server time in the weird decimal timestamp
// sync 1.5 uses
func XWeaveTimestampHandler(h http.Handler) http.Handler {
	return xWeaveTimestampHandler{h}
}

type xWeaveTimestampHandler struct {
	handler http.Handler
}

func (x xWeaveTimestampHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	// have to add it before any Write() or WriteHeader() calls
	// if this is a problem by writing it too early this
	// will be refactor into the actual context handlers
	// so it is at late as possible
	w.Header().Set("X-Weave-Timestamp",
		syncstorage.ModifiedToString(syncstorage.Now()))

	x.handler.ServeHTTP(w, req)
}
