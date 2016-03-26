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

// timestampWriter wraps a http.ResponseWriter and adds
// the X-Weave-Timestamp header right before a call to Write()
// or WriteHeader(). Since it is not possible to add headers after
// already sending headers or data to the client.
type timestampWriter struct {
	// the original
	w           http.ResponseWriter
	wroteHeader bool
}

// addTS will add the X-Weave-Timestamp
func (t *timestampWriter) addTS() {
	if t.wroteHeader {
		return
	}

	if lm := t.w.Header().Get("X-Last-Modified"); lm != "" {
		t.w.Header().Set("X-Weave-Timestamp", lm)
	} else {
		t.w.Header().Set("X-Weave-Timestamp",
			syncstorage.ModifiedToString(syncstorage.Now()))
	}

	t.wroteHeader = true
}

// implement http.ResponseWriter
func (t *timestampWriter) Header() http.Header { return t.w.Header() }
func (t *timestampWriter) Write(b []byte) (int, error) {
	t.addTS()
	return t.w.Write(b)
}
func (t *timestampWriter) WriteHeader(i int) {
	t.addTS()
	t.w.WriteHeader(i)
	return
}

func (x xWeaveTimestampHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	wrapWriter := &timestampWriter{w: w, wroteHeader: false}
	defer wrapWriter.addTS() // just in case it doesn't get added
	x.handler.ServeHTTP(wrapWriter, req)
}
