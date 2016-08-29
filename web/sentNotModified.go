package web

import (
	"net/http"

	"github.com/mozilla-services/go-syncstorage/syncstorage"
	"github.com/pkg/errors"
)

// The code below is a little weird and inelegant. Basically its purpose is
// to reduce boilerplate in the handlers for dealing with the X-If-Modified-Since and
// X-If-Unmodified-Since header logic from clients
type XModHeader int

const (
	X_TS_HEADER_NONE      XModHeader = iota
	X_IF_MODIFIED_SINCE              // X-If-Modified-Since
	X_IF_UNMODIFIED_SINCE            // X-If-Unmodified
)

// extractModified will extract either the X-Modified-Since or the X-If-Unmodified-Since
// headers from the request
func extractModifiedTimestamp(r *http.Request) (ts int, headerType XModHeader, err error) {

	modSince := r.Header.Get("X-If-Modified-Since")
	unmodSince := r.Header.Get("X-If-Unmodified-Since")

	if modSince != "" && unmodSince != "" {
		return 0, X_TS_HEADER_NONE, errors.New("X-If-Modified-Since and X-If-Unmodified-Since both provided")
	}

	if modSince != "" {
		ts, err := ConvertTimestamp(modSince)
		if err != nil || ts < 0 {
			return 0, X_TS_HEADER_NONE, errors.New("Invalid X-If-Modified-Since")
		}

		return ts, X_IF_MODIFIED_SINCE, nil
	}

	if unmodSince != "" {
		ts, err := ConvertTimestamp(unmodSince)
		if err != nil || ts < 0 {
			return 0, X_TS_HEADER_NONE, errors.New("Invalid X-If-Unmodified-Since")
		}

		return ts, X_IF_UNMODIFIED_SINCE, nil
	}

	return 0, X_TS_HEADER_NONE, nil
}

// sentNotModified will check the provided modified timestamp against
// either the X-If-Modified-Since or X-If-Unmodified-Since and return
// true if it wrote to w
func sentNotModified(w http.ResponseWriter, r *http.Request, modified int) (sentResponse bool) {
	ts, mHeaderType, err := extractModifiedTimestamp(r)
	if err != nil {
		sendRequestProblem(w, r, http.StatusBadRequest, err)
		return true
	}

	switch {
	case mHeaderType == X_IF_MODIFIED_SINCE && modified <= ts:
		w.Header().Set("X-Last-Modified", syncstorage.ModifiedToString(modified))
		sendRequestProblem(w, r, http.StatusNotModified, errors.New("Not Modified"))

		return true
	case mHeaderType == X_IF_UNMODIFIED_SINCE && modified > ts:
		w.Header().Set("X-Last-Modified", syncstorage.ModifiedToString(modified))
		sendRequestProblem(w, r, http.StatusPreconditionFailed,
			errors.Errorf("Condition requires %s, but modified at %s (△ %ss)",
				syncstorage.ModifiedToString(ts),
				syncstorage.ModifiedToString(modified),
				syncstorage.ModifiedToString(ts-modified)))

		return true
	}

	return false
}
