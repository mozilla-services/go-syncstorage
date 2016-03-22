package api

import (
	"errors"
	"net/http"
	"strconv"
)

// ConvertTimestamp converts the sync decimal time in seconds to
// a time in milliseconds
func ConvertTimestamp(ts string) (int, error) {

	f, err := strconv.ParseFloat(ts, 64)
	if err != nil {
		return 0, err
	}

	return int(f * 1000), nil

}

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
		return
	}

	if modSince != "" {
		ts, err := ConvertTimestamp(modSince)
		if err != nil {
			return 0, X_TS_HEADER_NONE, errors.New("Invalid X-If-Modified-Since")
		}

		return ts, X_IF_MODIFIED_SINCE, nil
	}

	if unmodSince != "" {
		ts, err := ConvertTimestamp(unmodSince)
		if err != nil {
			return 0, X_TS_HEADER_NONE, errors.New("Invalid X-If-Unmodified-Since")
		}

		return ts, X_IF_UNMODIFIED_SINCE, nil
	}

	return 0, X_TS_HEADER_NONE, nil
}
