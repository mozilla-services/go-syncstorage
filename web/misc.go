package web

import (
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strconv"

	log "github.com/Sirupsen/logrus"
	"github.com/gorilla/mux"
	"github.com/mostlygeek/go-syncstorage/syncstorage"
	"github.com/pkg/errors"
)

var (
	uidregex *regexp.Regexp
)

func init() {
	uidregex = regexp.MustCompile(`/1\.5/([0-9]+)`)
}

// extractUID extracts the UID from the path in http.Request
func extractUID(path string) string {
	matches := uidregex.FindStringSubmatch(path)
	if len(matches) > 0 {
		return matches[1]
	} else {
		return ""
	}
}

// used to massage post results into JSON
// the client expects
type PostResults struct {
	Modified string              `json:"modified"`
	Success  []string            `json:"success"`
	Failed   map[string][]string `json:"failed"`
}

type parseError struct {
	bId   string
	field string
	msg   string
}

func (e parseError) Error() string {
	return fmt.Sprintf("Could not parse field %s: %s", e.field, e.msg)
}

// parseIntoBSO takes JSON and turns into a syncstorage.PutBSOInput
func parseIntoBSO(jsonData json.RawMessage, bso *syncstorage.PutBSOInput) *parseError {
	// make sure JSON BSO data *only* has the keys that are allowed
	var bkeys map[string]json.RawMessage
	err := json.Unmarshal(jsonData, &bkeys)

	if err != nil {
		return &parseError{field: "-", msg: "Could not parse into object"}
	} else {
		for k, _ := range bkeys {
			switch k {
			case "id", "payload", "ttl", "sortindex":
				// it's ok
			case "modified":
				// to pass the python test_meta_global_sanity functional test
			default:
				return &parseError{field: k, msg: "invalid field"}
			}
		}
	}

	var bId string

	// check to make sure values are appropriate
	if r, ok := bkeys["id"]; ok {
		err := json.Unmarshal(r, &bId)
		if err != nil {
			return &parseError{field: "id", msg: "Invalid format"}
		} else {
			bso.Id = bId
		}
	}

	if r, ok := bkeys["payload"]; ok {
		var payload string
		err := json.Unmarshal(r, &payload)
		if err != nil {
			return &parseError{bId: bId, field: "payload", msg: "Invalid format"}
		} else {
			bso.Payload = &payload
		}
	}

	if r, ok := bkeys["ttl"]; ok {
		var ttl int
		err := json.Unmarshal(r, &ttl)
		if err != nil {
			return &parseError{bId: bId, field: "ttl", msg: "Invalid format"}
		} else {
			bso.TTL = &ttl
		}
	}

	if r, ok := bkeys["sortindex"]; ok {
		var sortindex int
		err := json.Unmarshal(r, &sortindex)
		if err != nil {
			return &parseError{bId: bId, field: "sortindex", msg: "Invalid format"}
		} else {
			bso.SortIndex = &sortindex
		}
	}

	return nil
}

// extractBsoId tries to extract and validate a BSO id in the path
func extractBsoId(r *http.Request) (bId string, ok bool) {
	bId, ok = mux.Vars(r)["bsoId"]
	if !ok {
		return
	}

	ok = syncstorage.BSOIdOk(bId)
	return
}

// extractBsoIdFail is like extraBsoId *and* has the sideeffect of writing an JSON error to w
func extractBsoIdFail(w http.ResponseWriter, r *http.Request) (bId string, ok bool) {
	bId, ok = extractBsoId(r)
	if !ok {
		JSONError(w, "Invalid bso ID", http.StatusNotFound)
	}
	return
}

// InternalError produces an HTTP 500 error, basically means a bug in the system
func InternalError(w http.ResponseWriter, r *http.Request, err error) {

	log.WithFields(log.Fields{
		"cause":  errors.Cause(err).Error(),
		"method": r.Method,
		"path":   r.URL.Path,
	}).Errorf("HTTP Error: %s", err.Error())
	JSONError(w, err.Error(), http.StatusInternalServerError)
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

// JsonNewline returns data as newline separated or as a single
// json array
func JsonNewline(w http.ResponseWriter, r *http.Request, val interface{}) {
	if r.Header.Get("Accept") == "application/newlines" {
		NewLine(w, r, val)
	} else {
		JSON(w, r, val)
	}
}

type jsonerr struct {
	Err string `json:"err"`
}

func JSONError(w http.ResponseWriter, msg string, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	js, _ := json.Marshal(jsonerr{msg})
	w.Write(js)
}

// ConvertTimestamp converts the sync decimal time in seconds to
// a time in milliseconds
func ConvertTimestamp(ts string) (int, error) {

	f, err := strconv.ParseFloat(ts, 64)
	if err != nil {
		return 0, err
	}

	return int(f * 1000), nil
}

// AcceptHeaderOk checks the Accept header is
// application/json or application/newlines. If not, it will write an error and
// return false
func AcceptHeaderOk(w http.ResponseWriter, r *http.Request) bool {
	accept := r.Header.Get("Accept")
	switch {
	case accept == "":
		r.Header.Set("Accept", "application/json")
		return true
	case accept != "application/json" && accept != "application/newlines":
		http.Error(w, http.StatusText(http.StatusNotAcceptable), http.StatusNotAcceptable)
		return false
	default:
		return true
	}
}

// OKResponse writes a 200 response with a simple string body
func OKResponse(w http.ResponseWriter, s string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, s)
}
