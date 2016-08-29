package web

import (
	"bytes"
	"encoding/json"
	"fmt"
	"mime"
	"net/http"
	"reflect"
	"regexp"
	"strconv"
	"strings"

	log "github.com/Sirupsen/logrus"
	"github.com/gorilla/mux"
	"github.com/mozilla-services/go-syncstorage/syncstorage"
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
	Batch    string
	Modified int
	Success  []string
	Failed   map[string][]string
}

// MarshalJSON manually creates the JSON string since the modified needs to be
// converted in the python (ugh) timeformat required for sync 1.5. Which means no quotes
func (p *PostResults) MarshalJSON() ([]byte, error) {
	buf := new(bytes.Buffer)

	buf.WriteString(`{"modified":`)
	buf.WriteString(syncstorage.ModifiedToString(p.Modified))
	buf.WriteString(",")
	if len(p.Success) == 0 {
		buf.WriteString(`"success":[]`)
	} else {
		buf.WriteString(`"success":`)
		data, err := json.Marshal(p.Success)
		if err != nil {
			return nil, err
		}
		_, err = buf.Write(data)
		if err != nil {
			return nil, errors.Wrap(err, "Could not encode PostResults.Success")
		}
	}

	buf.WriteString(",")
	if len(p.Failed) == 0 {
		buf.WriteString(`"failed":{}`)
	} else {
		buf.WriteString(`"failed":`)
		data, err := json.Marshal(p.Failed)
		if err != nil {
			return nil, err
		}
		_, err = buf.Write(data)
		if err != nil {
			return nil, errors.Wrap(err, "Could not encode PostResults.Failed")
		}
	}

	if p.Batch != "" {
		buf.WriteString(`,"batch":"`)
		buf.WriteString(p.Batch)
		buf.WriteString(`"`)
	}

	buf.WriteString("}")
	return buf.Bytes(), nil
}

// UnmarshalJSON reverses custom formatting from MarshalJSON
func (p *PostResults) UnmarshalJSON(data []byte) error {
	var tmp struct {
		Modified float64
		Batch    string
		Success  []string
		Failed   map[string][]string
	}

	if err := json.Unmarshal(data, &tmp); err != nil {
		return err
	}

	p.Modified = int(tmp.Modified * 1000)
	p.Batch = tmp.Batch
	p.Success = tmp.Success
	p.Failed = tmp.Failed
	return nil
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
		"path":   r.URL.EscapedPath() + "?" + r.URL.RawQuery,
	}).Errorf("HTTP Error: %s", err.Error())

	switch getMediaType(w.Header().Get("Content-Type")) {
	case "application/newlines":
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
	case "":
		fallthrough
	case "application/json":
		JSONError(w, err.Error(), http.StatusInternalServerError)
	}
}

// NewLine prints out new line \n separated JSON objects instead of a
// single JSON array of objects
func NewLine(w http.ResponseWriter, r *http.Request, val interface{}) {
	if valR := reflect.ValueOf(val); valR.Kind() == reflect.Slice || valR.Kind() == reflect.Array {
		w.Header().Set("Content-Type", "application/newlines")
		for i := 0; i < valR.Len(); i++ {
			if !valR.Index(i).CanInterface() {
				continue
			}

			someValue := valR.Index(i).Interface()
			var (
				raw []byte
				err error
			)

			if jM, ok := someValue.(json.Marshaler); ok {
				// if someValue implements json.Marshaler it's faster (~2x)
				// to call it directly than go through json.Marshal
				raw, err = jM.MarshalJSON()
			} else {
				raw, err = json.Marshal(someValue)
			}

			if err != nil {
				InternalError(w, r, errors.Wrap(err, "web.NewLine could not marshal an item"))
				return
			}

			// write it all into a buffer since we might error
			w.Write(raw)
			w.Write([]byte("\n"))
		}
	} else {
		js, err := json.Marshal(val)
		if err != nil {
			InternalError(w, r, errors.Wrap(err, "web.NewLine could not marshal the object"))
			return
		}

		w.Header().Set("Content-Type", "application/newlines")
		w.Write(js)
		w.Write([]byte("\n"))
	}
}

func JSON(w http.ResponseWriter, r *http.Request, val interface{}) {
	js, err := json.Marshal(val)
	if err != nil {
		InternalError(w, r, err)
	} else {
		w.Header().Set("Content-Type", "application/json")
		w.Write(js)
		w.Write([]byte("\n"))
	}
}

// JsonNewline returns data as newline separated or as a single
// json array
func JsonNewline(w http.ResponseWriter, r *http.Request, val interface{}) {
	if strings.Contains(r.Header.Get("Accept"), "application/newlines") {
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
var (
	rewriteAccept = []string{"*/*", "application/*", "*/json"}
)

func AcceptHeaderOk(w http.ResponseWriter, r *http.Request) bool {
	accept := r.Header.Get("Accept")

	if accept == "" {
		r.Header.Set("Accept", "application/json")
		return true
	}

	mediatype := getMediaType(accept)

	if mediatype == "application/json" || mediatype == "application/newlines" {
		return true
	}

	for _, rewrite := range rewriteAccept {
		if strings.Contains(accept, rewrite) {
			r.Header.Set("Accept", "application/json")
			return true
		}
	}

	// everything else is an error
	sendRequestProblem(w, r, http.StatusNotAcceptable,
		errors.Errorf("Unsupported Accept header: %s", accept))

	return false
}

// OKResponse writes a 200 response with a simple string body
func OKResponse(w http.ResponseWriter, s string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, s)
}

// sendRequestProblem logs the problem with the client's request
// and responds with a json payload of the error. Client side these
// are usually invisible so this helps with debugging
func sendRequestProblem(w http.ResponseWriter, req *http.Request, responseCode int, reason error) {
	logRequestProblem(req, responseCode, reason)
	JSONError(w, reason.Error(), responseCode)
}

func logRequestProblem(req *http.Request, responseCode int, reason error) {
	var causeMessage string
	if cause := errors.Cause(reason); cause != nil && cause != reason {
		causeMessage = fmt.Sprintf("%v", cause)
	} else {
		causeMessage = "n/a"
	}

	log.WithFields(log.Fields{
		"method":    req.Method,
		"path":      req.URL.Path,
		"ua":        req.UserAgent(),
		"http_code": responseCode,
		"error":     reason.Error(),
		"cause":     causeMessage,
	}).Warning("HTTP Request Problem")
}

// getMediaType extracts the mediatype portion from the http request header Content-Type
// it returns a blank string on error. It also discards the paramters. This is enough
// for working with sync clients
func getMediaType(contentType string) (mediatype string) {
	mediatype, _, _ = mime.ParseMediaType(contentType)
	return
}
