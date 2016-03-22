package api

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"

	log "github.com/Sirupsen/logrus"

	"github.com/gorilla/mux"
	. "github.com/mostlygeek/go-debug"
	"github.com/mostlygeek/go-syncstorage/syncstorage"
	"github.com/mostlygeek/go-syncstorage/token"
	"github.com/mozilla-services/hawk-go"
)

var apiDebug = Debug("syncapi")
var authDebug = Debug("syncapi:auth")

var (
	ErrMissingBSOId      = errors.New("Missing BSO Id")
	ErrInvalidPostJSON   = errors.New("Malformed POST JSON")
	ErrRequireSecretList = errors.New("Require secrets list")
	ErrRequireDispatch   = errors.New("Require Dispatch")
	ErrNoSecretsDefined  = errors.New("No secrets defined")

	ErrTokenInvalid = errors.New("Token is invalid")
	ErrTokenExpired = errors.New("Token is expired")
)

const (
	MAX_BSO_PER_POST_REQUEST = 100

	// maximum number of BSOs per GET request
	MAX_BSO_GET_LIMIT = 2500

	// old legacy stuff, used to keep compatibility with python/old clients
	// https://github.com/mozilla-services/server-syncstorage/blob/fd3c8b90278cb9944cb224964af6e6dae19c9263/syncstorage/tweens.py#L17-L21

	WEAVE_UNKNOWN_ERROR  = "0"
	WEAVE_ILLEGAL_METH   = "1"  // Illegal method/protocol
	WEAVE_MALFORMED_JSON = "6"  // Json parse failure
	WEAVE_INVALID_WBO    = "8"  // Invalid Weave Basic Object
	WEAVE_OVER_QUOTA     = "14" // User over quota
)

// NewRouterFromContext creates a mux.Router and registers handlers from
// the supplied context to handle routes
func NewRouterFromContext(c *Context) *mux.Router {
	r := mux.NewRouter()

	r.HandleFunc("/__heartbeat__", c.handleHeartbeat)
	r.HandleFunc("/1.5/{uid:[0-9]+}", c.hawk(c.hDeleteEverything)).Methods("DELETE")
	r.HandleFunc("/1.5/{uid:[0-9]+}/storage", c.hawk(c.hDeleteEverything)).Methods("DELETE")

	// support sync api version 1.5
	// https://docs.services.mozilla.com/storage/apis-1.5.html
	v := r.PathPrefix("/1.5/{uid:[0-9]+}/").Subrouter()

	// not part of the API, used to make sure uid matching works
	v.HandleFunc("/echo-uid", c.acceptOK(c.hawk(c.handleEchoUID))).Methods("GET")

	info := v.PathPrefix("/info/").Subrouter()
	info.HandleFunc("/collections", c.acceptOK(c.hawk(c.hInfoCollections))).Methods("GET")
	info.HandleFunc("/collection_usage", c.acceptOK(c.hawk(c.hInfoCollectionUsage))).Methods("GET")
	info.HandleFunc("/collection_counts", c.acceptOK(c.hawk(c.hInfoCollectionCounts))).Methods("GET")
	info.HandleFunc("/quota", c.hawk(c.hInfoQuota)).Methods("GET")

	storage := v.PathPrefix("/storage/").Subrouter()
	storage.HandleFunc("/", handleTODO).Methods("DELETE")

	storage.HandleFunc("/{collection}", c.acceptOK(c.hawk(c.hCollectionGET))).Methods("GET")
	storage.HandleFunc("/{collection}", c.hawk(c.hCollectionPOST)).Methods("POST")
	storage.HandleFunc("/{collection}", c.hawk(c.hCollectionDELETE)).Methods("DELETE")
	storage.HandleFunc("/{collection}/{bsoId}", c.acceptOK(c.hawk(c.hBsoGET))).Methods("GET")
	storage.HandleFunc("/{collection}/{bsoId}", c.acceptOK(c.hawk(c.hBsoPUT))).Methods("PUT")
	storage.HandleFunc("/{collection}/{bsoId}", c.hawk(c.hBsoDELETE)).Methods("DELETE")

	return r
}

func handleTODO(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "Not implemented", http.StatusNotImplemented)
}

type syncApiHandler func(http.ResponseWriter, *http.Request, string)

func NewContext(secrets []string, dispatch *syncstorage.Dispatch) (*Context, error) {

	if len(secrets) == 0 {
		return nil, ErrRequireSecretList
	}

	if dispatch == nil {
		return nil, ErrRequireDispatch
	}

	return &Context{
		Dispatch: dispatch,
		Secrets:  secrets,
	}, nil
}

type Context struct {
	Dispatch *syncstorage.Dispatch

	// preshared secrets with the token server
	// support a list of them as clients may send
	// a non-expired valid token created with a rotated secret
	Secrets []string

	// for testing
	DisableHawk bool

	// tweaks

	// Settings that tweak web behaviour
	MaxBSOGetLimit int
}

// acceptOK checks that the request has an Accept header that is either
// application/json or application/newlines
func (c *Context) acceptOK(h http.HandlerFunc) http.HandlerFunc {
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

// uid extracts the uid value from the URL and passes it another
// http.HandlerFunc for actual functionality
func (c *Context) uid(h syncApiHandler) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var uid string
		var ok bool

		vars := mux.Vars(r)
		if uid, ok = vars["uid"]; !ok {
			http.Error(w, "UID missing", http.StatusBadRequest)
		}

		// finally pass it off to the handler
		h(w, r, uid)
	})
}

// Error produces an HTTP 500 error, basically means a bug in the system
func (c *Context) Error(w http.ResponseWriter, r *http.Request, err error) {
	log.WithFields(log.Fields{
		"err":    err.Error(),
		"method": r.Method,
		"path":   r.URL.Path,
	}).Errorf("HTTP Error: %s", err.Error())
	http.Error(w,
		http.StatusText(http.StatusInternalServerError),
		http.StatusInternalServerError)
}

func (c *Context) WeaveInvalidWBOError(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusBadRequest)
	w.Write([]byte(WEAVE_INVALID_WBO))
}

// JsonNewline returns data as newline separated or as a single
// json array
func (c *Context) JsonNewline(w http.ResponseWriter, r *http.Request, val interface{}) {

	if r.Header.Get("Accept") == "application/newlines" {
		c.NewLine(w, r, val)
	} else {
		c.JSON(w, r, val)
	}
}

// NewLine prints out new line \n separated JSON objects instead of a
// single JSON array of objects
func (c *Context) NewLine(w http.ResponseWriter, r *http.Request, val interface{}) {

	var vals []json.RawMessage
	// make sure we can convert all of it to JSON before
	// trying to make it all newline JSON
	js, err := json.Marshal(val)
	if err != nil {
		c.Error(w, r, err)
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

func (c *Context) JSON(w http.ResponseWriter, r *http.Request, val interface{}) {
	js, err := json.Marshal(val)
	if err != nil {
		c.Error(w, r, err)
	} else {
		w.Header().Set("Content-Type", "application/json")
		w.Write(js)
	}
}

// getcid turns the collection name in the URI to its internal Id number. the `automake`
// parameter will auto-make it if it doesn't exist
func (c *Context) getcid(r *http.Request, uid string, automake bool) (cId int, err error) {
	collection := mux.Vars(r)["collection"]

	if !syncstorage.CollectionNameOk(collection) {
		err = syncstorage.ErrInvalidCollectionName
		return
	}

	cId, err = c.Dispatch.GetCollectionId(uid, collection)

	if err == nil {
		return
	}

	if err == syncstorage.ErrNotFound {
		if automake {
			cId, err = c.Dispatch.CreateCollection(uid, collection)
		}
	}

	return
}

// Ok writes a 200 response with a simple string body
func okResponse(w http.ResponseWriter, s string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, s)
}

func (c *Context) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	// todo check dependencies to make sure they're ok..
	okResponse(w, "OK")
}

func (c *Context) handleEchoUID(w http.ResponseWriter, r *http.Request, uid string) {
	okResponse(w, uid)
}

// hInfoQuota calculates the total disk space used by the user by calculating
// it based on the number of DB pages used * size of each page.
// TODO actually implement quotas in the system.
func (c *Context) hInfoQuota(w http.ResponseWriter, r *http.Request, uid string) {

	modified, err := c.Dispatch.LastModified(uid)
	if err != nil {
		c.Error(w, r, err)
	} else if sentNotModified(w, r, modified) {
		return
	}

	pagestats, err := c.Dispatch.Usage(uid)
	if err != nil {
		c.Error(w, r, err)
	} else {
		tmp := pagestats.Total * pagestats.Size / 1024

		// TODO implement quotas, see issue: #29
		c.JsonNewline(w, r, []*int{&tmp, nil})
	}
}

func (c *Context) hInfoCollections(w http.ResponseWriter, r *http.Request, uid string) {
	info, err := c.Dispatch.InfoCollections(uid)
	if err != nil {
		c.Error(w, r, err)
	} else {
		modified := 0
		for _, modtime := range info {
			if modtime > modified {
				modified = modtime
			}
		}

		if sentNotModified(w, r, modified) {
			return
		}

		m := syncstorage.ModifiedToString(modified)
		w.Header().Set("X-Last-Modified", m)
		c.JsonNewline(w, r, info)
	}
}

func (c *Context) hInfoCollectionUsage(w http.ResponseWriter, r *http.Request, uid string) {
	modified, err := c.Dispatch.LastModified(uid)
	if err != nil {
		c.Error(w, r, err)
	} else if sentNotModified(w, r, modified) {
		return
	}

	results, err := c.Dispatch.InfoCollectionUsage(uid)
	if err != nil {
		c.Error(w, r, err)
	} else {
		// the sync 1.5 api says data should be in KB
		resultsKB := make(map[string]float64)
		for name, bytes := range results {
			resultsKB[name] = float64(bytes) / 1024
		}
		c.JsonNewline(w, r, resultsKB)
	}
}

func (c *Context) hInfoCollectionCounts(w http.ResponseWriter, r *http.Request, uid string) {
	modified, err := c.Dispatch.LastModified(uid)
	if err != nil {
		c.Error(w, r, err)
	} else if sentNotModified(w, r, modified) {
		return
	}

	results, err := c.Dispatch.InfoCollectionCounts(uid)
	if err != nil {
		c.Error(w, r, err)
	} else {
		c.JsonNewline(w, r, results)
	}
}

func (c *Context) hCollectionGET(w http.ResponseWriter, r *http.Request, uid string) {

	// query params that control searching
	var (
		err    error
		ids    []string
		newer  int
		full   bool
		limit  int
		offset int
		sort   = syncstorage.SORT_NEWEST
	)

	cId, err := c.getcid(r, uid, false)

	if err != nil {
		if err == syncstorage.ErrNotFound {
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte("[]"))
			return
		} else {
			c.Error(w, r, err)
			return

		}
	}

	if err = r.ParseForm(); err != nil {
		http.Error(w, "Bad query parameters", http.StatusBadRequest)
		return
	}

	if v := r.Form.Get("ids"); v != "" {
		ids = strings.Split(v, ",")
		for i, id := range ids {
			id = strings.TrimSpace(id)
			if syncstorage.BSOIdOk(id) {
				ids[i] = id
			} else {
				http.Error(w, fmt.Sprintf("Invalid bso id %s", id), http.StatusBadRequest)
				return
			}
		}

		if len(ids) > 100 {
			http.Error(w, fmt.Sprintf("Too many ids provided"), http.StatusRequestEntityTooLarge)
			return
		}
	}

	// we expect to get sync's two decimal timestamps, these need
	// to be converted to milliseconds
	if v := r.Form.Get("newer"); v != "" {
		floatNew, err := strconv.ParseFloat(v, 64)
		if err != nil {
			http.Error(w, "Invalid newer param format", http.StatusBadRequest)
			return
		}

		newer = int(floatNew * 1000)
		if !syncstorage.NewerOk(newer) {
			http.Error(w, "Invalid newer value", http.StatusBadRequest)
			return
		}
	}

	if v := r.Form.Get("full"); v != "" {
		full = true
	}

	if v := r.Form.Get("limit"); v != "" {
		limit, err = strconv.Atoi(v)
		if err != nil || !syncstorage.LimitOk(limit) {
			http.Error(w, "Invalid limit value", http.StatusBadRequest)
			return
		}
	}

	// assign a default value for limit if nothing is supplied
	if limit == 0 {
		if c.MaxBSOGetLimit > 0 { // only use this if it was set
			limit = c.MaxBSOGetLimit
		} else {
			limit = MAX_BSO_GET_LIMIT
		}
	}

	// make sure limit is smaller than c.MaxBSOGetLimit if it is set
	if limit > c.MaxBSOGetLimit && c.MaxBSOGetLimit > 0 {
		limit = c.MaxBSOGetLimit
	}

	// finally a global max that we never want to go over
	// TODO is this value ok for prod?
	if limit > MAX_BSO_GET_LIMIT {
		limit = MAX_BSO_GET_LIMIT
	}

	if v := r.Form.Get("offset"); v != "" {
		offset, err = strconv.Atoi(v)
		if err != nil || !syncstorage.OffsetOk(offset) {
			http.Error(w, "Invalid offset value", http.StatusBadRequest)
			return
		}
	}

	if v := r.Form.Get("sort"); v != "" {
		switch v {
		case "newest":
			sort = syncstorage.SORT_NEWEST
		case "oldest":
			sort = syncstorage.SORT_OLDEST
		case "index":
			sort = syncstorage.SORT_INDEX
		default:
			http.Error(w, "Invalid sort value", http.StatusBadRequest)
			return
		}
	}

	// this is here since IO is more expensive than parsing
	// the GET parameters
	cmodified, err := c.Dispatch.GetCollectionModified(uid, cId)
	if err != nil {
		c.Error(w, r, err)
		return
	} else if sentNotModified(w, r, cmodified) {
		return
	}

	results, err := c.Dispatch.GetBSOs(uid, cId, ids, newer, sort, limit, offset)
	if err != nil {
		c.Error(w, r, err)
		return
	}
	m := syncstorage.ModifiedToString(cmodified)
	w.Header().Set("X-Last-Modified", m)

	w.Header().Set("X-Weave-Records", strconv.Itoa(results.Total))
	if results.More {
		w.Header().Set("X-Weave-Next-Offset", strconv.Itoa(results.Offset))
	}

	if full {
		c.JsonNewline(w, r, results.BSOs)
	} else {
		bsoIds := make([]string, len(results.BSOs))
		for i, b := range results.BSOs {
			bsoIds[i] = b.Id
		}
		c.JsonNewline(w, r, bsoIds)
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

func (c *Context) hCollectionPOST(w http.ResponseWriter, r *http.Request, uid string) {
	// accept text/plain from old (broken) clients
	ct := r.Header.Get("Content-Type")

	if ct != "application/json" && ct != "text/plain" && ct != "application/newlines" {
		http.Error(w, "Not acceptable Content-Type", http.StatusUnsupportedMediaType)
		return
	}

	// a list of all the raw json encoded BSOs
	var raw []json.RawMessage

	if ct == "application/json" || ct == "text/plain" {
		decoder := json.NewDecoder(r.Body)
		err := decoder.Decode(&raw)
		if err != nil {
			c.WeaveInvalidWBOError(w, r)
			return
		}
	} else { // deal with application/newlines
		raw = []json.RawMessage{}
		scanner := bufio.NewScanner(r.Body)
		for scanner.Scan() {
			bsoBytes := scanner.Bytes()
			raw = append(raw, bsoBytes)
		}
	}

	// bsoToBeProcessed will actually get sent to the DB
	bsoToBeProcessed := syncstorage.PostBSOInput{}
	results := syncstorage.NewPostResults(syncstorage.Now())

	for _, rawJSON := range raw {
		var b syncstorage.PutBSOInput
		if err := parseIntoBSO(rawJSON, &b); err == nil {
			bsoToBeProcessed = append(bsoToBeProcessed, &b)
		} else {
			// ignore empty whitespace lines from application/newlines
			if len(strings.TrimSpace(string(rawJSON))) == 0 {
				continue
			}

			// couldn't parse a BSO into something real
			// abort immediately
			if err.field == "-" { // json error, not an object
				c.WeaveInvalidWBOError(w, r)
				return
			}

			results.AddFailure(err.bId, fmt.Sprintf("invalid %s", err.field))
		}
	}

	if len(bsoToBeProcessed) > MAX_BSO_PER_POST_REQUEST {
		http.Error(w, fmt.Sprintf("Exceeded %d BSO per request", MAX_BSO_PER_POST_REQUEST),
			http.StatusRequestEntityTooLarge)
		return
	}

	cId, err := c.getcid(r, uid, true) // automake the collection if it doesn't exit
	if err != nil {
		if err == syncstorage.ErrInvalidCollectionName {
			http.Error(w, err.Error(), http.StatusBadRequest)
		} else {
			c.Error(w, r, err)
		}
		return
	}

	cmodified, err := c.Dispatch.GetCollectionModified(uid, cId)
	if err != nil {
		c.Error(w, r, err)
		return
	} else if sentNotModified(w, r, cmodified) {
		return
	}

	// change posted[].TTL from seconds (what clients send)
	// to milliseconds (what the DB uses)
	for _, p := range bsoToBeProcessed {
		if p.TTL != nil {
			tmp := *p.TTL * 1000
			p.TTL = &tmp
		}
	}

	// Send the changes to the database and merge
	// with `results` above
	postResults, err := c.Dispatch.PostBSOs(uid, cId, bsoToBeProcessed)

	if err != nil {
		c.Error(w, r, err)
	} else {
		m := syncstorage.ModifiedToString(postResults.Modified)

		for bsoId, failMessage := range postResults.Failed {
			results.Failed[bsoId] = failMessage
		}

		w.Header().Set("X-Last-Modified", m)
		c.JsonNewline(w, r, &PostResults{
			Modified: m,
			Success:  postResults.Success,
			Failed:   results.Failed,
		})
	}
}

func (c *Context) hCollectionDELETE(w http.ResponseWriter, r *http.Request, uid string) {
	cId, err := c.getcid(r, uid, false)

	if err != nil {
		if err == syncstorage.ErrNotFound {
			// nothing to delete... return a successful response
			c.JsonNewline(w, r, map[string]int{"modified": syncstorage.Now()})
		} else {
			c.Error(w, r, err)
		}
		return
	}

	cmodified, err := c.Dispatch.GetCollectionModified(uid, cId)
	if err != nil {
		c.Error(w, r, err)
		return
	} else if sentNotModified(w, r, cmodified) {
		return
	}

	modified := syncstorage.Now()
	bids, idExists := r.URL.Query()["ids"]
	if idExists {
		modified, err = c.Dispatch.DeleteBSOs(uid, cId, strings.Split(bids[0], ",")...)
		if err != nil {
			c.Error(w, r, err)
			return
		}
	} else {
		err = c.Dispatch.DeleteCollection(uid, cId)
		if err != nil {
			c.Error(w, r, err)
			return
		}
	}

	c.JsonNewline(w, r, map[string]int{"modified": modified})
}

func (c *Context) getbso(w http.ResponseWriter, r *http.Request) (bId string, ok bool) {
	bId, ok = mux.Vars(r)["bsoId"]
	if !ok || !syncstorage.BSOIdOk(bId) {
		http.Error(w, "Invalid bso ID", http.StatusNotFound)
	}

	return
}

func (c *Context) hBsoGET(w http.ResponseWriter, r *http.Request, uid string) {

	var (
		bId string
		ok  bool
		cId int
		err error
		bso *syncstorage.BSO
	)

	if bId, ok = c.getbso(w, r); !ok {
		return
	}

	cId, err = c.getcid(r, uid, false)

	if err != nil {
		if err == syncstorage.ErrNotFound {
			http.NotFound(w, r)
		} else {
			c.Error(w, r, err)
		}
		return
	}

	modified, err := c.Dispatch.GetBSOModified(uid, cId, bId)
	if err != nil {
		if err == syncstorage.ErrNotFound {
			http.NotFound(w, r)
		} else {
			c.Error(w, r, err)
		}
		return
	}

	if sentNotModified(w, r, modified) {
		return
	} else {
		m := syncstorage.ModifiedToString(modified)
		w.Header().Set("X-Last-Modified", m)
	}

	bso, err = c.Dispatch.GetBSO(uid, cId, bId)
	if err == nil {
		c.JsonNewline(w, r, bso)
	} else {
		c.Error(w, r, err)
	}
}

func (c *Context) hBsoPUT(w http.ResponseWriter, r *http.Request, uid string) {

	var (
		bId      string
		ok       bool
		cId      int
		modified int
		err      error
	)

	if bId, ok = c.getbso(w, r); !ok {
		return
	}

	cId, err = c.getcid(r, uid, true)
	if err != nil {
		c.Error(w, r, err)
		return
	}

	modified, err = c.Dispatch.GetBSOModified(uid, cId, bId)
	if err != nil && err != syncstorage.ErrNotFound {
		c.Error(w, r, err)
		return
	} else if sentNotModified(w, r, modified) {
		return
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		c.Error(w, r, errors.New("PUT could not read JSON body"))
		return
	}

	var bso syncstorage.PutBSOInput
	if err := parseIntoBSO(body, &bso); err != nil {
		c.WeaveInvalidWBOError(w, r)
		return
	}

	// change bso.TTL to milliseconds (what the db uses)
	// from seconds (what client's send)
	if bso.TTL != nil {
		tmp := *bso.TTL * 1000
		bso.TTL = &tmp
	}

	modified, err = c.Dispatch.PutBSO(uid, cId, bId, bso.Payload, bso.SortIndex, bso.TTL)

	if err != nil {
		if err == syncstorage.ErrPayloadTooBig {
			http.Error(w, http.StatusText(http.StatusRequestEntityTooLarge), http.StatusRequestEntityTooLarge)
			return
		}

		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	m := syncstorage.ModifiedToString(modified)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Last-Modified", m)
	w.Write([]byte(m))
}

func (c *Context) hBsoDELETE(w http.ResponseWriter, r *http.Request, uid string) {
	var (
		bId      string
		ok       bool
		cId      int
		modified int
		err      error
	)

	if bId, ok = c.getbso(w, r); !ok {
		return
	}

	cId, err = c.getcid(r, uid, false)
	if err == syncstorage.ErrNotFound {
		http.NotFound(w, r)
		return
	}

	// Trying to delete a BSO that is not there
	// should 404
	bso, err := c.Dispatch.GetBSO(uid, cId, bId)
	if err != nil {
		if err == syncstorage.ErrNotFound {
			http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		} else {
			c.Error(w, r, err)
		}
		return
	}

	if sentNotModified(w, r, bso.Modified) {
		return
	}

	modified, err = c.Dispatch.DeleteBSO(uid, cId, bId)
	if err != nil {
		c.Error(w, r, err)
		return
	} else {
		m := syncstorage.ModifiedToString(modified)
		w.Header().Set("Content-Type", "text/plain")
		w.Header().Set("X-Last-Modified", m)
		w.Write([]byte(m))
	}
}

func (c *Context) hDeleteEverything(w http.ResponseWriter, r *http.Request, uid string) {

	err := c.Dispatch.DeleteEverything(uid)
	if err != nil {
		c.Error(w, r, err)
		return
	} else {
		m := syncstorage.ModifiedToString(syncstorage.Now())
		w.Header().Set("Content-Type", "text/plain")
		w.Header().Set("X-Last-Modified", m)
		w.Write([]byte(m))
	}
}
