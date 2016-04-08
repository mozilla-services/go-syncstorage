package api

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/coocood/freecache"
	"github.com/gorilla/mux"
	. "github.com/mostlygeek/go-debug"
	"github.com/mostlygeek/go-syncstorage/syncstorage"
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

type cacheStatus int

const (
	BATCH_MAX_IDS = 100

	// maximum number of BSOs per GET request
	MAX_BSO_GET_LIMIT = 2500

	CACHE_HIT = iota
	CACHE_MISS
)

// NewRouterFromContext creates a mux.Router and registers handlers from
// the supplied context to handle routes
func NewRouterFromContext(c *Context) http.Handler {

	// wrappers to make code cleaner below

	// check hawk auth and synchronize db access
	hs := func(h syncApiHandler) http.HandlerFunc {
		return c.hawk(c.synchronizeDBAccess(h))
	}

	// check Accept header, hawk auth and synchronize db access
	ahs := func(h syncApiHandler) http.HandlerFunc {
		return acceptOK(c.hawk(c.synchronizeDBAccess(h)))
	}

	r := mux.NewRouter()

	r.HandleFunc("/__heartbeat__", c.handleHeartbeat)
	r.HandleFunc("/1.5/{uid:[0-9]+}", hs(c.hDeleteEverything)).Methods("DELETE")
	r.HandleFunc("/1.5/{uid:[0-9]+}/storage", hs(c.hDeleteEverything)).Methods("DELETE")

	// support c.sync api version 1.5
	// https://docs.services.mozilla.com/storage/apis-1.5.html
	v := r.PathPrefix("/1.5/{uid:[0-9]+}/").Subrouter()

	// not part of the API, used to make sure uid matching works
	v.HandleFunc("/echo-uid", acceptOK(c.hawk(c.handleEchoUID))).Methods("GET")

	info := v.PathPrefix("/info/").Subrouter()
	info.HandleFunc("/collections", ahs(c.hInfoCollections)).Methods("GET")
	info.HandleFunc("/collection_usage", ahs(c.hInfoCollectionUsage)).Methods("GET")
	info.HandleFunc("/collection_counts", ahs(c.hInfoCollectionCounts)).Methods("GET")
	info.HandleFunc("/quota", hs(c.hInfoQuota)).Methods("GET")

	storage := v.PathPrefix("/storage/").Subrouter()
	storage.HandleFunc("/", handleTODO).Methods("DELETE")

	storage.HandleFunc("/{collection}", ahs(c.hCollectionGET)).Methods("GET")
	storage.HandleFunc("/{collection}", hs(c.hCollectionPOST)).Methods("POST")
	storage.HandleFunc("/{collection}", hs(c.hCollectionDELETE)).Methods("DELETE")
	storage.HandleFunc("/{collection}/{bsoId}", ahs(c.hBsoGET)).Methods("GET")
	storage.HandleFunc("/{collection}/{bsoId}", ahs(c.hBsoPUT)).Methods("PUT")
	storage.HandleFunc("/{collection}/{bsoId}", hs(c.hBsoDELETE)).Methods("DELETE")

	// wrap into a WeaveHandler to deal with:
	// -- adding X-Weave-Timestamp
	// -- 404 should be application/json
	return WeaveHandler(r)
}

func handleTODO(w http.ResponseWriter, r *http.Request) {
	JSONError(w, "Not implemented", http.StatusNotImplemented)
}

func NewContext(secrets []string, dispatch *syncstorage.Dispatch) (*Context, error) {

	if len(secrets) == 0 {
		return nil, ErrRequireSecretList
	}

	if dispatch == nil {
		return nil, ErrRequireDispatch
	}

	return &Context{
		colCache: NewCollectionCache(),

		Dispatch: dispatch,
		Secrets:  secrets,

		// allocate space to store 256K nonce signatures (~4MB RAM w/ MD5)
		hawkCache: freecache.NewCache(256 * 1024 * HAWK_NONCE_SIGNATURE_SIZE),
	}, nil
}

type Context struct {
	Dispatch *syncstorage.Dispatch

	colCache *collectionCache

	// preshared secrets with the token server
	// support a list of them as clients may send
	// a non-expired valid token created with a rotated secret
	Secrets []string

	// for testing
	DisableHawk bool

	// caches
	hawkCache *freecache.Cache // nonce cache

	// tweaks

	// Settings that tweak web behaviour
	MaxBSOGetLimit int
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

// sentCacheModified will send the weave X-modified headers and returns true if it wrote to w
func (c *Context) sentCacheModified(w http.ResponseWriter, r *http.Request, uid string) bool {

	var modified int

	if modified = c.colCache.GetModified(uid); modified == 0 { // no cache
		modified, err := c.Dispatch.LastModified(uid)
		if err != nil {
			InternalError(w, r, err)
			return true
		}

		c.colCache.SetModified(uid, modified)
	}

	return sentNotModified(w, r, modified)
}

func (c *Context) synchronizeDBAccess(h syncApiHandler) syncApiHandler {
	return func(w http.ResponseWriter, r *http.Request, uid string) {
		if err := c.Dispatch.LockUser(uid); err != nil {
			InternalError(w, r, err)
			return
		}
		defer c.Dispatch.UnlockUser(uid)
		h(w, r, uid)
	}
}

func (c *Context) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	// todo check dependencies to make sure they're ok..
	OKResponse(w, "OK")
}

func (c *Context) handleEchoUID(w http.ResponseWriter, r *http.Request, uid string) {

	// sleep here to make sure X-Weave-Timestamp code puts in
	// a to spec value
	time.Sleep(100 * time.Millisecond)

	w.Header().Set("X-Last-Modified", syncstorage.ModifiedToString(syncstorage.Now()))
	OKResponse(w, uid)
}

// hInfoQuota calculates the total disk space used by the user by calculating
// it based on the number of DB pages used * size of each page.
// TODO actually implement quotas in the system.
func (c *Context) hInfoQuota(w http.ResponseWriter, r *http.Request, uid string) {
	var (
		modified int
		results  map[string]int
		err      error
	)

	if c.sentCacheModified(w, r, uid) {
		return
	} else {
		modified = c.colCache.GetModified(uid)
	}

	// load cached results, or from the DB
	if results = c.colCache.GetInfoCollectionUsage(uid); results == nil {
		results, err = c.Dispatch.InfoCollectionUsage(uid)
		if err != nil {
			InternalError(w, r, err)
			return
		}

		if len(results) > 0 {
			c.colCache.SetInfoCollectionUsage(uid, results)
		}

		setCacheHeader(w, CACHE_MISS)
	} else {
		setCacheHeader(w, CACHE_HIT)
	}

	used := 0
	for _, bytes := range results {
		used += bytes
	}

	m := syncstorage.ModifiedToString(modified)
	w.Header().Set("X-Last-Modified", m)

	tmp := float64(used) / 1024
	JsonNewline(w, r, []*float64{&tmp, nil}) // crazy pointer cause need the nil
}

func (c *Context) hInfoCollections(w http.ResponseWriter, r *http.Request, uid string) {

	var (
		cacheStat cacheStatus
		info      map[string]int
		err       error
	)

	if d := c.colCache.GetInfoCollections(uid); d != nil {
		cacheStat = CACHE_HIT
		info = d
	} else {
		info, err = c.Dispatch.InfoCollections(uid)
		if len(info) > 0 {
			c.colCache.SetInfoCollections(uid, info)
			cacheStat = CACHE_MISS
		}
	}

	if err != nil {
		InternalError(w, r, err)
		return
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

		c.colCache.SetModified(uid, modified)
		m := syncstorage.ModifiedToString(modified)
		w.Header().Set("X-Last-Modified", m)
		setCacheHeader(w, cacheStat)
		JsonNewline(w, r, info)
	}
}

func (c *Context) hInfoCollectionUsage(w http.ResponseWriter, r *http.Request, uid string) {
	var (
		modified int
		results  map[string]int
		err      error
	)

	if c.sentCacheModified(w, r, uid) {
		return
	} else {
		modified = c.colCache.GetModified(uid)
	}

	// load cached results, or from the DB
	if results = c.colCache.GetInfoCollectionUsage(uid); results == nil {
		results, err = c.Dispatch.InfoCollectionUsage(uid)
		if err != nil {
			InternalError(w, r, err)
			return
		}

		if len(results) > 0 {
			c.colCache.SetInfoCollectionUsage(uid, results)
		}

		setCacheHeader(w, CACHE_MISS)
	} else {
		setCacheHeader(w, CACHE_HIT)
	}

	// the sync 1.5 api says data should be in KB

	resultsKB := make(map[string]float64)
	for name, bytes := range results {
		resultsKB[name] = float64(bytes) / 1024
	}
	m := syncstorage.ModifiedToString(modified)
	w.Header().Set("X-Last-Modified", m)
	JsonNewline(w, r, resultsKB)
}

func (c *Context) hInfoCollectionCounts(w http.ResponseWriter, r *http.Request, uid string) {
	var (
		modified int
		results  map[string]int
		err      error
	)

	if c.sentCacheModified(w, r, uid) {
		return
	} else {
		modified = c.colCache.GetModified(uid)
	}

	if results = c.colCache.GetInfoCollectionCounts(uid); results == nil {
		results, err = c.Dispatch.InfoCollectionCounts(uid)
		if err != nil {
			InternalError(w, r, err)
			return
		}

		if len(results) > 0 {
			c.colCache.SetInfoCollectionCounts(uid, results)
		}

		setCacheHeader(w, CACHE_MISS)
	} else {
		setCacheHeader(w, CACHE_HIT)
	}

	m := syncstorage.ModifiedToString(modified)
	w.Header().Set("X-Last-Modified", m)
	JsonNewline(w, r, results)
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
			InternalError(w, r, err)
			return

		}
	}

	if err = r.ParseForm(); err != nil {
		JSONError(w, "Bad query parameters", http.StatusBadRequest)
		return
	}

	if v := r.Form.Get("ids"); v != "" {
		ids = strings.Split(v, ",")

		if len(ids) > BATCH_MAX_IDS {
			JSONError(w, "exceeded max batch size", http.StatusBadRequest)
			return
		}

		for i, id := range ids {
			id = strings.TrimSpace(id)
			if syncstorage.BSOIdOk(id) {
				ids[i] = id
			} else {
				JSONError(w, fmt.Sprintf("Invalid bso id %s", id), http.StatusBadRequest)
				return
			}
		}

		if len(ids) > 100 {
			JSONError(w, fmt.Sprintf("Too many ids provided"), http.StatusRequestEntityTooLarge)
			return
		}
	}

	// we expect to get sync's two decimal timestamps, these need
	// to be converted to milliseconds
	if v := r.Form.Get("newer"); v != "" {
		floatNew, err := strconv.ParseFloat(v, 64)
		if err != nil {
			JSONError(w, "Invalid newer param format", http.StatusBadRequest)
			return
		}

		newer = int(floatNew * 1000)
		if !syncstorage.NewerOk(newer) {
			JSONError(w, "Invalid newer value", http.StatusBadRequest)
			return
		}
	}

	if v := r.Form.Get("full"); v != "" {
		full = true
	}

	if v := r.Form.Get("limit"); v != "" {
		limit, err = strconv.Atoi(v)
		if err != nil || !syncstorage.LimitOk(limit) {
			JSONError(w, "Invalid limit value", http.StatusBadRequest)
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
			JSONError(w, "Invalid offset value", http.StatusBadRequest)
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
			JSONError(w, "Invalid sort value", http.StatusBadRequest)
			return
		}
	}

	// this is way down here since IO is more expensive
	// than parsing if the GET params are valid
	cmodified, err := c.Dispatch.GetCollectionModified(uid, cId)
	if err != nil {
		InternalError(w, r, err)
		return
	} else if sentNotModified(w, r, cmodified) {
		return
	}

	results, err := c.Dispatch.GetBSOs(uid, cId, ids, newer, sort, limit, offset)
	if err != nil {
		InternalError(w, r, err)
		return
	}
	m := syncstorage.ModifiedToString(cmodified)
	w.Header().Set("X-Last-Modified", m)

	w.Header().Set("X-Weave-Records", strconv.Itoa(results.Total))
	if results.More {
		w.Header().Set("X-Weave-Next-Offset", strconv.Itoa(results.Offset))
	}

	if full {
		JsonNewline(w, r, results.BSOs)
	} else {
		bsoIds := make([]string, len(results.BSOs))
		for i, b := range results.BSOs {
			bsoIds[i] = b.Id
		}
		JsonNewline(w, r, bsoIds)
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

func (c *Context) hCollectionPOST(w http.ResponseWriter, r *http.Request, uid string) {
	// accept text/plain from old (broken) clients
	ct := r.Header.Get("Content-Type")

	if ct != "application/json" && ct != "text/plain" && ct != "application/newlines" {
		JSONError(w, "Not acceptable Content-Type", http.StatusUnsupportedMediaType)
		return
	}

	// a list of all the raw json encoded BSOs
	var raw []json.RawMessage

	if ct == "application/json" || ct == "text/plain" {
		decoder := json.NewDecoder(r.Body)
		err := decoder.Decode(&raw)
		if err != nil {
			WeaveInvalidWBOError(w, r)
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
				WeaveInvalidWBOError(w, r)
				return
			}

			results.AddFailure(err.bId, fmt.Sprintf("invalid %s", err.field))
		}
	}

	if len(bsoToBeProcessed) > BATCH_MAX_IDS {
		JSONError(w, fmt.Sprintf("Exceeded %d BSO per request", BATCH_MAX_IDS),
			http.StatusRequestEntityTooLarge)
		return
	}

	cId, err := c.getcid(r, uid, true) // automake the collection if it doesn't exit
	if err != nil {
		if err == syncstorage.ErrInvalidCollectionName {
			JSONError(w, err.Error(), http.StatusBadRequest)
		} else {
			InternalError(w, r, err)
		}
		return
	}

	cmodified, err := c.Dispatch.GetCollectionModified(uid, cId)
	if err != nil {
		InternalError(w, r, err)
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
		InternalError(w, r, err)
	} else {
		m := syncstorage.ModifiedToString(postResults.Modified)

		for bsoId, failMessage := range postResults.Failed {
			results.Failed[bsoId] = failMessage
		}

		// remove all cached data for the user
		c.colCache.Clear(uid)

		w.Header().Set("X-Last-Modified", m)
		JsonNewline(w, r, &PostResults{
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
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{"modified":%s}`, syncstorage.ModifiedToString(syncstorage.Now()))
			return
		} else {
			InternalError(w, r, err)
		}
		return
	}

	cmodified, err := c.Dispatch.GetCollectionModified(uid, cId)
	if err != nil {
		InternalError(w, r, err)
		return
	} else if sentNotModified(w, r, cmodified) {
		return
	}

	modified := syncstorage.Now()
	bids, idExists := r.URL.Query()["ids"]
	if idExists {

		bidlist := strings.Split(bids[0], ",")

		if len(bidlist) > BATCH_MAX_IDS {
			JSONError(w, "exceeded max batch size", http.StatusBadRequest)
			return
		}

		modified, err = c.Dispatch.DeleteBSOs(uid, cId, bidlist...)
		if err != nil {
			InternalError(w, r, err)
			return
		}
	} else {
		err = c.Dispatch.DeleteCollection(uid, cId)
		if err != nil {
			InternalError(w, r, err)
			return
		}
	}

	// remove all cached data for the user
	c.colCache.Clear(uid)
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"modified":%s}`, syncstorage.ModifiedToString(modified))
}

func (c *Context) getbso(w http.ResponseWriter, r *http.Request) (bId string, ok bool) {
	bId, ok = mux.Vars(r)["bsoId"]
	if !ok || !syncstorage.BSOIdOk(bId) {
		JSONError(w, "Invalid bso ID", http.StatusNotFound)
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
			JSONError(w, "Collection Not Found", http.StatusNotFound)
		} else {
			InternalError(w, r, err)
		}
		return
	}

	modified, err := c.Dispatch.GetBSOModified(uid, cId, bId)
	if err != nil {
		if err == syncstorage.ErrNotFound {
			JSONError(w, "BSO Not Found", http.StatusNotFound)
		} else {
			InternalError(w, r, err)
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
		JsonNewline(w, r, bso)
	} else {
		InternalError(w, r, err)
	}
}

func (c *Context) hBsoPUT(w http.ResponseWriter, r *http.Request, uid string) {

	// accept text/plain from old (broken) clients
	ct := r.Header.Get("Content-Type")
	if ct != "application/json" && ct != "text/plain" && ct != "application/newlines" {
		JSONError(w, "Not acceptable Content-Type", http.StatusUnsupportedMediaType)
		return
	}

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
		InternalError(w, r, err)
		return
	}

	modified, err = c.Dispatch.GetBSOModified(uid, cId, bId)
	if err != nil && err != syncstorage.ErrNotFound {
		InternalError(w, r, err)
		return
	} else if sentNotModified(w, r, modified) {
		return
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		InternalError(w, r, errors.New("PUT could not read JSON body"))
		return
	}

	var bso syncstorage.PutBSOInput
	if err := parseIntoBSO(body, &bso); err != nil {
		WeaveInvalidWBOError(w, r)
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
			JSONError(w, http.StatusText(http.StatusRequestEntityTooLarge), http.StatusRequestEntityTooLarge)
			return
		}

		JSONError(w, err.Error(), http.StatusBadRequest)
		return
	}

	// remove all cached data for the user
	c.colCache.Clear(uid)

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
		JSONError(w, "Collection Not Found", http.StatusNotFound)
		return
	}

	// Trying to delete a BSO that is not there
	// should 404
	bso, err := c.Dispatch.GetBSO(uid, cId, bId)
	if err != nil {
		if err == syncstorage.ErrNotFound {
			JSONError(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		} else {
			InternalError(w, r, err)
		}
		return
	}

	if sentNotModified(w, r, bso.Modified) {
		return
	}

	modified, err = c.Dispatch.DeleteBSO(uid, cId, bId)
	if err != nil {
		InternalError(w, r, err)
		return
	} else {
		// remove all cached data for the user
		c.colCache.Clear(uid)

		m := syncstorage.ModifiedToString(modified)
		w.Header().Set("Content-Type", "text/plain")
		w.Header().Set("X-Last-Modified", m)
		w.Write([]byte(m))
	}
}

func (c *Context) hDeleteEverything(w http.ResponseWriter, r *http.Request, uid string) {
	err := c.Dispatch.DeleteEverything(uid)
	if err != nil {
		InternalError(w, r, err)
		return
	} else {
		// remove all cached data for the user
		c.colCache.Clear(uid)

		m := syncstorage.ModifiedToString(syncstorage.Now())
		w.Header().Set("Content-Type", "text/plain")
		w.Header().Set("X-Last-Modified", m)
		w.Write([]byte(m))
	}
}
