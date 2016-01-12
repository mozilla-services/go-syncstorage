package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/gorilla/mux"
	. "github.com/mostlygeek/go-debug"
	"github.com/mostlygeek/go-syncstorage/syncstorage"
)

var apiDebug = Debug("syncapi")

var (
	ErrMissingBSOId    = errors.New("Missing BSO Id")
	ErrInvalidPostJSON = errors.New("Malformed POST JSON")
)

const (
	MAX_BSO_PER_POST_REQUEST = 100
	MAX_BSO_PAYLOAD_SIZE     = 256 * 1024

	// maximum number of BSOs per GET request
	MAX_BSO_GET_LIMIT = 2500
)

// Dependencies holds run time created resources for handlers to use
type Dependencies struct {
	Dispatch *syncstorage.Dispatch

	// Settings that tweak web behaviour
	MaxBSOGetLimit int

	// Other dependent services:
	// HawkAuth
	// Datadog
	// Logging
	// Sentry?
}

// NewRouter creates a mux.Router with http handlers for the syncstorage API
func NewRouter(d *Dependencies) *mux.Router {
	r := mux.NewRouter()

	r.HandleFunc("/__heartbeat__", func(w http.ResponseWriter, r *http.Request) { handleHeartbeat(w, r, d) })
	r.HandleFunc("/", makeSyncHandler(d, notImplemented)).Methods("DELETE")

	// support sync api version 1.5
	// https://docs.services.mozilla.com/storage/apis-1.5.html
	v := r.PathPrefix("/1.5/{uid:[0-9]+}/").Subrouter()

	// not part of the API, used to make sure uid matching works
	v.HandleFunc("/echo-uid", makeSyncHandler(d, handleUIDecho)).Methods("GET")

	info := v.PathPrefix("/info/").Subrouter()
	info.HandleFunc("/collections", makeSyncHandler(d, hInfoCollections)).Methods("GET")
	info.HandleFunc("/quota", makeSyncHandler(d, notImplemented)).Methods("GET")
	info.HandleFunc("/collection_usage", makeSyncHandler(d, hInfoCollectionUsage)).Methods("GET")
	info.HandleFunc("/collection_counts", makeSyncHandler(d, hInfoCollectionCounts)).Methods("GET")

	storage := v.PathPrefix("/storage/").Subrouter()
	storage.HandleFunc("/", makeSyncHandler(d, notImplemented)).Methods("DELETE")

	storage.HandleFunc("/{collection}", makeSyncHandler(d, hCollectionGET)).Methods("GET")
	storage.HandleFunc("/{collection}", makeSyncHandler(d, hCollectionPOST)).Methods("POST")
	storage.HandleFunc("/{collection}", makeSyncHandler(d, hCollectionDELETE)).Methods("DELETE")

	storage.HandleFunc("/{collection}/{bsoId}", makeSyncHandler(d, hBsoGET)).Methods("GET")
	storage.HandleFunc("/{collection}/{bsoId}", makeSyncHandler(d, hBsoPUT)).Methods("PUT")
	storage.HandleFunc("/{collection}/{bsoId}", makeSyncHandler(d, hBsoDELETE)).Methods("DELETE")

	return r
}

type syncHandler func(http.ResponseWriter, *http.Request, *Dependencies, string)
type storageHandler func(http.ResponseWriter, *http.Request, *Dependencies, string, int)

func makeSyncHandler(d *Dependencies, h syncHandler) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// TODO check HAWK authentication
		var uid string
		var ok bool

		vars := mux.Vars(r)
		if uid, ok = vars["uid"]; !ok {
			http.Error(w, "UID missing", http.StatusBadRequest)
		}

		// finally pass it off to the handler
		h(w, r, d, uid)
	})
}

// getCollectionId turns the collection name in the URI to its internal Id number. the `automake`
// parameter will auto-make it if it doesn't exist
func getCollectionId(r *http.Request, d *Dependencies, uid string, automake bool) (cId int, err error) {
	collection := mux.Vars(r)["collection"]

	if !syncstorage.CollectionNameOk(collection) {
		err = syncstorage.ErrInvalidCollectionName
		return
	}

	cId, err = d.Dispatch.GetCollectionId(uid, collection)

	if err == nil {
		return
	}

	if err == syncstorage.ErrNotFound {
		if automake {
			cId, err = d.Dispatch.CreateCollection(uid, collection)
		}
	}

	return
}

func notImplemented(w http.ResponseWriter, r *http.Request, d *Dependencies, uid string) {
	http.Error(w, "Not implemented", http.StatusNotImplemented)
}

// Ok writes a 200 response with a simple string body
func okResponse(w http.ResponseWriter, s string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, s)
}

// errorResponse is used for Internal server errors which should have been
// caused by bugs. It is meant to aid in debugging
// on the serverside to aid in debugging
func errorResponse(w http.ResponseWriter, r *http.Request, d *Dependencies, err error) {
	// TODO someting with err and d...logging ? sentry? etc.
	apiDebug("errorResponse: err=%s", err.Error())
	http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
}

// jsonResponse takes some value and marshals it into JSON, returns an error response if
// it fails
func jsonResponse(w http.ResponseWriter, r *http.Request, d *Dependencies, val interface{}) {
	js, err := json.Marshal(val)
	if err != nil {
		errorResponse(w, r, d, err)
	} else {
		w.Header().Set("Content-Type", "application/json")
		w.Write(js)
	}
}

func handleHeartbeat(w http.ResponseWriter, r *http.Request, d *Dependencies) {
	// todo check dependencies to make sure they're ok..
	okResponse(w, "OK")
}

func handleUIDecho(w http.ResponseWriter, r *http.Request, d *Dependencies, uid string) {
	okResponse(w, uid)
}

func hInfoCollections(w http.ResponseWriter, r *http.Request, d *Dependencies, uid string) {
	info, err := d.Dispatch.InfoCollections(uid)
	if err != nil {
		errorResponse(w, r, d, err)
	} else {
		jsonResponse(w, r, d, info)
	}
}

func hInfoCollectionUsage(w http.ResponseWriter, r *http.Request, d *Dependencies, uid string) {
	results, err := d.Dispatch.InfoCollectionUsage(uid)
	if err != nil {
		errorResponse(w, r, d, err)
	} else {
		jsonResponse(w, r, d, results)
	}
}

func hInfoCollectionCounts(w http.ResponseWriter, r *http.Request, d *Dependencies, uid string) {
	results, err := d.Dispatch.InfoCollectionCounts(uid)
	if err != nil {
		errorResponse(w, r, d, err)
	} else {
		jsonResponse(w, r, d, results)
	}
}

func hCollectionGET(w http.ResponseWriter, r *http.Request, d *Dependencies, uid string) {

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

	cId, err := getCollectionId(r, d, uid, false)

	if err != nil {
		if err == syncstorage.ErrNotFound {
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte("[]"))
			return
		} else {
			errorResponse(w, r, d, err)
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
		if d.MaxBSOGetLimit > 0 { // only use this if it was set
			limit = d.MaxBSOGetLimit
		} else {
			limit = MAX_BSO_GET_LIMIT
		}
	}

	// make sure limit is smaller than d.MaxBSOGetLimit if it is set
	if limit > d.MaxBSOGetLimit && d.MaxBSOGetLimit > 0 {
		limit = d.MaxBSOGetLimit
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

	results, err := d.Dispatch.GetBSOs(uid, cId, ids, newer, sort, limit, offset)
	if err != nil {
		errorResponse(w, r, d, err)
		return
	}

	w.Header().Set("X-Weave-Records", strconv.Itoa(results.Total))
	if results.More {
		w.Header().Set("X-Weave-Next-Offset", strconv.Itoa(results.Offset))
	}

	if full {
		jsonResponse(w, r, d, results.BSOs)
	} else {
		bsoIds := make([]string, len(results.BSOs))
		for i, b := range results.BSOs {
			bsoIds[i] = b.Id
		}
		jsonResponse(w, r, d, bsoIds)
	}
}

func hCollectionDELETE(w http.ResponseWriter, r *http.Request, d *Dependencies, uid string) {

	cId, err := getCollectionId(r, d, uid, false)
	if err == nil {
		err = d.Dispatch.DeleteCollection(uid, cId)
	}

	if err != nil {
		if err != syncstorage.ErrNotFound {
			errorResponse(w, r, d, err)
		}
	} else {
		w.Header().Set("Content-Type", "text/plain")
		w.Write([]byte("ok"))
	}
}

func hCollectionPOST(w http.ResponseWriter, r *http.Request, d *Dependencies, uid string) {
	// accept text/plain from old (broken) clients
	if ct := r.Header.Get("Content-Type"); ct != "application/json" && ct != "text/plain" {
		http.Error(w, "Not acceptable Content-Type", http.StatusUnsupportedMediaType)
		return
	}

	// parsing the results is sort of ugly since fields can be left out
	// if they are not to be submitted
	var posted syncstorage.PostBSOInput

	decoder := json.NewDecoder(r.Body)
	err := decoder.Decode(&posted)
	if err != nil {
		http.Error(w, "Invalid JSON posted", http.StatusBadRequest)
		return
	}

	if len(posted) > MAX_BSO_PER_POST_REQUEST {
		http.Error(w, fmt.Sprintf("Exceeded %d BSO per request", MAX_BSO_PER_POST_REQUEST),
			http.StatusRequestEntityTooLarge)
		return
	}

	// validate basic bso data
	for _, b := range posted {
		if !syncstorage.BSOIdOk(b.Id) {
			http.Error(w, "Invalid or missing Id in data", http.StatusBadRequest)
			return
		}

		if b.Payload != nil && len(*b.Payload) > MAX_BSO_PAYLOAD_SIZE {
			http.Error(w, fmt.Sprintf("%s payload greater than max of %d bytes",
				b.Id, MAX_BSO_PAYLOAD_SIZE), http.StatusBadRequest)
			return
		}
	}

	cId, err := getCollectionId(r, d, uid, true) // automake the collection if it doesn't exit
	if err != nil {
		if err == syncstorage.ErrInvalidCollectionName {
			http.Error(w, err.Error(), http.StatusBadRequest)
		} else {
			errorResponse(w, r, d, err)
		}
		return
	}

	results, err := d.Dispatch.PostBSOs(uid, cId, posted)
	if err != nil {
		errorResponse(w, r, d, err)
	} else {
		jsonResponse(w, r, d, results)
	}
}

func hBsoGET(w http.ResponseWriter, r *http.Request, d *Dependencies, uid string) {

	bId, ok := mux.Vars(r)["bsoId"]
	if !ok || !syncstorage.BSOIdOk(bId) {
		http.Error(w, "Invalid bso ID", http.StatusBadRequest)
		return
	}

	var (
		cId int
		err error
		bso *syncstorage.BSO
	)

	cId, err = getCollectionId(r, d, uid, false)
	if err == nil {
		bso, err = d.Dispatch.GetBSO(uid, cId, bId)
		if err == nil {
			jsonResponse(w, r, d, bso)
			return
		}
	}

	if err == syncstorage.ErrNotFound {
		http.NotFound(w, r)
		return
	} else {
		errorResponse(w, r, d, err)
	}
}

func hBsoPUT(w http.ResponseWriter, r *http.Request, d *Dependencies, uid string) {

	bId, ok := mux.Vars(r)["bsoId"]
	if !ok || !syncstorage.BSOIdOk(bId) {
		http.Error(w, "Invalid bso ID", http.StatusBadRequest)
		return
	}

	var (
		cId      int
		modified int
		err      error
	)

	var bso struct {
		Payload   *string `json:"payload"`
		SortIndex *int    `json:"sortindex"`
		TTL       *int    `json:"ttl"`
	}

	cId, err = getCollectionId(r, d, uid, false)
	if err == syncstorage.ErrNotFound {
		http.NotFound(w, r)
		return
	}

	decoder := json.NewDecoder(r.Body)
	err = decoder.Decode(&bso)

	if err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	modified, err = d.Dispatch.PutBSO(uid, cId, bId, bso.Payload, bso.SortIndex, bso.TTL)

	if err != nil {
		if err == syncstorage.ErrPayloadTooBig {
			http.Error(w, http.StatusText(http.StatusRequestEntityTooLarge), http.StatusRequestEntityTooLarge)
			return
		}

		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	m := syncstorage.ModifiedToString(modified)
	w.Header().Set("Content-Type", "text/plain")
	w.Header().Set("X-Last-Modified", m)
	w.Write([]byte(m))
}

func hBsoDELETE(w http.ResponseWriter, r *http.Request, d *Dependencies, uid string) {
	notImplemented(w, r, d, uid)
}
