package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/gorilla/mux"
	"github.com/mostlygeek/go-syncstorage/syncstorage"
)

var (
	ErrMissingBSOId    = errors.New("Missing BSO Id")
	ErrInvalidPostJSON = errors.New("Malformed POST JSON")
)

const (
	MAX_BSO_PER_POST_REQUEST = 100
)

// Dependencies holds run time created resources for handlers to use
type Dependencies struct {
	Dispatch *syncstorage.Dispatch

	// Todo:
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

	storage.HandleFunc("/{collection}", makeSyncHandler(d, addcid(hCollectionGET))).Methods("GET")
	storage.HandleFunc("/{collection}", makeSyncHandler(d, addcid(hCollectionPOST))).Methods("POST")
	storage.HandleFunc("/{collection}", makeSyncHandler(d, addcid(hCollectionDELETE))).Methods("DELETE")

	storage.HandleFunc("/{collection}/{bsoId}", makeSyncHandler(d, notImplemented)).Methods("GET")
	storage.HandleFunc("/{collection}/{bsoId}", makeSyncHandler(d, notImplemented)).Methods("PUT")
	storage.HandleFunc("/{collection}/{bsoId}", makeSyncHandler(d, notImplemented)).Methods("DELETE")

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

// addcid adds another closure layer for extract the collection id from /1.5/{uid}/storage/{collection}
// so other handlers don't need to repeat this code
func addcid(h storageHandler) syncHandler {
	return syncHandler(func(w http.ResponseWriter, r *http.Request, d *Dependencies, uid string) {
		collection := mux.Vars(r)["collection"]
		cId, err := d.Dispatch.GetCollectionId(uid, collection)
		if err != nil {
			errorResponse(w, r, d, err)
		} else {
			h(w, r, d, uid, cId)
		}
	})
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

// ErrorResponse writes an error to the HTTP response but also does stuff
// on the serverside to aid in debugging
func errorResponse(w http.ResponseWriter, r *http.Request, d *Dependencies, err error) {
	// TODO someting with err and d...logging ? sentry? etc.
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

func hCollectionGET(w http.ResponseWriter, r *http.Request, d *Dependencies, uid string, cId int) {

	// query params that control searching
	var (
		err           error
		ids           []string
		newer         int
		full          bool
		limit, offset int
		sort          = syncstorage.SORT_NEWEST
	)

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

	if v := r.Form.Get("newer"); v != "" {
		newer, err = strconv.Atoi(v)
		if err != nil || !syncstorage.NewerOk(newer) {
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

	okResponse(w, fmt.Sprintf("ids: %v, newer: %d, full: %v, limit: %d, offset: %d, sort: %v, `%v`", ids, newer, full, limit, offset, sort, r.Form.Get("ids")))

}
func hCollectionDELETE(w http.ResponseWriter, r *http.Request, d *Dependencies, uid string, cId int) {
	notImplemented(w, r, d, uid)
}

func hCollectionPOST(w http.ResponseWriter, r *http.Request, d *Dependencies, uid string, cId int) {
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
		http.Error(w, fmt.Sprintf("Exceeded %d BSO per rquest", MAX_BSO_PER_POST_REQUEST),
			http.StatusRequestEntityTooLarge)
		return
	}

	// check for missing/invalid Ids
	for _, b := range posted {
		if !syncstorage.BSOIdOk(b.Id) {
			http.Error(w, "Invalid or missing Id in data", http.StatusBadRequest)
			return
		}
	}

	results, err := d.Dispatch.PostBSOs(uid, cId, posted)
	if err != nil {
		errorResponse(w, r, d, err)
	} else {
		jsonResponse(w, r, d, results)
	}
}

func extractPostRequestBSOs(data []byte) (posted syncstorage.PostBSOInput, err error) {
	err = json.Unmarshal(data, &posted)
	return
}

//func hBsoGET(w http.ResponseWriter, r *http.Request, d *Dependencies, uid string) {}
//func hBSOPUT(w http.ResponseWriter, r *http.Request, d *Dependencies, uid string) {}
//func hBsoDELETE(w http.ResponseWriter, r *http.Request, d *Dependencies, uid string) {}
