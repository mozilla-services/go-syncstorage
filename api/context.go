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

// NewRouterFromContext creates a mux.Router and registers handlers from
// the supplied context to handle routes
func NewRouterFromContext(c *Context) *mux.Router {
	r := mux.NewRouter()

	r.HandleFunc("/__heartbeat__", c.handleHeartbeat)
	r.HandleFunc("/1.5/{uid:[0-9]+}", c.hawk(c.uid(c.hDeleteEverything))).Methods("DELETE")
	r.HandleFunc("/1.5/{uid:[0-9]+}/storage", c.hawk(c.uid(c.hDeleteEverything))).Methods("DELETE")

	// support sync api version 1.5
	// https://docs.services.mozilla.com/storage/apis-1.5.html
	v := r.PathPrefix("/1.5/{uid:[0-9]+}/").Subrouter()

	// not part of the API, used to make sure uid matching works
	v.HandleFunc("/echo-uid", c.hawk(c.uid(c.handleEchoUID))).Methods("GET")

	info := v.PathPrefix("/info/").Subrouter()
	info.HandleFunc("/collections", c.hawk(c.uid(c.hInfoCollections))).Methods("GET")
	info.HandleFunc("/collection_usage", c.hawk(c.uid(c.hInfoCollectionUsage))).Methods("GET")
	info.HandleFunc("/collection_counts", c.hawk(c.uid(c.hInfoCollectionCounts))).Methods("GET")

	info.HandleFunc("/quota", handleTODO).Methods("GET")

	storage := v.PathPrefix("/storage/").Subrouter()
	storage.HandleFunc("/", handleTODO).Methods("DELETE")

	storage.HandleFunc("/{collection}", c.hawk(c.uid(c.hCollectionGET))).Methods("GET")
	storage.HandleFunc("/{collection}", c.hawk(c.uid(c.hCollectionPOST))).Methods("POST")
	storage.HandleFunc("/{collection}", c.hawk(c.uid(c.hCollectionDELETE))).Methods("DELETE")
	storage.HandleFunc("/{collection}/{bsoId}", c.hawk(c.uid(c.hBsoGET))).Methods("GET")
	storage.HandleFunc("/{collection}/{bsoId}", c.hawk(c.uid(c.hBsoPUT))).Methods("PUT")
	storage.HandleFunc("/{collection}/{bsoId}", c.hawk(c.uid(c.hBsoDELETE))).Methods("DELETE")

	return r
}

func handleTODO(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "Not implemented", http.StatusNotImplemented)
}

type Context struct {
	Dispatch *syncstorage.Dispatch

	// tweaks

	// Settings that tweak web behaviour
	MaxBSOGetLimit int
}

type syncApiHandler func(http.ResponseWriter, *http.Request, string)

// hawk checks HAWK authentication headers and returns an unauthorized response
// if they are invalid
func (c *Context) hawk(h http.HandlerFunc) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// TODO actually implement HAWK here
		h(w, r)
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

func (c *Context) Error(w http.ResponseWriter, r *http.Request, err error) {
	// TODO someting with err and d...logging ? sentry? etc.
	apiDebug("errorResponse: err=%s", err.Error())
	http.Error(w,
		http.StatusText(http.StatusInternalServerError),
		http.StatusInternalServerError)
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

func (c *Context) hInfoCollections(w http.ResponseWriter, r *http.Request, uid string) {
	info, err := c.Dispatch.InfoCollections(uid)
	if err != nil {
		c.Error(w, r, err)
	} else {
		c.JSON(w, r, info)
	}
}

func (c *Context) hInfoCollectionUsage(w http.ResponseWriter, r *http.Request, uid string) {
	results, err := c.Dispatch.InfoCollectionUsage(uid)
	if err != nil {
		c.Error(w, r, err)
	} else {
		c.JSON(w, r, results)
	}
}

func (c *Context) hInfoCollectionCounts(w http.ResponseWriter, r *http.Request, uid string) {
	results, err := c.Dispatch.InfoCollectionCounts(uid)
	if err != nil {
		c.Error(w, r, err)
	} else {
		c.JSON(w, r, results)
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

	results, err := c.Dispatch.GetBSOs(uid, cId, ids, newer, sort, limit, offset)
	if err != nil {
		c.Error(w, r, err)
		return
	}

	w.Header().Set("X-Weave-Records", strconv.Itoa(results.Total))
	if results.More {
		w.Header().Set("X-Weave-Next-Offset", strconv.Itoa(results.Offset))
	}

	if full {
		c.JSON(w, r, results.BSOs)
	} else {
		bsoIds := make([]string, len(results.BSOs))
		for i, b := range results.BSOs {
			bsoIds[i] = b.Id
		}
		c.JSON(w, r, bsoIds)
	}
}

func (c *Context) hCollectionPOST(w http.ResponseWriter, r *http.Request, uid string) {
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

	cId, err := c.getcid(r, uid, true) // automake the collection if it doesn't exit
	if err != nil {
		if err == syncstorage.ErrInvalidCollectionName {
			http.Error(w, err.Error(), http.StatusBadRequest)
		} else {
			c.Error(w, r, err)
		}
		return
	}

	results, err := c.Dispatch.PostBSOs(uid, cId, posted)
	if err != nil {
		c.Error(w, r, err)
	} else {
		c.JSON(w, r, results)
	}
}

func (c *Context) hCollectionDELETE(w http.ResponseWriter, r *http.Request, uid string) {

	cId, err := c.getcid(r, uid, false)
	if err == nil {
		err = c.Dispatch.DeleteCollection(uid, cId)
	}

	if err != nil {
		if err != syncstorage.ErrNotFound {
			c.Error(w, r, err)
		}
	} else {
		w.Header().Set("Content-Type", "text/plain")
		w.Write([]byte("ok"))
	}
}

func (c *Context) getbso(w http.ResponseWriter, r *http.Request) (bId string, ok bool) {
	bId, ok = mux.Vars(r)["bsoId"]
	if !ok || !syncstorage.BSOIdOk(bId) {
		http.Error(w, "Invalid bso ID", http.StatusBadRequest)
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
	if err == nil {
		bso, err = c.Dispatch.GetBSO(uid, cId, bId)
		if err == nil {
			c.JSON(w, r, bso)
			return
		}
	}

	if err == syncstorage.ErrNotFound {
		http.NotFound(w, r)
		return
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

	var bso struct {
		Payload   *string `json:"payload"`
		SortIndex *int    `json:"sortindex"`
		TTL       *int    `json:"ttl"`
	}

	cId, err = c.getcid(r, uid, false)
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
	w.Header().Set("Content-Type", "text/plain")
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
