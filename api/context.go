package api

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/mostlygeek/go-syncstorage/syncstorage"
)

// NewRouterFromContext creates a mux.Router and registers handlers from
// the supplied context to handle routes
func NewRouterFromContext(c *Context) *mux.Router {
	r := mux.NewRouter()

	r.HandleFunc("/__heartbeat__", c.handleHeartbeat)
	r.HandleFunc("/", handleTODO).Methods("DELETE")

	// support sync api version 1.5
	// https://docs.services.mozilla.com/storage/apis-1.5.html
	v := r.PathPrefix("/1.5/{uid:[0-9]+}/").Subrouter()

	// not part of the API, used to make sure uid matching works
	v.HandleFunc("/echo-uid", c.hawk(c.uid(c.handleEchoUID))).Methods("GET")

	info := v.PathPrefix("/info/").Subrouter()
	info.HandleFunc("/collections", c.hawk(c.uid(c.hInfoCollections))).Methods("GET")
	/*
		info.HandleFunc("/quota", c.hawk(c.uid(c.notImplemented))).Methods("GET")
		info.HandleFunc("/collection_usage", c.hawk(c.uid(c.hInfoCollectionUsage))).Methods("GET")
		info.HandleFunc("/collection_counts", c.hawk(c.uid(c.hInfoCollectionCounts))).Methods("GET")

		storage := v.PathPrefix("/storage/").Subrouter()
		storage.HandleFunc("/", c.hawk(c.uid(c.notImplemented))).Methods("DELETE")

		storage.HandleFunc("/{collection}", c.hawk(c.uid(c.hCollectionGET))).Methods("GET")
		storage.HandleFunc("/{collection}", c.hawk(c.uid(c.hCollectionPOST))).Methods("POST")
		storage.HandleFunc("/{collection}", c.hawk(c.uid(c.hCollectionDELETE))).Methods("DELETE")

		storage.HandleFunc("/{collection}/{bsoId}", c.hawk(c.uid(c.hBsoGET))).Methods("GET")
		storage.HandleFunc("/{collection}/{bsoId}", c.hawk(c.uid(c.hBsoPUT))).Methods("PUT")
		storage.HandleFunc("/{collection}/{bsoId}", c.hawk(c.uid(c.hBsoDELETE))).Methods("DELETE")
	*/

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
