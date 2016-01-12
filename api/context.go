package api

import (
	"net/http"

	"github.com/gorilla/mux"
	"github.com/mostlygeek/go-syncstorage/syncstorage"
)

// NewRouterFromContext creates a mux.Router and registers handlers from
// the supplied context to handle routes
func NewRouterFromContext(c *Context) *mux.Router {
	r := mux.NewRouter()

	r.HandleFunc("/__heartbeat__", c.handleHeartbeat)

	// support sync api version 1.5
	// https://docs.services.mozilla.com/storage/apis-1.5.html
	v := r.PathPrefix("/1.5/{uid:[0-9]+}/").Subrouter()

	// not part of the API, used to make sure uid matching works
	v.HandleFunc("/echo-uid", c.hawk(c.uid(c.handleEchoUID))).Methods("GET")

	/*
		r.HandleFunc("/", makeSyncHandler(d, notImplemented)).Methods("DELETE")



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
	*/

	return r
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

func (c *Context) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	// todo check dependencies to make sure they're ok..
	okResponse(w, "OK")
}

func (c *Context) handleEchoUID(w http.ResponseWriter, r *http.Request, uid string) {
	okResponse(w, uid)
}
