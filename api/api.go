package api

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/mostlygeek/go-syncstorage/syncstorage"
)

// Dependencies holds run time created resources for handlers to use
type Dependencies struct {
	Dispatch *syncstorage.Dispatch
}

// NewRouter creates a mux.Router with http handlers for the syncstorage API
func NewRouter(d *Dependencies) *mux.Router {
	r := mux.NewRouter()

	r.HandleFunc("/__heartbeat__", func(w http.ResponseWriter, r *http.Request) { heartbeatHandler(w, r, d) })

	// support sync api version 1.5
	// https://docs.services.mozilla.com/storage/apis-1.5.html
	v := r.PathPrefix("/1.5/{uid:[0-9]+}/").Subrouter()

	// not part of the API, used to make sure uid matching works
	v.HandleFunc("/echo-uid", makeSyncHandler(d, uidHandler)).Methods("GET")

	//r.Methods("DELETE").HandleFunc("/", makeSyncHandler(d, notImplemented))

	info := v.PathPrefix("/info/").Subrouter()
	info.HandleFunc("/collections", makeSyncHandler(d, handleInfoCollections)).Methods("GET")
	/*
		info.HandleFunc("/quota", makeSyncHandler(d, notImplemented)).Methods("GET")
		info.HandleFunc("/collection_usage", makeSyncHandler(d, notImplemented)).Methods("GET")
		info.HandleFunc("/collection_counts", makeSyncHandler(d, notImplemented)).Methods("GET")

			storage := r.PathPrefix("/storage").Subrouter()
			storage.Methods("DELETE").HandleFunc("/", makeSyncHandler(d, notImplemented))

			storage.Methods("GET").HandleFunc("/{collection}", makeSyncHandler(d, notImplemented))
			storage.Methods("POST").HandleFunc("/{collection}", makeSyncHandler(d, notImplemented))
			storage.Methods("DELETE").HandleFunc("/{collection}", makeSyncHandler(d, notImplemented))

			storage.Methods("GET").HandleFunc("/{collection}/{bsoId}", makeSyncHandler(d, notImplemented))
			storage.Methods("PUT").HandleFunc("/{collection}/{bsoId}", makeSyncHandler(d, notImplemented))
			storage.Methods("DELETE").HandleFunc("/{collection}/{bsoId}", makeSyncHandler(d, notImplemented))
	*/

	// non-standard to test things
	//v.HandleFunc("/echo/", makeSyncHandler(d, uidHandler)).Methods("GET")
	r.HandleFunc("/1.6/{uid}/echo", makeSyncHandler(d, uidHandler)).Methods("GET")

	return r

}

type syncHandler func(http.ResponseWriter, *http.Request, *Dependencies, string)

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

func notImplemented(w http.ResponseWriter, r *http.Request, d *Dependencies, uid string) {
	http.Error(w, "Not implemented", http.StatusNotImplemented)
}

// Ok writes a 200 response with a simple string body
func Ok(w http.ResponseWriter, s string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, s)
}

func heartbeatHandler(w http.ResponseWriter, r *http.Request, d *Dependencies) {
	// todo check dependencies to make sure they're ok..
	Ok(w, "OK")
}

func uidHandler(w http.ResponseWriter, r *http.Request, d *Dependencies, uid string) { Ok(w, uid) }

// ErrorResponse writes an error to the HTTP response but also does stuff
// on the serverside to aid in debugging
func errorResponse(w http.ResponseWriter, r *http.Request, d *Dependencies, err error) {
	// TODO someting with err and d...logging ? sentry? etc.
	http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
}

func jsonResponse(w http.ResponseWriter, r *http.Request, d *Dependencies, val interface{}) {
	js, err := json.Marshal(val)
	if err != nil {
		errorResponse(w, r, d, err)
	} else {
		w.Header().Set("Content-Type", "application/json")
		w.Write(js)
	}
}

func handleInfoCollections(w http.ResponseWriter, r *http.Request, d *Dependencies, uid string) {
	info, err := d.Dispatch.InfoCollections(uid)
	if err != nil {
		errorResponse(w, r, d, err)
	} else {
		jsonResponse(w, r, d, info)
	}
}

//func infoCollections(w http.ResponseWriter, r *http.Request, d *Dependencies, uid string)
