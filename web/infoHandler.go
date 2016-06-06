package web

import (
	"fmt"
	"net/http"
	"os"
	"path"

	"github.com/gorilla/mux"
)

// InfoHandler serves endpoints that are not part of the sync 1.5
// api that a syncserver should provide
type InfoHandler struct {
	router *mux.Router
}

func NewInfoHandler(h http.Handler) *InfoHandler {

	r := mux.NewRouter()
	server := &InfoHandler{
		router: r,
	}

	r.NotFoundHandler = h
	r.HandleFunc("/", server.handleRoot)
	r.HandleFunc("/__heartbeat__", server.handleHeartbeat)
	r.HandleFunc("/__version__", server.handleVersion)

	return server
}

func (h *InfoHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	h.router.ServeHTTP(w, req)
}

func (h *InfoHandler) handleRoot(w http.ResponseWriter, req *http.Request) {
	OKResponse(w, "It Works!  SyncStorage is successfully running on this host.")
}

func (h *InfoHandler) handleHeartbeat(w http.ResponseWriter, req *http.Request) {
	OKResponse(w, "OK")
}

func (h *InfoHandler) handleVersion(w http.ResponseWriter, req *http.Request) {
	dir, err := os.Getwd()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprint(w, "Could not get CWD")
		return
	}

	//f, err := file.Open(path.Clean(dir + os.PathSeparator + "version.json"))
	filename := path.Clean(dir + string(os.PathSeparator) + "version.json")

	f, err := os.Open(filename)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	stat, err := f.Stat()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "stat failed")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	http.ServeContent(w, req, "__version__", stat.ModTime(), f)
}
