package web

import (
	"net/http"
	"net/http/pprof"
)

// PprofHandler adds net/http/pprof into the system
type PprofHandler struct {
	handler http.Handler
	mux     *http.ServeMux
}

func NewPprofHandler(h http.Handler) *PprofHandler {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	return &PprofHandler{handler: h, mux: mux}
}

func (h *PprofHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	muxHandler, pattern := h.mux.Handler(req)
	if pattern == "" {
		h.handler.ServeHTTP(w, req)
		return
	} else {
		muxHandler.ServeHTTP(w, req)
	}
}
