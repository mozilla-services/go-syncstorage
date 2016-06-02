package web

import (
	"net/http"
	"sync"
)

// StoppableHandler abstracts common logic to stop serving regular
// traffic
type StoppableHandler struct {
	sync.Mutex
	stopped    bool
	RetryAfter string
}

func (s *StoppableHandler) StopHTTP() {
	s.Lock()
	s.stopped = true
	s.Unlock()
}

func (s *StoppableHandler) IsStopped() bool {
	s.Lock()
	defer s.Unlock()
	return s.stopped
}

func (s *StoppableHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	retryAfter := s.RetryAfter
	if retryAfter == "" {
		retryAfter = "60"
	}

	w.Header().Set("Retry-After", retryAfter)
	http.Error(w, "Service Unvailable", http.StatusServiceUnavailable)
}
