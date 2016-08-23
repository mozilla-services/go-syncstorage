package web

import (
	"crypto/sha1"
	"encoding/binary"
	"net/http"
	"strconv"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/pkg/errors"
)

const (
	conflictAttempts = 3
	conflictSleep    = 250 * time.Millisecond
)

type SyncPoolHandler struct {
	StoppableHandler

	// use multiple pools to spread lock
	// contention for parallel requests
	pools []*handlerPool

	userHandlerConfig *SyncUserHandlerConfig
}

type SyncPoolConfig struct {
	Basepath    string
	NumPools    int
	TTL         time.Duration
	MaxPoolSize int
}

func NewDefaultSyncPoolConfig(basepath string) *SyncPoolConfig {
	return &SyncPoolConfig{
		Basepath:    basepath,
		NumPools:    1,
		TTL:         5 * time.Minute,
		MaxPoolSize: 100,
	}
}

func NewSyncPoolHandler(config *SyncPoolConfig, userHandlerConfig *SyncUserHandlerConfig) *SyncPoolHandler {
	pools := make([]*handlerPool, config.NumPools, config.NumPools)
	for i := 0; i < config.NumPools; i++ {
		pools[i] = newHandlerPool(config.Basepath, config.MaxPoolSize)
	}

	if userHandlerConfig == nil {
		userHandlerConfig = NewDefaultSyncUserHandlerConfig()
	}

	server := &SyncPoolHandler{
		pools:             pools,
		userHandlerConfig: userHandlerConfig,
	}

	return server
}

func (s *SyncPoolHandler) poolIndex(uid string) uint16 {
	h := sha1.Sum([]byte(uid))
	// There are 20 bytes in a sha1 sum, we only need the
	// last 2 to determine the id
	return binary.BigEndian.Uint16(h[18:]) % uint16(len(s.pools))
}

// proxyHandler extracts the UID from the URL and passes control over
// to one of the internal handlers
func (s *SyncPoolHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if s.IsStopped() {
		s.StoppableHandler.ServeHTTP(w, req)
		return
	}

	var (
		uid     string
		element *poolElement
		err     error
	)

	if session, ok := SessionFromContext(req.Context()); ok {
		uid = session.Token.UidString()
	}

	if uid == "" {
		http.Error(w, "Invalid sync path", http.StatusBadRequest)
		return
	}

	poolId := s.poolIndex(uid)

	// if a request comes in while an element is being
	// cleaned up/closing, we retry a few times before failing
	for i := 1; i <= conflictAttempts; i++ {
		element, err = s.pools[poolId].getElement(uid)
		if err != nil {
			if err == errElementStopped {

				log.WithFields(log.Fields{
					"uid":     uid,
					"attempt": i,
				}).Info("pool.getElement conflict")

				if i == conflictAttempts {
					w.Header().Add("Retry-After", strconv.Itoa(60))
					http.Error(w, "DB Busy", http.StatusConflict)
					return
				}

				time.Sleep(conflictSleep)
			} else {
				InternalError(w, req, errors.Wrap(err, "Could not get Pool Element"))
				return
			}
		} else {
			break
		}
	}

	// pass it on
	element.handler.ServeHTTP(w, req)
}

// Stop immediately stops serving web requests and then it
// stops all additional handlers
func (s *SyncPoolHandler) StopHTTP() {
	if s.IsStopped() {
		return
	}

	s.StoppableHandler.StopHTTP()
	for _, p := range s.pools {
		p.stopHandlers()
	}
}
