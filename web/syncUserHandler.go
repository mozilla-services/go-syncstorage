package web

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/pkg/errors"

	"github.com/gorilla/mux"
	"github.com/mozilla-services/go-syncstorage/syncstorage"
)

const (
	BATCH_MAX_IDS = 100

	// maximum number of BSOs per GET request
	MAX_BSO_GET_LIMIT = 2500
)

// SyncUserHandler provides all the sync 1.5 API routes for a single user.
// It implements http.Handler. It's design is kept simple on purpose
// to make it easy to wrap it in other http.Handler.
type SyncUserHandler struct {
	StoppableHandler
	requestLock sync.Mutex

	router *mux.Router
	uid    string
	db     *syncstorage.DB

	// Sync 1.5 tracks changes based on timestamps.
	// The X-Last-Modified has an accuracy of 10's of milliseconds.
	// Changes (POST, PUT, DELETE) require a unique timestamp so the handler
	// must sleep for a bit to ensure all timestamps are unique. Also all changes
	// need to be synchronized
	lastChange time.Time

	// Over rides
	MaxBSOGetLimit int
}

func NewSyncUserHandler(uid string, db *syncstorage.DB) *SyncUserHandler {

	// https://docs.services.mozilla.com/storage/apis-1.5.html
	r := mux.NewRouter()

	server := &SyncUserHandler{
		uid:    uid,
		router: r,
		db:     db,
	}

	// top level deletions for the user and their storage
	// Note: not part of the sub-routers since since they don't end with a `/`
	r.HandleFunc("/1.5/"+uid, server.hDeleteEverything).Methods("DELETE")
	r.HandleFunc("/1.5/"+uid+"/storage", server.hDeleteEverything).Methods("DELETE")

	v := r.PathPrefix("/1.5/" + uid + "/").Subrouter()

	info := v.PathPrefix("/info/").Subrouter()
	info.HandleFunc("/collections", server.hInfoCollections).Methods("GET")
	info.HandleFunc("/collection_usage", server.hInfoCollectionUsage).Methods("GET")
	info.HandleFunc("/collection_counts", server.hInfoCollectionCounts).Methods("GET")
	info.HandleFunc("/quota", server.hInfoQuota).Methods("GET")

	storage := v.PathPrefix("/storage/").Subrouter()

	storage.HandleFunc("/{collection}", server.hCollectionGET).Methods("GET")
	storage.HandleFunc("/{collection}", server.hCollectionPOST).Methods("POST")
	storage.HandleFunc("/{collection}", server.hCollectionDELETE).Methods("DELETE")
	storage.HandleFunc("/{collection}/{bsoId}", server.hBsoGET).Methods("GET")
	storage.HandleFunc("/{collection}/{bsoId}", server.hBsoPUT).Methods("PUT")
	storage.HandleFunc("/{collection}/{bsoId}", server.hBsoDELETE).Methods("DELETE")

	return server
}

func (s *SyncUserHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	s.requestLock.Lock()
	defer s.requestLock.Unlock()

	if s.IsStopped() {
		s.StoppableHandler.ServeHTTP(w, req)
		return
	}

	switch req.Method {
	case "POST", "PUT", "DELETE":
		// make sure all X-Last-Modified values are unique we sleep for a bit
		var toSleep time.Duration

		if s.lastChange.IsZero() {
			// edge case race where db is closed by cleanup and
			// reopened (new request) < 10ms later results in the same
			// modified timestmap. Sleep just to be safe for all new changes
			toSleep = 10 * time.Millisecond
		} else {
			toSleep = 11*time.Millisecond - time.Now().Sub(s.lastChange)
		}

		if toSleep > 0 {
			if log.GetLevel() == log.DebugLevel {
				log.WithFields(log.Fields{
					"t_ms":   toSleep,
					"uid":    s.uid,
					"method": req.Method,
					"p":      req.RequestURI,
				}).Debug("write-delay")
			}
			time.Sleep(toSleep)
		}
		s.router.ServeHTTP(w, req)
		s.lastChange = time.Now()
	default:
		s.router.ServeHTTP(w, req)
	}
}

// Stop immediately prevents handling web requests then purges
// expired BSOs before closing the DB.
func (s *SyncUserHandler) StopHTTP() {
	s.requestLock.Lock()
	defer s.requestLock.Unlock()

	if s.IsStopped() {
		return
	}

	s.StoppableHandler.StopHTTP()

	numPurged, err := s.db.PurgeExpired()
	if err != nil {
		log.WithFields(log.Fields{
			"uid": s.uid,
			"err": err.Error(),
		}).Error("SyncUserHandler.StopHTTP - Fail")
	} else {
		log.WithFields(log.Fields{
			"uid":         s.uid,
			"bsos_purged": numPurged,
		}).Info("SyncUserHandler.StopHTTP - OK")
	}

	s.db.Close()
}

// getcid looks up a collection by name and returns its id. If it doesn't
// exist it will create it if automake is true
func (s *SyncUserHandler) getcid(r *http.Request, automake bool) (cId int, err error) {
	collection := mux.Vars(r)["collection"]

	if !syncstorage.CollectionNameOk(collection) {
		err = syncstorage.ErrInvalidCollectionName
		return
	}

	cId, err = s.db.GetCollectionId(collection)

	if err == nil {
		return
	}

	if err == syncstorage.ErrNotFound && automake {
		cId, err = s.db.CreateCollection(collection)
	}

	return
}

// hInfoQuota calculates the total disk space used by the user by calculating
// it based on the number of DB pages used * size of each page.
// TODO actually implement quotas in the system.
func (s *SyncUserHandler) hInfoQuota(w http.ResponseWriter, r *http.Request) {
	results, err := s.db.InfoCollectionUsage()
	if err != nil {
		InternalError(w, r, err)
		return
	}

	modified, err := s.db.LastModified()
	if err != nil {
		InternalError(w, r, err)
		return
	}

	if sentNotModified(w, r, modified) {
		return
	}

	used := 0
	for _, bytes := range results {
		used += bytes
	}

	m := syncstorage.ModifiedToString(modified)
	w.Header().Set("X-Last-Modified", m)

	tmp := float64(used) / 1024
	JsonNewline(w, r, []*float64{&tmp, nil}) // crazy pointer cause need the nil
}

func (s *SyncUserHandler) hInfoCollections(w http.ResponseWriter, r *http.Request) {

	if !AcceptHeaderOk(w, r) {
		return
	}

	if info, err := s.db.InfoCollections(); err != nil {
		InternalError(w, r, err)
		return
	} else {
		modified := 0
		for _, modtime := range info {
			if modtime > modified {
				modified = modtime
			}
		}

		if sentNotModified(w, r, modified) {
			return
		}

		m := syncstorage.ModifiedToString(modified)
		w.Header().Set("X-Last-Modified", m)
		JsonNewline(w, r, info)
	}
}

func (s *SyncUserHandler) hInfoCollectionUsage(w http.ResponseWriter, r *http.Request) {
	if !AcceptHeaderOk(w, r) {
		return
	}

	modified, err := s.db.LastModified()
	if err != nil {
		InternalError(w, r, err)
		return
	}

	if sentNotModified(w, r, modified) {
		return
	}

	if results, err := s.db.InfoCollectionUsage(); err != nil {
		InternalError(w, r, err)
		return
	} else {
		// the sync 1.5 api says data should be in KB
		resultsKB := make(map[string]float64)
		for name, bytes := range results {
			resultsKB[name] = float64(bytes) / 1024
		}
		m := syncstorage.ModifiedToString(modified)
		w.Header().Set("X-Last-Modified", m)
		JsonNewline(w, r, resultsKB)
	}
}

func (s *SyncUserHandler) hInfoCollectionCounts(w http.ResponseWriter, r *http.Request) {
	if !AcceptHeaderOk(w, r) {
		return
	}
	results, err := s.db.InfoCollectionCounts()
	if err != nil {
		InternalError(w, r, err)
		return
	}

	modified, err := s.db.LastModified()
	if err != nil {
		InternalError(w, r, err)
		return
	}

	if sentNotModified(w, r, modified) {
		return
	}

	m := syncstorage.ModifiedToString(modified)
	w.Header().Set("X-Last-Modified", m)
	JsonNewline(w, r, results)
}

func (s *SyncUserHandler) hCollectionGET(w http.ResponseWriter, r *http.Request) {

	if !AcceptHeaderOk(w, r) {
		return
	}

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

	cId, err := s.getcid(r, false)

	if err != nil {
		if err == syncstorage.ErrNotFound {
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte("[]"))
			return
		} else {
			InternalError(w, r, err)
			return
		}
	}

	if err = r.ParseForm(); err != nil {
		JSONError(w, "Bad query parameters", http.StatusBadRequest)
		return
	}

	if v := r.Form.Get("ids"); v != "" {
		ids = strings.Split(v, ",")

		if len(ids) > BATCH_MAX_IDS {
			JSONError(w, "exceeded max batch size", http.StatusBadRequest)
			return
		}

		for i, id := range ids {
			id = strings.TrimSpace(id)
			if syncstorage.BSOIdOk(id) {
				ids[i] = id
			} else {
				JSONError(w, fmt.Sprintf("Invalid bso id %s", id), http.StatusBadRequest)
				return
			}
		}

		if len(ids) > 100 {
			JSONError(w, fmt.Sprintf("Too many ids provided"), http.StatusRequestEntityTooLarge)
			return
		}
	}

	// we expect to get sync's two decimal timestamps, these need
	// to be converted to milliseconds
	if v := r.Form.Get("newer"); v != "" {
		floatNew, err := strconv.ParseFloat(v, 64)
		if err != nil {
			JSONError(w, "Invalid newer param format", http.StatusBadRequest)
			return
		}

		newer = int(floatNew * 1000)
		if !syncstorage.NewerOk(newer) {
			JSONError(w, "Invalid newer value", http.StatusBadRequest)
			return
		}
	}

	if v := r.Form.Get("full"); v != "" {
		full = true
	}

	if v := r.Form.Get("limit"); v != "" {
		limit, err = strconv.Atoi(v)
		if err != nil || !syncstorage.LimitOk(limit) {
			JSONError(w, "Invalid limit value", http.StatusBadRequest)
			return
		}
	}

	// assign a default value for limit if nothing is supplied
	if limit == 0 {
		if s.MaxBSOGetLimit > 0 { // only use this if it was set
			limit = s.MaxBSOGetLimit
		} else {
			limit = MAX_BSO_GET_LIMIT
		}
	}

	// make sure limit is smaller than s.MaxBSOGetLimit if it is set
	if limit > s.MaxBSOGetLimit && s.MaxBSOGetLimit > 0 {
		limit = s.MaxBSOGetLimit
	}

	// finally a global max that we never want to go over
	// TODO is this value ok for prod?
	if limit > MAX_BSO_GET_LIMIT {
		limit = MAX_BSO_GET_LIMIT
	}

	if v := r.Form.Get("offset"); v != "" {
		offset, err = strconv.Atoi(v)
		if err != nil || !syncstorage.OffsetOk(offset) {
			JSONError(w, "Invalid offset value", http.StatusBadRequest)
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
			JSONError(w, "Invalid sort value", http.StatusBadRequest)
			return
		}
	}

	// this is way down here since IO is more expensive
	// than parsing if the GET params are valid
	cmodified, err := s.db.GetCollectionModified(cId)
	if err != nil {
		InternalError(w, r, err)
		return
	} else if sentNotModified(w, r, cmodified) {
		return
	}

	results, err := s.db.GetBSOs(cId, ids, newer, sort, limit, offset)
	if err != nil {
		InternalError(w, r, err)
		return
	}
	m := syncstorage.ModifiedToString(cmodified)
	w.Header().Set("X-Last-Modified", m)

	w.Header().Set("X-Weave-Records", strconv.Itoa(results.Total))
	if results.More {
		w.Header().Set("X-Weave-Next-Offset", strconv.Itoa(results.Offset))
	}

	if full {
		JsonNewline(w, r, results.BSOs)
	} else {
		bsoIds := make([]string, len(results.BSOs))
		for i, b := range results.BSOs {
			bsoIds[i] = b.Id
		}
		JsonNewline(w, r, bsoIds)
	}
}

func (s *SyncUserHandler) hCollectionPOST(w http.ResponseWriter, r *http.Request) {
	// accept text/plain from old (broken) clients
	ct := r.Header.Get("Content-Type")

	if ct != "application/json" && ct != "text/plain" && ct != "application/newlines" {
		JSONError(w, "Not acceptable Content-Type", http.StatusUnsupportedMediaType)
		return
	}

	// a list of all the raw json encoded BSOs
	var raw []json.RawMessage

	if ct == "application/json" || ct == "text/plain" {
		decoder := json.NewDecoder(r.Body)
		err := decoder.Decode(&raw)
		if err != nil {
			WeaveInvalidWBOError(w, r)
			return
		}
	} else { // deal with application/newlines
		raw = []json.RawMessage{}
		scanner := bufio.NewScanner(r.Body)
		for scanner.Scan() {
			bsoBytes := scanner.Bytes()
			raw = append(raw, bsoBytes)
		}
	}

	// bsoToBeProcessed will actually get sent to the DB
	bsoToBeProcessed := syncstorage.PostBSOInput{}
	results := syncstorage.NewPostResults(syncstorage.Now())

	for _, rawJSON := range raw {
		var b syncstorage.PutBSOInput
		if err := parseIntoBSO(rawJSON, &b); err == nil {
			bsoToBeProcessed = append(bsoToBeProcessed, &b)
		} else {
			// ignore empty whitespace lines from application/newlines
			if len(strings.TrimSpace(string(rawJSON))) == 0 {
				continue
			}

			// couldn't parse a BSO into something real
			// abort immediately
			if err.field == "-" { // json error, not an object
				WeaveInvalidWBOError(w, r)
				return
			}

			results.AddFailure(err.bId, fmt.Sprintf("invalid %s", err.field))
		}
	}

	if len(bsoToBeProcessed) > BATCH_MAX_IDS {
		JSONError(w, fmt.Sprintf("Exceeded %d BSO per request", BATCH_MAX_IDS),
			http.StatusRequestEntityTooLarge)
		return
	}

	cId, err := s.getcid(r, true) // automake the collection if it doesn't exit
	if err != nil {
		if err == syncstorage.ErrInvalidCollectionName {
			JSONError(w, err.Error(), http.StatusBadRequest)
		} else {
			InternalError(w, r, err)
		}
		return
	}

	cmodified, err := s.db.GetCollectionModified(cId)
	if err != nil {
		InternalError(w, r, err)
		return
	} else if sentNotModified(w, r, cmodified) {
		return
	}

	// change posted[].TTL from seconds (what clients send)
	// to milliseconds (what the DB uses)
	for _, p := range bsoToBeProcessed {
		if p.TTL != nil {
			tmp := *p.TTL * 1000
			p.TTL = &tmp
		}
	}

	// Send the changes to the database and merge
	// with `results` above
	postResults, err := s.db.PostBSOs(cId, bsoToBeProcessed)

	if err != nil {
		InternalError(w, r, err)
	} else {
		m := syncstorage.ModifiedToString(postResults.Modified)

		for bsoId, failMessage := range postResults.Failed {
			results.Failed[bsoId] = failMessage
		}

		w.Header().Set("X-Last-Modified", m)
		JsonNewline(w, r, &PostResults{
			Modified: m,
			Success:  postResults.Success,
			Failed:   results.Failed,
		})
	}
}

func (s *SyncUserHandler) hCollectionDELETE(w http.ResponseWriter, r *http.Request) {

	cId, err := s.getcid(r, false)

	if err != nil {
		if err == syncstorage.ErrNotFound {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{"modified":%s}`, syncstorage.ModifiedToString(syncstorage.Now()))
			return
		} else {
			InternalError(w, r, err)
		}
		return
	}

	cmodified, err := s.db.GetCollectionModified(cId)
	if err != nil {
		InternalError(w, r, err)
		return
	} else if sentNotModified(w, r, cmodified) {
		return
	}

	modified := syncstorage.Now()
	bids, idExists := r.URL.Query()["ids"]
	if idExists {

		bidlist := strings.Split(bids[0], ",")

		if len(bidlist) > BATCH_MAX_IDS {
			JSONError(w, "exceeded max batch size", http.StatusBadRequest)
			return
		}

		modified, err = s.db.DeleteBSOs(cId, bidlist...)
		if err != nil {
			InternalError(w, r, err)
			return
		}
	} else {
		err = s.db.DeleteCollection(cId)
		if err != nil {
			InternalError(w, r, err)
			return
		}
	}

	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"modified":%s}`, syncstorage.ModifiedToString(modified))
}

func (s *SyncUserHandler) hBsoGET(w http.ResponseWriter, r *http.Request) {

	if !AcceptHeaderOk(w, r) {
		return
	}

	var (
		bId string
		ok  bool
		cId int
		err error
		bso *syncstorage.BSO
	)

	if bId, ok = extractBsoIdFail(w, r); !ok {
		return
	}

	cId, err = s.getcid(r, false)

	if err != nil {
		if err == syncstorage.ErrNotFound {
			JSONError(w, "Collection Not Found", http.StatusNotFound)
		} else {
			InternalError(w, r, err)
		}
		return
	}

	if bso, err = s.db.GetBSO(cId, bId); err == nil {

		if sentNotModified(w, r, bso.Modified) {
			return
		}

		log.WithFields(log.Fields{
			"bso_t": bso.TTL,
			"now":   syncstorage.Now(),
			"diff":  syncstorage.Now() - bso.TTL,
		}).Debug("bso-expired")

		m := syncstorage.ModifiedToString(bso.Modified)
		w.Header().Set("X-Last-Modified", m)
		JsonNewline(w, r, bso)
	} else {
		if err == syncstorage.ErrNotFound {
			JSONError(w, "BSO Not Found", http.StatusNotFound)
		} else {
			InternalError(w, r, err)
		}
	}
}

func (s *SyncUserHandler) hBsoPUT(w http.ResponseWriter, r *http.Request) {
	if !AcceptHeaderOk(w, r) {
		return
	}

	// accept text/plain from old (broken) clients
	ct := r.Header.Get("Content-Type")
	if ct != "application/json" && ct != "text/plain" && ct != "application/newlines" {
		JSONError(w, "Not acceptable Content-Type", http.StatusUnsupportedMediaType)
		return
	}

	var (
		bId      string
		ok       bool
		cId      int
		modified int
		err      error
	)

	if bId, ok = extractBsoIdFail(w, r); !ok {
		return
	}

	cId, err = s.getcid(r, true)
	if err != nil {
		InternalError(w, r, err)
		return
	}

	modified, err = s.db.GetBSOModified(cId, bId)
	if err != nil {
		if err != syncstorage.ErrNotFound {
			InternalError(w, r, errors.Wrap(err, "Could not get Modified ts"))
			return
		}
	}

	if sentNotModified(w, r, modified) {
		return
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		InternalError(w, r, errors.New("PUT could not read JSON body"))
		return
	}

	var bso syncstorage.PutBSOInput
	if err := parseIntoBSO(body, &bso); err != nil {
		WeaveInvalidWBOError(w, r)
		return
	}

	// change bso.TTL to milliseconds (what the db uses)
	// from seconds (what client's send)
	if bso.TTL != nil {
		tmp := *bso.TTL * 1000
		bso.TTL = &tmp
	}

	modified, err = s.db.PutBSO(cId, bId, bso.Payload, bso.SortIndex, bso.TTL)

	if err != nil {
		if err == syncstorage.ErrPayloadTooBig {
			JSONError(w,
				http.StatusText(http.StatusRequestEntityTooLarge),
				http.StatusRequestEntityTooLarge)
			return
		}

		JSONError(w, err.Error(), http.StatusBadRequest)
		return
	}
	m := syncstorage.ModifiedToString(modified)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Last-Modified", m)
	w.Write([]byte(m))
}

func (s *SyncUserHandler) hBsoDELETE(w http.ResponseWriter, r *http.Request) {
	var (
		bId      string
		ok       bool
		cId      int
		modified int
		err      error
	)

	if bId, ok = extractBsoIdFail(w, r); !ok {
		return
	}

	cId, err = s.getcid(r, false)
	if err == syncstorage.ErrNotFound {
		JSONError(w, "Collection Not Found", http.StatusNotFound)
		return
	}

	// Trying to delete a BSO that is not there
	// should 404
	bso, err := s.db.GetBSO(cId, bId)
	if err != nil {
		if err == syncstorage.ErrNotFound {
			JSONError(w, fmt.Sprintf("BSO id: %s Not Found", bId), http.StatusNotFound)
		} else {
			InternalError(w, r, err)
		}
		return
	}

	if sentNotModified(w, r, bso.Modified) {
		return
	}

	modified, err = s.db.DeleteBSO(cId, bso.Id)

	if err != nil {
		InternalError(w, r, err)
	} else {
		m := syncstorage.ModifiedToString(modified)
		w.Header().Set("Content-Type", "text/plain")
		w.Header().Set("X-Last-Modified", m)
		w.Write([]byte(m))
	}
}

func (s *SyncUserHandler) hDeleteEverything(w http.ResponseWriter, r *http.Request) {
	err := s.db.DeleteEverything()
	if err != nil {
		InternalError(w, r, err)
	} else {
		m := syncstorage.ModifiedToString(syncstorage.Now())
		w.Header().Set("Content-Type", "text/plain")
		w.Header().Set("X-Last-Modified", m)
		w.Write([]byte(m))
	}
}
