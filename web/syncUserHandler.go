package web

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
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

type SyncUserHandlerConfig struct {
	// Over rides
	MaxBSOGetLimit int

	// API Limits
	MaxRequestBytes       int
	MaxPOSTRecords        int
	MaxPOSTBytes          int
	MaxTotalRecords       int
	MaxTotalBytes         int
	MaxBatchTTL           int
	MaxRecordPayloadBytes int // largest BSO payload
}

func NewDefaultSyncUserHandlerConfig() *SyncUserHandlerConfig {
	return &SyncUserHandlerConfig{
		// API Limits
		MaxBSOGetLimit:        1000,
		MaxRequestBytes:       2 * 1024 * 1024,
		MaxPOSTRecords:        100,
		MaxPOSTBytes:          2 * 1024 * 1024,
		MaxTotalRecords:       1000,
		MaxTotalBytes:         20 * 1024 * 1024,
		MaxRecordPayloadBytes: 1024 * 256,

		// batches older than this are likely to be purged
		MaxBatchTTL: 2 * 60 * 60 * 1000, // 2 hours in milliseconds
	}
}

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

	config *SyncUserHandlerConfig
}

func NewSyncUserHandler(uid string, db *syncstorage.DB, config *SyncUserHandlerConfig) *SyncUserHandler {

	// https://docs.services.mozilla.com/storage/apis-1.5.html
	r := mux.NewRouter()

	if config == nil {
		config = NewDefaultSyncUserHandlerConfig()
	}

	server := &SyncUserHandler{
		uid:    uid,
		router: r,
		db:     db,
		config: config,
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
	info.HandleFunc("/configuration", server.hInfoConfiguration).Methods("GET")
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

// TidyUp will purge expired BSOs and Batches. When the database has exceeded
// vacuumKB (in kilobytes) it will be optimized. This could
// potentially be a long operation as the database vacuumed needs to rewrite
// the entire database file
func (s *SyncUserHandler) TidyUp(minPurge, maxPurge time.Duration, vacuumKB int) (skipped bool, took time.Duration, err error) {
	// Purge Expired BSOs
	start := time.Now()

	nextStr, err := s.db.GetKey("NEXT_PURGE")
	if err != nil {
		log.WithFields(log.Fields{
			"uid": s.uid,
			"err": err.Error(),
		}).Error("SyncUserHandler - Error Fetching next purge time")
		return true, time.Since(start), err
	}

	if nextStr != "" {
		nextPurge, err := time.Parse(time.RFC3339Nano, nextStr)
		if err != nil {
			log.WithFields(log.Fields{
				"uid": s.uid,
				"err": err.Error(),
			}).Error("SyncUserHandler - Error parsing NEXT_PURGE value")

			// try to fix it for next time
			s.db.SetKey("NEXT_PURGE", time.Now().Format(time.RFC3339Nano))
			return true, time.Since(start), nil
		}

		if time.Now().Before(nextPurge) {
			if log.GetLevel() == log.DebugLevel {
				log.WithFields(log.Fields{
					"purge_valid_in": nextPurge.Sub(time.Now()).String(),
				}).Debug("SyncUserHandler: Skipping TidyUp")
			}
			return true, took, nil
		}
	} else {
		// never been purged, skip it and set it to the maxpurge time in the future
		nextPurge := time.Now().Add(maxPurge)
		err = s.db.SetKey("NEXT_PURGE", nextPurge.Format(time.RFC3339Nano))
		return true, time.Since(start), err
	}

	numBSOPurged, err := s.db.PurgeExpired()
	if err != nil {
		log.WithFields(log.Fields{
			"uid": s.uid,
			"err": err.Error(),
		}).Error("SyncUserHandler - Error purging expired BSOs")
		return
	}

	numBatchesPurged, err := s.db.BatchPurge(s.config.MaxBatchTTL)
	if err != nil {
		log.WithFields(log.Fields{
			"uid": s.uid,
			"err": err.Error(),
		}).Error("SyncUserHandler - Error purging expired Batches")
		return
	}

	usage, err := s.db.Usage()
	if err != nil {
		log.WithFields(log.Fields{
			"uid": s.uid,
			"err": err.Error(),
		}).Error("SyncUserHandler - Error retrieving usage")
		return
	}
	freeKB := (usage.Free * usage.Size / 1024)

	log.WithFields(log.Fields{
		"uid":            s.uid,
		"bsos_purged":    numBSOPurged,
		"batches_purged": numBatchesPurged,
		"t":              (time.Since(start).Nanoseconds() / 1000 / 1000),
		"free_kb":        freeKB,
	}).Info("SyncUserHandler - Purge OK")

	if vacuumKB > 0 && freeKB >= vacuumKB {
		if err = s.db.Vacuum(); err != nil {
			log.WithFields(log.Fields{
				"uid": s.uid,
				"err": err.Error(),
			}).Error("SyncUserHandler - Error Vacuuming DB")
			return
		}

		after, err := s.db.Usage()
		if err != nil {
			log.WithFields(log.Fields{
				"uid": s.uid,
				"err": err.Error(),
			}).Error("SyncUserHandler - Error retrieving usage after vacuum")
			return true, time.Since(start), err
		}

		beforeSz := usage.Total * usage.Size / 1024
		afterSz := after.Total * after.Size / 1024
		log.WithFields(log.Fields{
			"uid":       s.uid,
			"t":         (time.Since(start).Nanoseconds() / 1000 / 1000),
			"before_kb": beforeSz,
			"after_kb":  afterSz,
			"freed_kb":  beforeSz - afterSz,
		}).Info("SyncUserHandler - Vacuum OK")
	}

	deltaTime := minPurge
	if maxPurge.Nanoseconds() > 0 {
		deltaTime = time.Duration(rand.Int63n(maxPurge.Nanoseconds()))
	}

	if deltaTime < minPurge {
		deltaTime = minPurge
	}

	nextPurge := time.Now().Add(deltaTime)
	err = s.db.SetKey("NEXT_PURGE", nextPurge.Format(time.RFC3339Nano))

	if err != nil {
		log.WithFields(log.Fields{
			"uid": s.uid,
			"err": err.Error(),
		}).Error("SyncUserHandler - Error Setting Next Purge Key")
		return
	}

	took = time.Since(start)
	return
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
	s.db.Close()

	if log.GetLevel() == log.DebugLevel {
		log.WithFields(log.Fields{
			"uid": s.uid,
		}).Debug("syncUserHandler stopped")
	}
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
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, "{")
		num := len(info)
		for name, modified := range info {
			fmt.Fprintf(w, `"%s":%s`, name, syncstorage.ModifiedToString(modified))
			num--
			if num != 0 {
				fmt.Fprint(w, ",")
			}
		}
		fmt.Fprint(w, "}")
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

func (s *SyncUserHandler) hInfoConfiguration(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{
		"max_post_records":%d,
		"max_post_bytes":%d,
		"max_total_records":%d,
		"max_total_bytes":%d,
		"max_request_bytes":%d,
	    "max_record_payload_bytes":%d}`,
		s.config.MaxPOSTRecords,
		s.config.MaxPOSTBytes,
		s.config.MaxTotalRecords,
		s.config.MaxTotalBytes,
		s.config.MaxRequestBytes,
		s.config.MaxRecordPayloadBytes,
	)
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
		sendRequestProblem(w, r, http.StatusBadRequest, errors.Wrap(err, "Bad query parameters"))
		return
	}

	if v := r.Form.Get("ids"); v != "" {
		ids = strings.Split(v, ",")

		if len(ids) > s.config.MaxPOSTRecords {
			sendRequestProblem(w, r, http.StatusBadRequest, errors.New("Exceeded max batch size"))
			return
		}

		for i, id := range ids {
			id = strings.TrimSpace(id)
			if syncstorage.BSOIdOk(id) {
				ids[i] = id
			} else {
				sendRequestProblem(w, r, http.StatusBadRequest, errors.Errorf("Invalid bso id %s", id))
				return
			}
		}

		if len(ids) > 100 {
			sendRequestProblem(w, r, http.StatusBadRequest, errors.New("Too many ids provided"))
			return
		}
	}

	// we expect to get sync's two decimal timestamps, these need
	// to be converted to milliseconds
	if v := r.Form.Get("newer"); v != "" {
		floatNew, err := strconv.ParseFloat(v, 64)
		if err != nil {
			sendRequestProblem(w, r, http.StatusBadRequest, errors.Wrap(err, "Invalid newer param format"))
			return
		}

		newer = int(floatNew * 1000)
		if !syncstorage.NewerOk(newer) {
			sendRequestProblem(w, r, http.StatusBadRequest, errors.New("Invalid newer value"))
			return
		}
	}

	if v := r.Form.Get("full"); v != "" {
		full = true
	}

	if v := r.Form.Get("limit"); v != "" {
		limit, err = strconv.Atoi(v)
		if err != nil || !syncstorage.LimitOk(limit) {
			errMessage := "Invalid limit value"
			if err != nil {
				err = errors.Wrap(err, errMessage)
			} else {
				err = errors.New(errMessage)
			}
			sendRequestProblem(w, r, http.StatusBadRequest, err)
			return
		}
	}

	// assign a default value for limit if nothing is supplied
	if limit <= 0 || limit > s.config.MaxBSOGetLimit {
		limit = s.config.MaxBSOGetLimit
	}

	if v := r.Form.Get("offset"); v != "" {
		offset, err = strconv.Atoi(v)
		if err != nil || !syncstorage.OffsetOk(offset) {
			errMessage := "Invalid offset value"
			if err != nil {
				err = errors.Wrap(err, errMessage)
			} else {
				err = errors.New(errMessage)
			}
			sendRequestProblem(w, r, http.StatusBadRequest, err)
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
			sendRequestProblem(w, r, http.StatusBadRequest, errors.New("Invalid sort value"))
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
	ct := getMediaType(r.Header.Get("Content-Type"))
	if ct != "application/json" && ct != "text/plain" && ct != "application/newlines" {
		sendRequestProblem(w, r, http.StatusUnsupportedMediaType, errors.Errorf("Not acceptable Content-Type: %s", ct))
		return
	}

	cId, err := s.getcid(r, true) // automake the collection if it doesn't exit
	if err != nil {
		if err == syncstorage.ErrInvalidCollectionName {
			sendRequestProblem(w, r, http.StatusBadRequest, errors.Wrap(err, "Invalid collection name"))
		} else {
			InternalError(w, r, err)
		}
		return
	}

	// handle X-If-Unmodified-Since and X-If-Modified-Since
	cmodified, err := s.db.GetCollectionModified(cId)
	if err != nil {
		InternalError(w, r, err)
		return
	} else if sentNotModified(w, r, cmodified) {
		return
	}

	batchFound, batchId, batchCommit := GetBatchIdAndCommit(r)
	if batchCommit && !batchFound {
		sendRequestProblem(w, r, http.StatusBadRequest, errors.New("Batch ID expected with commit"))
		return
	} else if batchId != "" || (batchId == "true" && batchCommit == false) {
		s.hCollectionPOSTBatch(cId, w, r)
	} else {
		s.hCollectionPOSTClassic(cId, w, r)
	}
}

// hCollectionPOSTClassic is the historical POST handling logic prior to
// the addition of atomic commits from multiple POST requests
func (s *SyncUserHandler) hCollectionPOSTClassic(collectionId int, w http.ResponseWriter, r *http.Request) {

	bsoToBeProcessed, results, err := RequestToPostBSOInput(r, s.config.MaxRecordPayloadBytes)
	if err != nil {
		WeaveInvalidWBOError(w, r, errors.Wrap(err, "Failed turning POST body into BSO work list"))
		return
	}

	if len(bsoToBeProcessed) > s.config.MaxPOSTRecords {
		sendRequestProblem(w, r, http.StatusRequestEntityTooLarge,
			errors.Errorf("Exceed %d BSO per request", s.config.MaxPOSTRecords))
		return
	}

	// Send the changes to the database and merge
	// with `results` above
	postResults, err := s.db.PostBSOs(collectionId, bsoToBeProcessed)

	if err != nil {
		InternalError(w, r, err)
	} else {
		for bsoId, failMessage := range postResults.Failed {
			results.Failed[bsoId] = failMessage
		}

		w.Header().Set("X-Last-Modified", syncstorage.ModifiedToString(postResults.Modified))
		JsonNewline(w, r, &PostResults{
			Modified: postResults.Modified,
			Success:  postResults.Success,
			Failed:   results.Failed,
		})
	}
}

// hCollectionPOSTBatch handles batch=? requests. It is called internally by hCollectionPOST
// to handle batch request logic
func (s *SyncUserHandler) hCollectionPOSTBatch(collectionId int, w http.ResponseWriter, r *http.Request) {

	// CHECK client provided headers to quickly determine if batch exceeds limits
	// this is meant to be a cheap(er) check without actually having to parse the
	// data provided by the user
	for _, headerName := range []string{"X-Weave-Total-Records", "X-Weave-Total-Bytes", "X-Weave-Records", "X-Weave-Bytes"} {
		if strVal := r.Header.Get(headerName); strVal != "" {
			if intVal, err := strconv.Atoi(strVal); err == nil {
				max := 0
				switch headerName {
				case "X-Weave-Total-Records":
					max = s.config.MaxTotalRecords
				case "X-Weave-Total-Bytes":
					max = s.config.MaxTotalBytes
				case "X-Weave-Bytes":
					max = s.config.MaxPOSTBytes
				case "X-Weave-Records":
					max = s.config.MaxPOSTRecords
				}

				if intVal > max {
					WeaveSizeLimitExceeded(w, r,
						errors.Errorf("Limit %s exceed. %d/%d", headerName, intVal, max))
					return
				}
			} else {
				// header value is invalid (not an int)
				sendRequestProblem(w, r, http.StatusBadRequest, errors.Errorf("Invalid integer value for %s", headerName))
				return
			}
		}
	}

	// CHECK the POST size, if possible from client supplied data
	// hopefully shortcut a fail if this exceeds limits
	if r.ContentLength > 0 && r.ContentLength > int64(s.config.MaxPOSTBytes) {
		WeaveSizeLimitExceeded(w, r,
			errors.Errorf("MaxPOSTBytes exceeded in request.ContentLength(%d) > %d",
				r.ContentLength, s.config.MaxPOSTBytes))
		return
	}

	// EXTRACT actual data to check
	bsoToBeProcessed, results, err := RequestToPostBSOInput(r, s.config.MaxRecordPayloadBytes)
	if err != nil {
		WeaveInvalidWBOError(w, r, errors.Wrap(err, "Failed turning POST body into BSO work list"))
		return
	}

	// CHECK actual BSOs sent to see if they exceed limits
	if len(bsoToBeProcessed) > s.config.MaxPOSTRecords {
		sendRequestProblem(w, r, http.StatusRequestEntityTooLarge,
			errors.Errorf("Exceeded %d BSO per request", s.config.MaxPOSTRecords))
		return
	}

	// CHECK BSO validation errors. Don't even start a Batch if there are.
	if len(results.Failed) > 0 {
		modified := syncstorage.Now()
		w.Header().Set("X-Last-Modified", syncstorage.ModifiedToString(modified))
		JsonNewline(w, r, &PostResults{
			Modified: modified,
			Success:  nil,
			Failed:   results.Failed,
		})
		return
	}

	// Get batch id, commit command and internal collection Id
	_, batchId, batchCommit := GetBatchIdAndCommit(r)

	// CHECK batch id is valid for appends. Do this before loading and decoding
	// the body to be more efficient.
	if batchId != "true" {
		id, err := batchIdInt(batchId)
		if err != nil {
			sendRequestProblem(w, r, http.StatusBadRequest, errors.Wrap(err, "Invalid Batch ID Format"))
			return
		}

		if _, err := s.db.BatchExists(id, collectionId); err != nil {
			if err == syncstorage.ErrNotFound {
				sendRequestProblem(w, r, http.StatusBadRequest,
					errors.Wrapf(err, "Batch id: %s does not exist", batchId))
			} else {
				InternalError(w, r, err)
			}
			return
		}
	}

	// JSON Serialize the data for storage in the DB
	buf := new(bytes.Buffer)
	if len(bsoToBeProcessed) > 0 {
		encoder := json.NewEncoder(buf)
		for _, bso := range bsoToBeProcessed {
			if err := encoder.Encode(bso); err != nil { // Note: this writes a newline after each record
				// whoa... presumably should never happen
				InternalError(w, r, errors.Wrap(err, "Failed encoding BSO for payload"))
				return
			}
		}
	}

	// Save either as a new batch or append to an existing batch
	var dbBatchId int
	//   - batchIdInt used to track the internal batchId number in the database after
	//   - the create || append
	if batchId == "true" {
		newBatchId, err := s.db.BatchCreate(collectionId, buf.String())
		if err != nil {
			InternalError(w, r, errors.Wrap(err, "Failed creating batch"))
			return
		}

		dbBatchId = newBatchId

	} else {
		id, err := batchIdInt(batchId)
		if err != nil {
			sendRequestProblem(w, r, http.StatusBadRequest, errors.Wrap(err, "Invalid Batch ID Format"))
			return
		}

		if len(bsoToBeProcessed) > 0 { // append only if something to do
			if err := s.db.BatchAppend(id, collectionId, buf.String()); err != nil {
				InternalError(w, r, errors.Wrap(err, fmt.Sprintf("Failed append to batch id:%d", dbBatchId)))
				return
			}
		}

		dbBatchId = id
	}

	if batchCommit {
		batchRecord, err := s.db.BatchLoad(dbBatchId, collectionId)
		if err != nil {
			InternalError(w, r, errors.Wrap(err, "Failed Loading Batch to commit"))
			return
		}

		rawJSON := ReadNewlineJSON(bytes.NewBufferString(batchRecord.BSOS))

		// CHECK final data before committing it to the database
		numInBatch := len(rawJSON)
		if numInBatch > s.config.MaxTotalRecords {
			s.db.BatchRemove(dbBatchId)
			WeaveSizeLimitExceeded(w, r,
				errors.Errorf("Too many BSOs (%d) in Batch(%d)", numInBatch, dbBatchId))
			return
		}

		postData := make(syncstorage.PostBSOInput, len(rawJSON), len(rawJSON))
		for i, bsoJSON := range rawJSON {
			var bso syncstorage.PutBSOInput
			if parseErr := parseIntoBSO(bsoJSON, &bso); parseErr != nil {
				// well there is definitely a bug somewhere if this happens
				InternalError(w, r, errors.Wrap(parseErr, "Could not decode batch data"))
				return
			} else {
				postData[i] = &bso
			}
		}

		// CHECK that actual Batch data size
		sum := 0
		for _, bso := range postData {
			if bso.Payload == nil {
				continue
			}

			sum := sum + len(*bso.Payload)
			if sum > s.config.MaxTotalBytes {
				s.db.BatchRemove(dbBatchId)
				WeaveSizeLimitExceeded(w, r,
					errors.Errorf("Batch size(%d) exceeded MaxTotalBytes limit(%d)",
						sum, s.config.MaxTotalBytes))

				return
			}
		}

		postResults, err := s.db.PostBSOs(collectionId, postData)
		if err != nil {
			InternalError(w, r, err)
			return
		}

		// DELETE the batch from the DB
		s.db.BatchRemove(dbBatchId)

		w.Header().Set("X-Last-Modified", syncstorage.ModifiedToString(postResults.Modified))
		JsonNewline(w, r, &PostResults{
			Modified: postResults.Modified,
			Success:  postResults.Success,
			Failed:   postResults.Failed,
		})
	} else {
		modified := syncstorage.Now()
		w.Header().Set("X-Last-Modified", syncstorage.ModifiedToString(modified))
		JsonNewline(w, r, &PostResults{
			Batch:    batchIdString(dbBatchId),
			Modified: modified,
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

		if len(bidlist) > s.config.MaxPOSTRecords {
			sendRequestProblem(w, r, http.StatusBadRequest, errors.New("Exceeded max batch size"))
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
			sendRequestProblem(w, r, http.StatusNotFound, errors.Wrap(err, "Collection Not Found"))
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
			sendRequestProblem(w, r, http.StatusNotFound, errors.Wrap(err, "BSO Not Found"))
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
	ct := getMediaType(r.Header.Get("Content-Type"))
	if ct != "application/json" && ct != "text/plain" && ct != "application/newlines" {
		sendRequestProblem(w, r, http.StatusUnsupportedMediaType, errors.Errorf("Not acceptable Content-Type: %s", ct))
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
		WeaveInvalidWBOError(w, r, errors.Wrap(err, "Could not parse body into BSO"))
		return
	}

	if bso.Payload != nil && len(*bso.Payload) > s.config.MaxRecordPayloadBytes {
		sendRequestProblem(w, r,
			http.StatusRequestEntityTooLarge,
			errors.New("Payload too big"))
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
		sendRequestProblem(w, r, http.StatusBadRequest, err)
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
		sendRequestProblem(w, r, http.StatusNotAcceptable, errors.Wrap(err, "Could not find collection"))
		return
	}

	// Trying to delete a BSO that is not there
	// should 404
	bso, err := s.db.GetBSO(cId, bId)
	if err != nil {
		if err == syncstorage.ErrNotFound {
			sendRequestProblem(w, r, http.StatusNotFound, errors.Errorf("BSO id: %s Not Found", bId))
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
