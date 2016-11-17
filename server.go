package main

import (
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"

	"go.mozilla.org/hawk"

	log "github.com/Sirupsen/logrus"
	"github.com/facebookgo/httpdown"

	"github.com/mozilla-services/go-syncstorage/config"
	"github.com/mozilla-services/go-syncstorage/syncstorage"
	"github.com/mozilla-services/go-syncstorage/web"
)

func init() {
	switch config.Log.Level {
	case "fatal":
		log.SetLevel(log.FatalLevel)
	case "error":
		log.SetLevel(log.ErrorLevel)
	case "warn":
		log.SetLevel(log.WarnLevel)
	case "debug":
		log.SetLevel(log.DebugLevel)
	default:
		log.SetLevel(log.InfoLevel)
	}
}

func main() {

	hawk.MaxTimestampSkew = time.Second * time.Duration(config.HawkTimestampMaxSkew)

	syncLimitConfig := web.NewDefaultSyncUserHandlerConfig()
	syncLimitConfig.MaxRequestBytes = config.Limit.MaxRequestBytes
	syncLimitConfig.MaxBSOGetLimit = config.Limit.MaxBSOGetLimit
	syncLimitConfig.MaxPOSTRecords = config.Limit.MaxPOSTRecords
	syncLimitConfig.MaxPOSTBytes = config.Limit.MaxPOSTBytes
	syncLimitConfig.MaxTotalBytes = config.Limit.MaxTotalBytes
	syncLimitConfig.MaxTotalRecords = config.Limit.MaxTotalRecords
	syncLimitConfig.MaxBatchTTL = config.Limit.MaxBatchTTL * 1000
	syncLimitConfig.MaxRecordPayloadBytes = config.Limit.MaxRecordPayloadBytes

	// The base functionality is the sync 1.5 api
	poolHandler := web.NewSyncPoolHandler(&web.SyncPoolConfig{
		Basepath:      config.DataDir,
		NumPools:      config.Pool.Num,
		MaxPoolSize:   config.Pool.MaxSize,
		VacuumKB:      config.Pool.VacuumKB,
		DBConfig:      &syncstorage.Config{config.Sqlite.CacheSize},
		PurgeMinHours: config.Pool.PurgeMinHours,
		PurgeMaxHours: config.Pool.PurgeMaxHours,
	}, syncLimitConfig)

	var router http.Handler
	router = poolHandler

	if config.InfoCacheSize > 0 {
		router = web.NewCacheHandler(router, web.CacheConfig{MaxCacheSize: config.InfoCacheSize})
	}

	// legacy weave hacks
	router = web.NewWeaveHandler(router)

	// All sync 1.5 access requires Hawk Authorization
	router = web.NewHawkHandler(router, config.Secrets)

	// Serve non sync 1.5 endpoints
	router = web.NewInfoHandler(router)

	// Log all the things
	if config.Log.DisableHTTP != true {
		logHandler := web.NewLogHandler(log.StandardLogger(), router)

		if config.Log.OnlyHTTPErrors {
			h := logHandler.(*web.LoggingHandler)
			h.OnlyHTTPErrors = true
		}

		router = logHandler
	}

	if config.EnablePprof {
		log.Info("Enabling pprof profile at /debug/pprof/")
		router = web.NewPprofHandler(router)
	}

	listenOn := config.Host + ":" + strconv.Itoa(config.Port)
	server := &http.Server{
		Addr:    listenOn,
		Handler: router,
	}

	if config.Log.Mozlog {
		log.SetFormatter(&web.MozlogFormatter{
			Hostname: config.Hostname,
			Pid:      os.Getpid(),
		})
	}

	hd := &httpdown.HTTP{
		// how long until connections are force closed
		StopTimeout: 3 * time.Minute,

		// how long before complete abort (even when clients are connected)
		// this is above StopTimeout. In other worse, how much time to give
		// force stopping of connections to finish
		KillTimeout: 2 * time.Minute,
	}

	log.WithFields(log.Fields{
		"addr":                           listenOn,
		"PID":                            os.Getpid(),
		"POOL_NUM":                       config.Pool.Num,
		"POOL_MAX_SIZE":                  config.Pool.MaxSize,
		"POOL_VACUUM_KB":                 config.Pool.VacuumKB,
		"POOL_PURGE_MIN_HOURS":           config.Pool.PurgeMinHours,
		"POOL_PURGE_MAX_HOURS":           config.Pool.PurgeMaxHours,
		"LIMIT_MAX_BSO_GET_LIMIT":        syncLimitConfig.MaxBSOGetLimit,
		"LIMIT_MAX_POST_RECORDS":         syncLimitConfig.MaxPOSTRecords,
		"LIMIT_MAX_POST_BYTES":           syncLimitConfig.MaxPOSTBytes,
		"LIMIT_MAX_TOTAL_RECORDS":        syncLimitConfig.MaxTotalRecords,
		"LIMIT_MAX_TOTAL_BYTES":          syncLimitConfig.MaxTotalBytes,
		"LIMIT_MAX_REQUEST_BYTES":        syncLimitConfig.MaxRequestBytes,
		"LIMIT_MAX_BATCH_TTL":            fmt.Sprintf("%d seconds", syncLimitConfig.MaxBatchTTL/1000),
		"LIMIT_MAX_RECORD_PAYLOAD_BYTES": syncLimitConfig.MaxRecordPayloadBytes,
		"SQLITE3_CACHE_SIZE":             config.Sqlite.CacheSize,
		"INFO_CACHE_SIZE":                config.InfoCacheSize,
		"HAWK_TIMESTAMP_MAX_SKEW":        hawk.MaxTimestampSkew.Seconds(),
	}).Info("HTTP Listening at " + listenOn)

	err := httpdown.ListenAndServe(server, hd)
	if err != nil {
		log.Error(err.Error())
	}

	poolHandler.StopHTTP()
}
