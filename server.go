package main

import (
	"net/http"
	"os"
	"strconv"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/facebookgo/httpdown"

	"github.com/mostlygeek/go-syncstorage/config"
	"github.com/mostlygeek/go-syncstorage/web"
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

	var router http.Handler

	// The base functionality is the sync 1.5 api + legacy weave hacks
	poolHandler := web.NewSyncPoolHandler(config.DataDir, 1, config.TTL)
	router = web.NewWeaveHandler(poolHandler)

	// All sync 1.5 access requires Hawk Authorization
	router = web.NewHawkHandler(router, config.Secrets)

	// Serve non sync 1.5 endpoints
	router = web.NewInfoHandler(router)

	// Log all the things
	router = web.NewLogHandler(router)

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
		"addr": listenOn,
		"PID":  os.Getpid(),
		"TTL":  config.TTL,
	}).Info("HTTP Listening at " + listenOn)

	err := httpdown.ListenAndServe(server, hd)
	if err != nil {
		log.Error(err.Error())
	}

	poolHandler.StopHTTP()
}
