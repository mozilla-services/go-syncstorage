package main

import (
	"net/http"
	"strconv"

	log "github.com/Sirupsen/logrus"

	"github.com/mostlygeek/go-syncstorage/api"
	"github.com/mostlygeek/go-syncstorage/config"
	"github.com/mostlygeek/go-syncstorage/syncstorage"
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

type logHandler struct {
	handler http.Handler
}

func (h logHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {

}

func main() {

	// for now we will use a fixed number of pools
	// and spread config.MaxOpenFiles evenly among them
	numPools := uint16(8)
	cacheSize := int(uint16(config.MaxOpenFiles) / numPools)

	dispatch, err := syncstorage.NewDispatch(
		numPools, config.DataDir, syncstorage.TwoLevelPath, cacheSize)

	if err != nil {
		log.Fatal(err)
	}

	context, err := api.NewContext(config.Secrets, dispatch)
	if err != nil {
		log.Fatal(err)
	}

	router := api.NewRouterFromContext(context)
	loggedRouter := api.LogHandler(router)

	// set up additional handlers

	listenOn := ":" + strconv.Itoa(config.Port)
	if config.Tls.Cert != "" {
		log.WithFields(log.Fields{"addr": listenOn, "tls": true}).Info("HTTP Listening at " + listenOn)
		err := http.ListenAndServeTLS(
			listenOn, config.Tls.Cert, config.Tls.Key, loggedRouter)
		if err != nil {
			log.Fatal(err)
		}
	} else {
		log.WithFields(log.Fields{"addr": listenOn, "tls": false}).Info("HTTP Listening at " + listenOn)
		err := http.ListenAndServe(listenOn, loggedRouter)
		if err != nil {
			log.Fatal(err)
		}
	}

}
