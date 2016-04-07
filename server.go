package main

import (
	"net/http"
	"strconv"
	"time"

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

func main() {

	numPools := uint16(8)
	dispatch, err := syncstorage.NewDispatch(
		numPools, config.DataDir, 5*time.Minute)

	if err != nil {
		log.Fatal(err)
	}

	context, err := api.NewContext(config.Secrets, dispatch)
	if err != nil {
		log.Fatal(err)
	}

	router := api.NewRouterFromContext(context)
	router = api.LogHandler(router)

	// set up additional handlers

	listenOn := config.Host + ":" + strconv.Itoa(config.Port)
	if config.Tls.Cert != "" {
		log.WithFields(log.Fields{"addr": listenOn, "tls": true}).Info("HTTP Listening at " + listenOn)
		err := http.ListenAndServeTLS(
			listenOn, config.Tls.Cert, config.Tls.Key, router)
		if err != nil {
			log.Fatal(err)
		}
	} else {
		log.WithFields(log.Fields{"addr": listenOn, "tls": false}).Info("HTTP Listening at " + listenOn)
		err := http.ListenAndServe(listenOn, router)
		if err != nil {
			log.Fatal(err)
		}
	}

}
