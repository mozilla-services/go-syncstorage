package main

import (
	"net/http"
	"os"
	"strconv"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/facebookgo/httpdown"

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
	server := &http.Server{
		Addr:    listenOn,
		Handler: router,
	}

	hd := &httpdown.HTTP{
		// how long until connections are force closed
		StopTimeout: 3 * time.Minute,

		// how long before complete abort (even when clients are connected)
		// this is above StopTimeout. In other worse, how much time to give
		// force stopping of connections to finish
		KillTimeout: 2 * time.Minute,
	}

	log.WithFields(log.Fields{"addr": listenOn, "tls": false, "PID": os.Getpid()}).Info("HTTP Listening at " + listenOn)

	// TODO add in TLS support

	err = httpdown.ListenAndServe(server, hd)
	if err != nil {
		log.Error(err.Error())
	}

	// start closing all the database connections
	dispatch.Shutdown()

}
