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

	listenOn := ":" + strconv.Itoa(config.Port)
	if config.Tls.Cert != "" {
		log.Printf("Listening for TLS+HTTP on port %s", listenOn)
		err := http.ListenAndServeTLS(
			listenOn, config.Tls.Cert, config.Tls.Key, router)
		if err != nil {
			log.Fatal(err)
		}
	} else {
		log.Printf("Listening for HTTP on port %s", listenOn)
		err := http.ListenAndServe(listenOn, router)
		if err != nil {
			log.Fatal(err)
		}
	}

}
