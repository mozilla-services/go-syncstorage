package main

import (
	"fmt"
	"os"

	"github.com/vrischmann/envconfig"
)

var Conf struct {
	Port    int
	Secrets []string

	// Where to save data
	DataDir string `envconfig:"default=/var/lib/sync"`

	// POOL_COUNT * POOL_OPEN_FILES = total number of sqlite
	// open file handlers are used at a time. Help keeps
	// a lid file handler usage.
	Pool struct {
		Count     int `envconfig:"default=8"`
		OpenFiles int `envconfig:"default=32,optional,POOL_OPEN_FILES"`
	}

	// TLS is optiona. If these are empty listens on HTTP
	Tls struct {
		Cert string `envconfig:"default=none"`
		Key  string `envconfig:"default=none"`
	}
}

func main() {

	if err := envconfig.Init(&Conf); err != nil {
		fmt.Printf("err=%s\n", err)
		os.Exit(1)
	}

	// validate configuration values

	// initialize dispatcher
	// initialize the HTTP context
	// Listen
}
