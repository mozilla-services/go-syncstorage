package config

import (
	"os"
	"path/filepath"
	"runtime"

	log "github.com/Sirupsen/logrus"

	"github.com/vrischmann/envconfig"
)

type LogConfig struct {

	// logging level, panic, fatal, error, warn, info, debug
	Level string `envconfig:"default=info"`

	// use mozlog format
	Mozlog bool `envconfig:"default=false"`

	// Disable HTTP Logging
	DisableHTTP bool `envconfig:"default=false"`
}

// configures limits for web/SyncUserHandler
type UserHandlerConfig struct {
	MaxRequestBytes       int `envconfig:"default=2097152"`
	MaxBSOGetLimit        int `envconfig:"default=1000"`
	MaxPOSTRecords        int `envconfig:"default=100"`
	MaxPOSTBytes          int `envconfig:"default=2097152"`
	MaxTotalRecords       int `envconfig:"default=1000"`
	MaxTotalBytes         int `envconfig:"default=20971520"`
	MaxBatchTTL           int `envconfig:"default=7200"`   // 2 hours
	MaxRecordPayloadBytes int `envconfig:"default=262144"` // 256KB
}

type PoolConfig struct {
	Num     int `envconfig:"default=0"`
	MaxSize int `envconfig:"default=25"`
}

var Config struct {
	Log      *LogConfig
	Hostname string `envconfig:"optional"`
	Host     string `envconfig:"default=0.0.0.0"`
	Port     int
	Secrets  []string
	DataDir  string
	Pool     *PoolConfig

	// Enable the pprof web endpoint /debug/pprof/
	EnablePprof bool `envconfig:"default=false"`

	// SyncUserHandler limits / configuration
	// available as LIMIT_x
	Limit *UserHandlerConfig
}

// so we can use config.Port and not config.Config.Port
var (
	Hostname    string
	Log         *LogConfig
	Host        string
	Port        int
	DataDir     string
	Secrets     []string
	Pool        *PoolConfig
	EnablePprof bool

	Limit *UserHandlerConfig

	DisableHTTPLogs bool
)

func init() {
	if err := envconfig.Init(&Config); err != nil {
		log.Fatalf("Config Error: %s\n", err)
	}

	if Config.Port < 1 || Config.Port > 65535 {
		log.Fatal("Config.Error: PORT invalid")
	}

	if Config.DataDir != ":memory:" {
		if _, err := os.Stat(Config.DataDir); os.IsNotExist(err) {
			log.Fatal("Config Error: DATA_DIR does not exist")
		}

		stat, err := os.Stat(Config.DataDir)
		if os.IsNotExist(err) {
			log.Fatal("Config Error: DATA_DIR does not exist")
		}
		if !stat.IsDir() {
			log.Fatal("Config Error: DATA_DIR is not a directory")
		}

		Config.DataDir = filepath.Clean(Config.DataDir)
		testfile := Config.DataDir + string(os.PathSeparator) + "test.writable"
		f, err := os.Create(testfile)
		if err != nil {
			log.Fatal("Config Error: DATA_DIR is not writable")
		} else {
			f.Close()
			os.Remove(testfile)
		}
	}

	switch Config.Log.Level {
	case "panic", "fatal", "error", "warn", "info", "debug":
	default:
		log.Fatalf("Config Error: LOG_LEVEL must be [panic, fatal, error, warn, info, debug]")
	}

	if Config.Hostname == "" {
		Config.Hostname, _ = os.Hostname()
	}

	if Config.Pool.Num <= 0 {
		Config.Pool.Num = runtime.NumCPU()
	}

	if Config.Limit.MaxBSOGetLimit < 1 {
		log.Fatal("LIMIT_MAX_BSO_GET_LIMIT must be >= 1")
	}
	if Config.Limit.MaxPOSTRecords < 1 {
		log.Fatal("LIMIT_MAX_POST_RECORDS must be >= 1")
	}
	if Config.Limit.MaxPOSTBytes < 1 {
		log.Fatal("LIMIT_MAX_MAX_POST_BYTES must be >= 1")
	}
	if Config.Limit.MaxTotalRecords < 1 {
		log.Fatal("LIMIT_MAX_TOTAL_RECORDS must be >= 1")
	}
	if Config.Limit.MaxTotalBytes < 1 {
		log.Fatal("LIMIT_MAX_TOTAL_BYTES must be >= 1")
	}
	if Config.Limit.MaxBatchTTL < 10 {
		log.Fatal("LIMIT_MAX_BATCH_TTL must be >= 10")
	}
	if Config.Limit.MaxRecordPayloadBytes < 1 {
		log.Fatal("LIMIT_MAX_RECORD_PAYLOAD_BYTES must be >= 1")
	}

	Hostname = Config.Hostname
	Log = Config.Log
	Host = Config.Host
	Port = Config.Port
	Secrets = Config.Secrets
	DataDir = Config.DataDir
	Pool = Config.Pool
	EnablePprof = Config.EnablePprof
	Limit = Config.Limit
}
