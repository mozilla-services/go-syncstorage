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

	// Filter out all messages where errno=0
	OnlyHTTPErrors bool `envconfig:"default=false"`
}

// configures limits for web/SyncUserHandler
type UserHandlerConfig struct {
	MaxRequestBytes       int `envconfig:"default=2097152"`
	MaxPOSTRecords        int `envconfig:"default=100"`
	MaxPOSTBytes          int `envconfig:"default=2097152"`
	MaxTotalRecords       int `envconfig:"default=1000"`
	MaxTotalBytes         int `envconfig:"default=20971520"`
	MaxBatchTTL           int `envconfig:"default=7200"`    // 2 hours
	MaxRecordPayloadBytes int `envconfig:"default=2097152"` // 2MB
}

type PoolConfig struct {
	Num           int `envconfig:"default=0"`
	MaxSize       int `envconfig:"default=25"`
	PurgeMinHours int `envconfig:"default=168"`
	PurgeMaxHours int `envconfig:"default=336"`
	VacuumKB      int `envconfig:"default=0"`
}

type SqliteConfig struct {
	CacheSize int `envconfig:"default=0"`
}

var Config struct {
	Log      *LogConfig
	Hostname string `envconfig:"optional"`
	Host     string `envconfig:"default=0.0.0.0"`
	Port     int
	Secrets  []string
	DataDir  string
	Pool     *PoolConfig
	Sqlite   *SqliteConfig

	// Enable the pprof web endpoint /debug/pprof/
	EnablePprof bool `envconfig:"default=false"`

	// SyncUserHandler limits / configuration
	// available as LIMIT_x
	Limit *UserHandlerConfig

	// cache size in MB for /info/collections cache
	InfoCacheSize int `envconfig:"default=0"`

	// max skew for hawk timestamps in seconds
	HawkTimestampMaxSkew int `envconfig:"default=60"`
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
	Sqlite      *SqliteConfig
	EnablePprof bool

	Limit *UserHandlerConfig

	InfoCacheSize        int
	HawkTimestampMaxSkew int
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

	if Config.InfoCacheSize < 0 {
		log.Fatal("INFO_CACHE_SIZE must be >= 0")
	}

	if Config.Pool.VacuumKB < 0 {
		log.Fatal("POOL_VACUUM_KB must be >= 0")
	}
	if Config.Pool.PurgeMinHours <= 0 {
		log.Fatal("POOL_MIN_HOURS must be > 0")
	}
	if Config.Pool.PurgeMaxHours <= 0 {
		log.Fatal("POOL_MAX_HOURS must be > 0")
	}
	if Config.Pool.PurgeMaxHours < Config.Pool.PurgeMinHours {
		log.Fatal("POOL_MAX_HOURS must be > POOL_MIN_HOURS")
	}

	if Config.HawkTimestampMaxSkew < 60 {
		log.Fatal("HAWK_TIMESTAMP_MAX_SKEW must be >= 60")
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
	Sqlite = Config.Sqlite
	InfoCacheSize = Config.InfoCacheSize
	HawkTimestampMaxSkew = Config.HawkTimestampMaxSkew
}
