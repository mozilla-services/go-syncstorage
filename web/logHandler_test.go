package web

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestLogHandler(t *testing.T) {
	assert := assert.New(t)
	var buf bytes.Buffer

	logger := logrus.New()
	logger.Out = &buf
	logger.Formatter = &MozlogFormatter{
		Hostname: "test.localdomain",
		Pid:      os.Getpid(),
	}

	hFunc := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte("OK"))
	})

	handler := NewLogHandler(logger, hFunc)
	request("GET", "/1.5/12346", nil, handler)

	// should be able to decode the log message
	if !assert.True(buf.Len() > 0) {
		return
	}
	var record mozlog
	if err := json.Unmarshal(buf.Bytes(), &record); !assert.NoError(err) {
		return
	}

	assert.True(record.Timestamp > 0)
	assert.Equal("request.summary", record.Type)
	assert.Equal("go-syncstorage", record.Logger)
	assert.Equal("test.localdomain", record.Hostname)
	assert.Equal("2.0", record.EnvVersion)
	assert.Equal(os.Getpid(), record.Pid)
	assert.Equal(uint8(6), record.Severity)

	// field test
	tests := map[string]interface{}{
		"uid": "12346",
		// fxa_uid and device_id are derived from the uid
		"fxa_uid":   "fxa_12346",
		"device_id": "34d128f5",
		"errno":     float64(0), // use float64 since it is what json supports
		"method":    "GET",
		"agent":     "go-tester",
	}

	for key, test := range tests {
		assert.Equal(test, record.Fields[key], fmt.Sprintf("Key: %s", key))
	}
}

func TestLogHandlerMozlogFormatter(t *testing.T) {
	assert := assert.New(t)
	fields := logrus.Fields{
		"agent":     "benchmark agent",
		"errno":     float64(0),
		"method":    "GET",
		"path":      "/so/fassst",
		"req_sz":    float64(0),
		"res_sz":    float64(1024),
		"t":         float64(20),
		"uid":       "123456",
		"fxa_uid":   "123456",
		"device_id": "7654321",
	}

	entry := logrus.WithFields(fields)
	entry.Level = logrus.InfoLevel
	entry.Time = time.Date(2013, time.January, 14, 0, 0, 0, 0, time.FixedZone("UTC", 0))

	formatter := &MozlogFormatter{
		Hostname: "test.localdomain",
		Pid:      os.Getpid(),
	}

	logData, err := formatter.Format(entry)
	if !assert.NoError(err) {
		return
	}

	var record mozlog
	if err := json.Unmarshal(logData, &record); !assert.NoError(err) {
		return
	}

	assert.True(record.Timestamp > 0)
	assert.Equal("request.summary", record.Type)
	assert.Equal("go-syncstorage", record.Logger)
	assert.Equal("test.localdomain", record.Hostname)
	assert.Equal("2.0", record.EnvVersion)
	assert.Equal(os.Getpid(), record.Pid)
	assert.Equal(uint8(6), record.Severity)

	// field test
	for key, test := range fields {
		assert.Equal(test, record.Fields[key], fmt.Sprintf("Key: %s", key))
	}

	// make sure there's a new line at the end
	assert.Equal("\n", string(logData[len(logData)-1:]))

}

func BenchmarkMozlogFormatter(b *testing.B) {

	entry := logrus.WithFields(logrus.Fields{
		"agent":     "benchmark agent",
		"errno":     0,
		"method":    "GET",
		"path":      "/so/fassst",
		"req_sz":    0,
		"res_sz":    1024,
		"t":         20,
		"uid":       "123456",
		"fxa_uid":   "123456",
		"device_id": "7654321",
	})

	formatter := &MozlogFormatter{
		Hostname: "test.localdomain",
		Pid:      os.Getpid(),
	}

	for i := 0; i < b.N; i++ {
		formatter.Format(entry)
	}

}
