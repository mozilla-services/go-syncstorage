package web

import (
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
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

	handler := NewLogHandler(logger, EchoHandler)

	{
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
		assert.Equal("mozsvc.metrics", record.Type)
		assert.Equal("Sync-1_5", record.Logger)
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

		// make sure there is no error field
		assert.Nil(record.Fields["error"])
	}

	// test that very large path strings do not get truncated
	buf.Reset()
	{
		ids := make([]string, 200, 200)
		for i := 0; i < len(ids); i++ {
			data := []byte(time.Now().Format(time.RFC3339Nano))
			sum := sha1.Sum(data)
			ids[i] = hex.EncodeToString(sum[:])[0:16]
		}
		path := "/1.5/12346/forms?ids=" + strings.Join(ids, ",")
		request("DELETE", path, nil, handler)
		var record mozlog
		if err := json.Unmarshal(buf.Bytes(), &record); !assert.NoError(err) {
			return
		}

		//  it'll be about 3420 bytes
		assert.Equal(path, record.Fields["path"])
	}
}

// TestLogHandlerContext tests that Token payload data was properly passed
// and retained as it went through several handlers
func TestLogHandlerContext(t *testing.T) {

	assert := assert.New(t)
	var buf bytes.Buffer

	logger := logrus.New()
	logger.Out = &buf
	logger.Formatter = &MozlogFormatter{
		Hostname: "test.localdomain",
		Pid:      os.Getpid(),
	}

	// pass it through the hawk and the EchoHandler
	hawkHandle := NewHawkHandler(EchoHandler, []string{"sekret"})
	logHandle := NewLogHandler(logger, hawkHandle)

	var uid uint64 = 12345
	tok := testtoken(hawkHandle.secrets[0], uid)
	req, _ := hawkrequestbody("POST", syncurl(uid, "some/endpoint"), tok, "text/plain",
		bytes.NewBufferString(strings.Repeat("ABC", 10)))
	resp := sendrequest(req, logHandle)

	assert.Equal(http.StatusOK, resp.Code)

	// make sure fxa_uid and device_id was logged correctly
	// are passed around in the session context
	var record mozlog
	if err := json.Unmarshal(buf.Bytes(), &record); !assert.NoError(err) {
		return
	}

	assert.Equal("fxa_12345", record.Fields["fxa_uid"])
	assert.Equal("device_12345", record.Fields["device_id"])

	// make sure res_sz is correct
	assert.Equal(float64(resp.Body.Len()), record.Fields["res_sz"]) // use float64 cause json converted
}

func TestLogHandlerCauseFromContext(t *testing.T) {
	assert := assert.New(t)
	buf := new(bytes.Buffer)

	logger := logrus.New()
	logger.Out = buf
	logger.Formatter = &MozlogFormatter{
		Hostname: "test.localdomain",
		Pid:      os.Getpid(),
	}

	handler := NewLogHandler(logger, OKFailHandler)

	{
		request("GET", "/fail", nil, handler)
		// should be able to decode the log message
		if !assert.True(buf.Len() > 0) {
			return
		}
		var record mozlog
		if err := json.Unmarshal(buf.Bytes(), &record); !assert.NoError(err) {
			return
		}

		// make sure the error was logged correctly with the cause mashed together
		assert.Equal(float64(http.StatusBadRequest), record.Fields["errno"])
		assert.Equal("The Error: The Cause", record.Fields["error"])
	}

	{ // make sure regular requests log..
		buf.Reset()
		request("GET", "/ok", nil, handler)
		if !assert.True(buf.Len() > 0) {
			return
		}
	}

	{ // disable all logging except for error logging
		buf.Reset()
		if h, ok := handler.(*LoggingHandler); assert.True(ok) {
			h.OnlyHTTPErrors = true
		}
		// make sure
		request("GET", "/ok", nil, handler)
		assert.Equal(0, buf.Len())

		buf.Reset()
		request("GET", "/fail", nil, handler)
		assert.True(buf.Len() > 0)
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
	assert.Equal("mozsvc.metrics", record.Type)
	assert.Equal("Sync-1_5", record.Logger)
	assert.Equal("test.localdomain", record.Hostname)
	assert.Equal("2.0", record.EnvVersion)
	assert.Equal(os.Getpid(), record.Pid)
	assert.Equal(uint8(6), record.Severity)

	// field test
	for key, test := range fields {
		assert.Equal(test, record.Fields[key], fmt.Sprintf("Key: %s", key))
	}
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
