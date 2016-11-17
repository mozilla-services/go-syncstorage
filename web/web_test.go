package web

import (
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"sync"

	log "github.com/Sirupsen/logrus"
	"github.com/mozilla-services/go-syncstorage/token"
	"github.com/pkg/errors"
)

func init() {
	switch os.Getenv("TEST_LOG_LEVEL") {
	case "fatal":
		log.SetLevel(log.FatalLevel)
	case "warn":
		log.SetLevel(log.WarnLevel)
	case "info":
		log.SetLevel(log.InfoLevel)
	case "debug":
		log.SetLevel(log.DebugLevel)
	default:
		log.SetLevel(log.ErrorLevel)
	}
}

var (
	uidLock  sync.Mutex
	uidCount = 10000

	// Helps with testing layers of http.Handler
	EchoHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Body != nil {
			io.Copy(w, r.Body)
		}
		w.WriteHeader(http.StatusOK)
	})

	OKFailHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/fail" {
			cause := errors.New("The Cause")
			sendRequestProblem(w, r, http.StatusBadRequest, errors.Wrap(cause, "The Error"))
		} else {
			w.WriteHeader(http.StatusOK)
		}
	})
)

func uniqueUID() string {
	uidLock.Lock()
	defer uidLock.Unlock()

	uidCount += 1
	return strconv.Itoa(uidCount)
}

// syncurl makes a syncurl cause i'm too lazy to type it all the time
func syncurl(uid interface{}, path string) string {
	var u string

	switch uid.(type) {
	case string:
		u = uid.(string)
	case uint64:
		u = strconv.FormatUint(uid.(uint64), 10)
	case int:
		u = strconv.Itoa(uid.(int))
	default:
		panic("Unknown uid type")
	}

	return "http://synchost/1.5/" + u + "/" + path
}

func request(method, urlStr string, body io.Reader, h http.Handler) *httptest.ResponseRecorder {
	header := make(http.Header)
	header.Set("Accept", "application/json")
	return requestheaders(method, urlStr, body, header, h)
}

func jsonrequest(method, urlStr string, body io.Reader, h http.Handler) *httptest.ResponseRecorder {
	header := make(http.Header)
	header.Set("Accept", "application/json")
	header.Set("Content-Type", "application/json")
	return requestheaders(method, urlStr, body, header, h)
}

func requestheaders(method, urlStr string, body io.Reader, header http.Header, h http.Handler) *httptest.ResponseRecorder {

	req, err := http.NewRequest(method, urlStr, body)
	req.Header = header
	if req.Header.Get("User-Agent") == "" {
		req.Header.Set("User-Agent", "go-tester")
	}
	if err != nil {
		panic(err)
	}

	// add the session data in
	// I should have used int64 from the beginning... ¯\_(ツ)_/¯
	uid := extractUID(urlStr)
	if uid == "" {
		uid = "24601"
	}

	uid64, err := strconv.ParseUint(uid, 10, 64)
	if err != nil {
		panic(err)
	}

	deviceId := fmt.Sprintf("%x", sha256.Sum256([]byte(uid)))[:8]

	session := &Session{
		Token: token.TokenPayload{
			Uid:      uid64,
			FxaUID:   "fxa_" + extractUID(urlStr),
			DeviceId: deviceId,
		},
	}
	reqCtx := req.WithContext(
		NewSessionContext(req.Context(), session))

	return sendrequest(reqCtx, h)
}

func sendrequest(req *http.Request, h http.Handler) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	if h == nil {
		panic("Handler required")
	}

	h.ServeHTTP(w, req)
	return w
}
