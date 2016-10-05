package web

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
)

// NewLogHandler return a http.Handler that wraps h and logs
// request out to logrus INFO level with fields
func NewLogHandler(l logrus.FieldLogger, h http.Handler) http.Handler {
	return &LoggingHandler{l, h}
}

type LoggingHandler struct {
	logger  logrus.FieldLogger
	handler http.Handler
}

func (h *LoggingHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	logger := makeLogger(w)
	url := *req.URL

	start := time.Now()

	// reuse or add a session context to the request
	if _, ok := SessionFromContext(req.Context()); ok {
		h.handler.ServeHTTP(logger, req)
	} else {
		// change the context of the request...
		newCtx := NewSessionContext(req.Context(), &Session{})
		req = req.WithContext(newCtx)
		h.handler.ServeHTTP(logger, req)
	}

	took := int(time.Duration(time.Since(start).Nanoseconds()) / time.Millisecond)

	uri := req.RequestURI

	// Requests using the CONNECT method over HTTP/2.0 must use
	// the authority field (aka r.Host) to identify the target.
	// Refer: https://httpwg.github.io/specs/rfc7540.html#CONNECT
	if req.ProtoMajor == 2 && req.Method == "CONNECT" {
		uri = req.Host
	}
	if uri == "" {
		uri = url.RequestURI()
	}

	// human readable request info redundant when mozlogging
	var logMsg string
	if l, ok := h.logger.(*logrus.Logger); ok {
		if _, ok := l.Formatter.(*MozlogFormatter); !ok {
			logMsg = fmt.Sprintf("%s %s %d",
				req.Method, uri,
				logger.Status())
		}
	}

	errno := logger.Status()
	if errno == http.StatusOK {
		errno = 0
	}

	// common fields to log with every request
	fields := logrus.Fields{
		"agent":  req.UserAgent(),
		"errno":  errno,
		"method": req.Method,
		"path":   uri,
		"req_sz": req.ContentLength,
		"res_sz": logger.Size(),
		"t":      took,
		"uid":    extractUID(uri),
	}

	if session, ok := SessionFromContext(req.Context()); ok && session.Token.Uid != 0 {
		fields["fxa_uid"] = session.Token.FxaUID
		fields["device_id"] = session.Token.DeviceId
	}

	h.logger.WithFields(fields).Info(logMsg)
}

// mozlog represents the MozLog standard format https://github.com/mozilla-services/Dockerflow/blob/master/docs/mozlog.md
type mozlog struct {
	Timestamp  int64
	Type       string
	Logger     string
	Hostname   string
	EnvVersion string
	Pid        int
	Severity   uint8
	Fields     logrus.Fields
}

// MozlogFormatter is a custom logrus formatter
type MozlogFormatter struct {
	Hostname string
	Pid      int
}

var encoderPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

// Format a logrus.Entry into a mozlog JSON object
func (f *MozlogFormatter) Format(entry *logrus.Entry) ([]byte, error) {

	m := &mozlog{
		Timestamp:  entry.Time.UnixNano(),
		Type:       "system",
		Logger:     "go-syncstorage",
		Hostname:   f.Hostname,
		EnvVersion: "2.0",
		Pid:        f.Pid,
		Severity:   0,
		Fields:     entry.Data,
	}

	if _, ok := entry.Data["method"]; ok {
		if _, ok2 := entry.Data["path"]; ok2 {
			m.Type = "request.summary"
		}
	}

	if entry.Message != "" {
		entry.Data["msg"] = entry.Message
	}

	switch entry.Level {
	case logrus.PanicLevel:
		m.Severity = 1
	case logrus.FatalLevel:
		m.Severity = 2
	case logrus.ErrorLevel:
		m.Severity = 3
	case logrus.WarnLevel:
		m.Severity = 4
	case logrus.InfoLevel:
		m.Severity = 6
	case logrus.DebugLevel:
		m.Severity = 7
	}

	b := encoderPool.Get().(*bytes.Buffer)
	defer func() {
		b.Reset()
		encoderPool.Put(b)
	}()

	// encode the fields in there
	enc := json.NewEncoder(b)
	if err := enc.Encode(m); err != nil {
		return nil, err
	}
	b.WriteString("\n")

	return b.Bytes(), nil
}

/*
 * ==============================================================
 * Much of this code was ported / copied over from
 * https://github.com/gorilla/handlers/blob/master/handlers.go
 * and used to implement a custom logger
 * ==============================================================
 */

func makeLogger(w http.ResponseWriter) loggingResponseWriter {
	var logger loggingResponseWriter = &responseLogger{w: w}
	if _, ok := w.(http.Hijacker); ok {
		logger = &hijackLogger{responseLogger{w: w}}
	}
	h, ok1 := logger.(http.Hijacker)
	c, ok2 := w.(http.CloseNotifier)
	if ok1 && ok2 {
		return hijackCloseNotifier{logger, h, c}
	}
	if ok2 {
		return &closeNotifyWriter{logger, c}
	}
	return logger
}

type loggingResponseWriter interface {
	http.ResponseWriter
	http.Flusher
	Status() int
	Size() int
}

// responseLogger is wrapper of http.ResponseWriter that keeps track of its HTTP
// status code and body size
type responseLogger struct {
	w      http.ResponseWriter
	status int
	size   int
}

func (l *responseLogger) Header() http.Header {
	return l.w.Header()
}

func (l *responseLogger) Write(b []byte) (int, error) {
	if l.status == 0 {
		// The status will be StatusOK if WriteHeader has not been called yet
		l.status = http.StatusOK
	}
	size, err := l.w.Write(b)
	l.size += size
	return size, err
}

func (l *responseLogger) WriteHeader(s int) {
	l.w.WriteHeader(s)
	l.status = s
}

func (l *responseLogger) Status() int {
	return l.status
}

func (l *responseLogger) Size() int {
	return l.size
}

func (l *responseLogger) Flush() {
	f, ok := l.w.(http.Flusher)
	if ok {
		f.Flush()
	}
}

type hijackLogger struct {
	responseLogger
}

func (l *hijackLogger) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	h := l.responseLogger.w.(http.Hijacker)
	conn, rw, err := h.Hijack()
	if err == nil && l.responseLogger.status == 0 {
		// The status will be StatusSwitchingProtocols if there was no error and
		// WriteHeader has not been called yet
		l.responseLogger.status = http.StatusSwitchingProtocols
	}
	return conn, rw, err
}

type closeNotifyWriter struct {
	loggingResponseWriter
	http.CloseNotifier
}

type hijackCloseNotifier struct {
	loggingResponseWriter
	http.Hijacker
	http.CloseNotifier
}
