package api

import (
	"bufio"
	"fmt"
	"net"
	"net/http"

	log "github.com/Sirupsen/logrus"
)

// LogHandler return a http.Handler that wraps h and logs
// request out to logrus INFO level with fields
func LogHandler(h http.Handler) http.Handler {
	return loggingHandler{h}
}

type loggingHandler struct {
	handler http.Handler
}

func (h loggingHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	logger := makeLogger(w)
	url := *req.URL
	h.handler.ServeHTTP(logger, req)

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

	logMsg := fmt.Sprintf("%s %s %d %d %d",
		req.Method, uri,
		logger.Status(),
		req.ContentLength,
		logger.Size())

	if log.GetLevel() == log.DebugLevel {
		fields := log.Fields{
			"method":     req.Method,
			"path":       uri,
			"req_size":   req.ContentLength,
			"status":     logger.Status(),
			"size":       logger.Size(),
			"req_header": req.Header,
			"res_header": logger.Header(),
		}

		log.WithFields(fields).Debug(logMsg)
	} else {
		log.WithFields(log.Fields{
			"method":   req.Method,
			"path":     uri,
			"req_size": req.ContentLength,
			"status":   logger.Status(),
			"size":     logger.Size(),
		}).Info(logMsg)
	}
}

/*
 * Much of this code was ported / copied over from
 * https://github.com/gorilla/handlers/blob/master/handlers.go
 * and used to implement a custom logger
 *
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
