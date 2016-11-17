package web

import (
	"context"

	"github.com/mozilla-services/go-syncstorage/token"
)

type sessionKey int

var sKey sessionKey = 0

type Session struct {
	Token       token.TokenPayload
	ErrorResult error
}

func NewSessionContext(ctx context.Context, ses *Session) context.Context {
	return context.WithValue(ctx, sKey, ses)
}

func SessionFromContext(ctx context.Context) (*Session, bool) {
	s, ok := ctx.Value(sKey).(*Session)
	return s, ok
}
