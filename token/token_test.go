package token

// copy/pasted here from the original [1] since it was not compatible with
// the python token server's spec which provided the expires timestamp as
// a float [2]
//
// [1] https://raw.githubusercontent.com/st3fan/moz-tokenserver/master/token/token_test.go
// [2] https://github.com/mozilla-services/tokenserver/blob/3b3d98359285dcbcae1706ded664a63fcb457639/tokenserver/views.py#L262

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_NewToken(t *testing.T) {
	payload := TokenPayload{
		Uid:     1234,
		Node:    "http://node.mozilla.org",
		Expires: 1452807004.454294,
	}

	token, err := NewToken([]byte("thisisasecret"), payload)
	if err != nil {
		t.Error(err)
	}

	if len(token.Token) == 0 {
		t.Error("token.Token is empty")
	}

	if len(token.DerivedSecret) == 0 {
		t.Error("token.DerivedSecret is empty")
	}
}

func Test_ParseToken(t *testing.T) {
	payload := TokenPayload{
		Uid:     1234,
		Node:    "http://node.mozilla.org",
		Expires: 1452807004.454294,
	}

	generatedToken, err := NewToken([]byte("thisisasecret"), payload)
	if err != nil {
		t.Error(err)
	}

	if len(generatedToken.Token) == 0 {
		t.Error("generatedToken.Token is empty")
	}

	if len(generatedToken.DerivedSecret) == 0 {
		t.Error("generatedToken.DerivedSecret is empty")
	}

	parsedToken, err := ParseToken([]byte("thisisasecret"), generatedToken.Token)
	if err != nil {
		t.Error(err)
	}

	if generatedToken.Payload.Salt != parsedToken.Payload.Salt {
		t.Error("Different Payload.Salt")
	}
	if generatedToken.Payload.Uid != parsedToken.Payload.Uid {
		t.Error("Different Payload.Uid")
	}
	if generatedToken.Payload.Node != parsedToken.Payload.Node {
		t.Error("Different Payload.Node")
	}
	if generatedToken.Payload.Expires != parsedToken.Payload.Expires {
		t.Error("Different Payload.Expires")
	}

	if generatedToken.Token != parsedToken.Token {
		t.Errorf("Different Token %+v vs %+v", generatedToken, parsedToken)
	}

	if generatedToken.DerivedSecret != parsedToken.DerivedSecret {
		t.Errorf("Different DerivedSecret %+v vs %+v", generatedToken, parsedToken)
	}
}

func Test_TokenExpired(t *testing.T) {
	expectExpired := map[bool]float64{
		true:  (float64(time.Now().Unix()) - 10000),
		false: (float64(time.Now().Unix()) + 10000),
	}

	for expected, ts := range expectExpired {
		payload := TokenPayload{
			Uid:     1234,
			Node:    "http://node.mozilla.org",
			Expires: ts,
		}

		generatedToken, err := NewToken([]byte("thisisasecret"), payload)
		if err != nil {
			t.Error(err)
		}

		if generatedToken.Expired() != expected {
			t.Errorf("Unexpected Expired() == %v\n", expected)
		}
	}

}

func TestTokenPayload(t *testing.T) {
	payload := TokenPayload{
		Uid:     1234,
		Node:    "http://node.mozilla.org",
		Expires: 1452807004.454294,
	}

	assert.Equal(t, "1234", payload.UidString())
}
