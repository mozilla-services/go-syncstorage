package syncstorage

import (
	"math/rand"
	"strings"
	"testing"
	"time"
)

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-"

func init() {
	rand.Seed(time.Now().UnixNano())
}

// randData produce as random string of url safe base64 characters
func randData(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Int63()%int64(len(letterBytes))]
	}
	return string(b)
}

func TestValidateBSOIds(t *testing.T) {

	tests := map[string]bool{
		"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-": true,
		"012345678-ab": true,
		"a":            true,

		// should not validate ... this could be a lot ...
		"":             false,
		"?12345678-ab": false,
		".":            false,

		strings.Repeat("a", 65): false,
		strings.Repeat("a", 64): true,
	}

	for test, expect := range tests {
		if ValidateBSOId(test) != expect {
			t.Errorf("'%s' expected %v, but got %v", test, expect, !expect)
		}
	}

	// one wrong value should result in false
	if ValidateBSOId("ok", "?notOK") != false {
		t.Errorf("expected fail on any wrong value")
	}

	// one wrong value should result in false
	if ValidateBSOId("ok", "alsoOK") != true {
		t.Errorf("expected all to validate")
	}
}
