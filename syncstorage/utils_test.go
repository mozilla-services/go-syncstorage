package syncstorage

import (
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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
		"?12345678-ab": true,
		".":            true,

		// should not validate ... this could be a lot ...
		"":     false,
		"\t":   false,
		"\t\n": false,
		"\r":   false,

		// range
		strings.Repeat("a", 65): false,
		strings.Repeat("a", 64): true,
	}

	for test, expect := range tests {
		if ValidateBSOId(test) != expect {
			t.Errorf("'%s' expected %v, but got %v", test, expect, !expect)
		}
	}

	// one wrong value should result in false
	if ValidateBSOId("ok", "\n") != false {
		t.Errorf("expected fail on any wrong value")
	}

	// one wrong value should result in false
	if ValidateBSOId("ok", "alsoOK") != true {
		t.Errorf("expected all to validate")
	}
}

func TestValidateCollectionNames(t *testing.T) {

	// any combination of 32 url safe base64 characters
	expectTrue := []string{
		"012345678901234567890123456789aa",
		"abcdefghijklmnopqrstuvwyz",
		"ABCDEFGHIJKLMNOPQRSTUVWXYZ",
		"0123456789",
		"-_.",
	}

	// anything > 32 chars, not in the url safe base64 character set
	// ...
	expectFalse := []string{
		"", // too short
		"012345678901234567890123456789aas", // too long
		"abcd@",
	}

	for _, test := range expectTrue {
		assert.True(t, CollectionNameOk(test))
	}

	for _, test := range expectFalse {
		assert.False(t, CollectionNameOk(test))
	}
}
