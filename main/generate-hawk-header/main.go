package main

// Generate a Hawk string useful for using with CURL

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"regexp"
	"strconv"
	"time"

	"go.mozilla.org/hawk"

	"github.com/mozilla-services/go-syncstorage/token"
)

func errorAndExit(format string, vals ...interface{}) {
	fmt.Printf(format, vals...)
	fmt.Println()
	os.Exit(1)
}

func nonce() string {
	b := make([]byte, 8)
	_, err := io.ReadFull(rand.Reader, b)
	if err != nil {
		panic(err)
	}
	return base64.StdEncoding.EncodeToString(b)[:8]
}

func main() {

	if len(os.Args) < 3 {
		fmt.Printf("Usage: %s <syncurl> <secret>\n", path.Base(os.Args[0]))
		os.Exit(1)
	}

	urlPath := os.Args[1]
	secret := os.Args[2]

	// create a token
	parts, err := url.Parse(urlPath)
	if err != nil {
		errorAndExit("Err parsing url %s\n", err.Error())
	}

	// extract the uid from url
	uidregex := regexp.MustCompile(`/1\.5/([0-9]+)`)
	matches := uidregex.FindStringSubmatch(parts.Path)
	var uid uint64
	if len(matches) > 0 {
		id, _ := strconv.ParseInt(matches[1], 10, 32)
		uid = uint64(id)

	} else {
		errorAndExit("Could not find uid in path %s", parts.Path)
	}

	payload := token.TokenPayload{
		Uid:     uid,
		Node:    parts.Host,
		Expires: float64(time.Now().UnixNano()/int64(time.Second) + 30),
	}

	token, err := token.NewToken([]byte(secret), payload)
	if err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	}

	creds := &hawk.Credentials{
		ID:   token.Token,
		Key:  token.DerivedSecret,
		Hash: sha256.New,
	}
	auth, _ := hawk.NewURLAuth(urlPath, creds, 0)
	auth.Nonce = nonce()
	fmt.Print(auth.RequestHeader())
}
