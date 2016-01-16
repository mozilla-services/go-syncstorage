package api

// Notes about how mozilla's tokenserver, Hawk Auth and sync server interact
//
// 0. User is signed into Firefox Accounts (FxA), has an FxA assertion
// 1. The token server issues a token (url base64 encoded) w/ FxA assertion
// 2. On the client side, the Token == Hawk Id, token's derived secret == Hawk secret
//   - so in the Hawk auth headers, the Id actually contains a enough info to generate
//     the hawk secret. The hawk secret no longer needs to be pre-shared. Only the
//     secret between the tokenserver and the sync-storage node needs to be
//     preshared. This provides a unique secret for every user from only requiring
//     a single pre-shared secret.
// 3. This syncstorage node extract the Token from the hawk id, validates it and
//    and uses the derived secret as the Hawk secret to authenticate requests

import (
	"encoding/base64"
	"errors"
	"net/http"

	. "github.com/mostlygeek/go-debug"
	"github.com/mostlygeek/go-syncstorage/token"
	"github.com/mostlygeek/gohawk/hawk"
)

var debug = Debug("syncapi:hawk")

var (
	ErrTokenExpired = errors.New("Token is expired")
)

// Hawk authentication bits

type Credentials struct {
	key hawk.Key
	uid uint64
}

func (c *Credentials) Key() hawk.Key {
	return c.key
}

type CredentialsStore struct {
	// keep a list of valid secrets. These are pre-shared symmetric secrets with
	// the Token server.
	//
	//The Token server uses a specific shared secret per
	//syncstorage server. When these rotate we want to be able to accept
	//requests from users with old, but still valid tokens
	secrets []string
}

// CredentialsForKeyIdentifier specifically handles how the mozilla tokenserver+sync server
// pass credentials. The Hawk KEY is actually the token data returned from tokenserver.
// The token is extracted, validated its "derived key" is used as the Hawk SECRET.
// With the Hawk SECRET extracted it is possible to validate the Hawk authentication header.
func (cs *CredentialsStore) CredentialsForKeyIdentifier(keyId string) (hawk.Credentials, error) {

	var (
		err         error
		parsedToken token.Token
	)

	// try all the secrets we have
	for _, secret := range cs.secrets {
		parsedToken, err = token.ParseToken([]byte(secret), keyId)

		// could not extract token w/ secret...
		if err != nil {
			continue
		}

		if parsedToken.Expired() {
			// just fail as the token no longer valid
			return nil, ErrTokenExpired
		}

		debug("Got valid token, token secret: %s, derived secret: %s", secret, parsedToken.DerivedSecret)

		// the derived secret is a base64 url encoded string and it needs to be converted
		// back into the same bytes to be useful in HKDF operations...
		hawkSecret, err := base64.URLEncoding.DecodeString(parsedToken.DerivedSecret)
		if err != nil {
			return nil, err
		}

		return &Credentials{
			key: hawk.Key{
				Identifier: keyId,
				Secret:     hawkSecret,
				Algorithm:  "sha256",
			},

			uid: parsedToken.Payload.Uid,
		}, nil
	}

	return nil, err
}

// HawkHeaderFromToken generates a Hawk authorization header from a token and hawk
// specific parameters (id, timestamp, nonce, ext)
func HawkHeaderFromToken(r *http.Request, token token.Token, timestamp int64, nonce, ext string) (string, error) {
	derivedBytes, err := base64.URLEncoding.DecodeString(token.DerivedSecret)
	if err != nil {
		return "", err
	}

	creds := hawk.NewBasicCredentials(token.Token, derivedBytes, "sha256")
	return hawk.CreateRequestHeader(r, creds,
		token.Token, // use as the Hawk Id
		timestamp,
		nonce,
		ext,
	)

}
