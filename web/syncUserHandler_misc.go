package web

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/mozilla-services/go-syncstorage/syncstorage"
	"github.com/pkg/errors"
)

// RequestToPostBSOInput extracts and unmarshals request.Body into a syncstorage.PostBSOInput. It
// returns a PostResults as well since it also validates BSOs
func RequestToPostBSOInput(r *http.Request) (
	syncstorage.PostBSOInput,
	*syncstorage.PostResults,
	error,
) {

	// bsoToBeProcessed will actually get sent to the DB
	bsoToBeProcessed := syncstorage.PostBSOInput{}
	results := syncstorage.NewPostResults(syncstorage.Now())

	// a list of all the raw json encoded BSOs
	var raw []json.RawMessage

	if ct := getMediaType(r.Header.Get("Content-Type")); ct == "application/json" || ct == "text/plain" {
		decoder := json.NewDecoder(r.Body)
		err := decoder.Decode(&raw)
		if err != nil {
			return nil, nil, errors.Wrap(err, "Could not unmarshal Request body")
		}
	} else { // deal with application/newlines
		raw = ReadNewlineJSON(r.Body)
	}

	for _, rawJSON := range raw {
		var b syncstorage.PutBSOInput
		if parseErr := parseIntoBSO(rawJSON, &b); parseErr == nil {
			bsoToBeProcessed = append(bsoToBeProcessed, &b)
		} else {
			// couldn't parse a BSO into something real
			// abort immediately
			if parseErr.field == "-" { // json error, not an object
				return nil, nil, errors.Wrap(parseErr, "Could not unmarshal BSO")
			}

			results.AddFailure(parseErr.bId, fmt.Sprintf("invalid %s", parseErr.field))
		}
	}

	// change TTL from seconds (what clients sends)
	// to milliseconds (what the DB uses)
	for _, p := range bsoToBeProcessed {
		if p.TTL != nil {
			tmp := *p.TTL * 1000
			p.TTL = &tmp
		}
	}

	return bsoToBeProcessed, results, nil
}

// ReadNewlineDelimitedJSON takes newline separate JSON and produces
// produces an array of json.RawMessage
func ReadNewlineJSON(data io.Reader) []json.RawMessage {
	raw := []json.RawMessage{}
	scanner := bufio.NewScanner(data)
	for scanner.Scan() {
		bsoBytes := scanner.Bytes()

		// ignore empty lines
		if len(strings.TrimSpace(string(bsoBytes))) == 0 {
			continue
		}

		raw = append(raw, bsoBytes)
	}

	return raw
}

func GetBatchIdAndCommit(r *http.Request) (batchFound bool, batchId string, batchCommit bool) {
	if vals, ok := r.URL.Query()["batch"]; ok && len(vals) > 0 {
		batchId = vals[0]
		batchFound = true
	}

	// any value is ok for commit param, as long as it exists
	_, batchCommit = r.URL.Query()["commit"]

	return
}
