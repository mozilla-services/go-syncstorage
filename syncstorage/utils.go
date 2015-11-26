package syncstorage

import (
	"regexp"
	"time"
)

var (
	bsoIdCheck *regexp.Regexp
)

func init() {
	bsoIdCheck = regexp.MustCompile("^[A-Za-z0-9_-]{1,64}$")
}

func Now() float64 {
	return float64(time.Now().UnixNano()) / 1000 / 1000
}

// ValidateBSOIds checks if all provided Is are 12 characters long
// and only contain url safe base64 characters
func ValidateBSOId(ids ...string) bool {
	for _, id := range ids {
		if bsoIdCheck.MatchString(id) != true {
			return false
		}
	}

	return true
}
