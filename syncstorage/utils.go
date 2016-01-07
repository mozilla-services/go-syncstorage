package syncstorage

import (
	"regexp"
	"time"
)

var (
	bsoIdCheck *regexp.Regexp
)

func init() {
	bsoIdCheck = regexp.MustCompile("^[[:print:]]{1,64}$")
}

func Now() int {
	return int(time.Now().UnixNano() / 1000)
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

func BSOIdOk(bId string) bool {
	return bsoIdCheck.MatchString(bId)
}

// SortIndexOK validates a sortIndex int
func SortIndexOk(sortIndex int) bool {
	return (sortIndex >= 0 && sortIndex < 1000000000)
}

func TTLOk(ttl int) bool {
	return (ttl > 0)
}

func LimitOk(limit int) bool {
	return (limit > 0)
}

func OffsetOk(offset int) bool {
	return (offset >= 0)
}

func NewerOk(newer int) bool {
	return (newer >= 0)
}

func String(s string) *string { return &s }
func Int(u int) *int          { return &u }
