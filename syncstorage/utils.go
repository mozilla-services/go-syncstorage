package syncstorage

import (
	"fmt"
	"regexp"
	"time"
)

var (
	bsoIdCheck *regexp.Regexp
	cNameCheck *regexp.Regexp
)

func init() {
	bsoIdCheck = regexp.MustCompile("^[[:print:]]{1,64}$")
	cNameCheck = regexp.MustCompile(`^[\w-\.]{1,32}$`)
}

// Now returns the number of millisecond since the unix epoch
func Now() int {

	// milliseconds since the epoch
	ms := int(time.Now().UnixNano() / 1000 / 1000)

	// make it accurate only the hundredth of a millisecond
	// since the epoch. We only round up.
	//
	// the sync 1.5 api has modified timestamps for BSOs
	// only accurate to the hundredth of a ms. Keeping any more
	// causes rounding issues when matching for newer records
	// as the client will never have the thousandth's of a second
	// level of accuracy that we may have in the db.

	// since golang doesn't really have a regular ceil function for
	// integers, we add what we need to get the the nearest
	// hundredth
	ms = ms + 10 - (ms % 10)

	return ms
}

// ModifiedToString turns the output of Now(), an integer of milliseconds since
// the epoch to the sync 1.5's seconds w/ two decimals format
func ModifiedToString(modified int) string {
	return fmt.Sprintf("%.2f", float64(modified)/1000)
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

func CollectionNameOk(cName string) bool {
	return cNameCheck.MatchString(cName)
}

func String(s string) *string { return &s }
func Int(u int) *int          { return &u }
