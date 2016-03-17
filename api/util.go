package api

import "strconv"

// ConvertTimestamp converts the sync decimal time in seconds to
// a time in milliseconds
func ConvertTimestamp(ts string) (int, error) {

	f, err := strconv.ParseFloat(ts, 64)
	if err != nil {
		return 0, err
	}

	return int(f * 1000), nil

}
