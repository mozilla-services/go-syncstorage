package syncstorage

import "strings"

// PathMaker generates a sub-directory structure used for saving files
type PathMaker func(string) []string

// DefaultPathMaker saves database in a single directory
var (
	// single level
	DefaultPathMaker = func(uid string) []string { return []string{} }

	// two level, ie: "abc" => b/a
	TwoLevelPath = PathMakerMaker(2)

	// three level, ie: "abc" => c/b/a
	ThreeLevelPath = PathMakerMaker(2)
)

// PathMakerMaker is meta... makes a PathMaker func that turns
// "abcd" --> []string["d","c", ...], reverse order
func PathMakerMaker(levels int) PathMaker {
	return func(uid string) []string {
		parts := strings.Split(uid, "")

		if levels > len(parts) {
			levels = len(parts)
		}

		path := make([]string, levels)
		for i := 0; i < levels; i++ {
			path[i], parts = parts[len(parts)-1], parts[:len(parts)-1]
		}

		return path
	}
}
