package syncstorage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPathMakerMaker(t *testing.T) {
	assert := assert.New(t)
	pf := PathMakerMaker(3)
	paths := pf("1234")
	if assert.Len(paths, 3) {
		assert.Equal([]string{"4", "3", "2"}, paths)
	}

	paths2 := pf("12")
	if assert.Len(paths2, 2, "Expected PathMaker to only use what it has") {
		assert.Equal([]string{"2", "1"}, paths2)
	}
}
