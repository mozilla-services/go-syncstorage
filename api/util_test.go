package api

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConvertTimestamp(t *testing.T) {
	i, err := ConvertTimestamp("123.45")
	assert.NoError(t, err)
	assert.Equal(t, 123450, i)

	// TODO add more bad formatting here
	_, err = ConvertTimestamp("abcd")
	assert.NotNil(t, err)
}
