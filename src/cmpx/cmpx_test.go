package cmpx

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMin(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 1, Min(1, 1))
	assert.Equal(t, 1, Min(1, 2))
	assert.Equal(t, 1, Min(2, 1))
	assert.Equal(t, 0, Min(0, 0))
	assert.Equal(t, 0, Min(0, 1))
	assert.Equal(t, 0, Min(1, 0))
}
