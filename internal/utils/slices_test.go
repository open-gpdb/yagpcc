package utils_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/open-gpdb/yagpcc/internal/utils"
)

func TestNUniq(t *testing.T) {
	n := utils.NumberOfUniqueSlices([]int64{1})
	assert.Equal(t, n, int64(1))
	n = utils.NumberOfUniqueSlices(nil)
	assert.Equal(t, n, int64(0))
	n = utils.NumberOfUniqueSlices([]int64{1, 1, 1, 1, 0})
	assert.Equal(t, n, int64(2))
	n = utils.NumberOfUniqueSlices([]int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
	assert.Equal(t, n, int64(10))
}
