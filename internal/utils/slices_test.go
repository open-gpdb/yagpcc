// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright
// ownership. The ASF licenses this file to You under the Apache
// License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the
// License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

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
