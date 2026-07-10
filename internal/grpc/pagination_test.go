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

package grpc

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pbc "github.com/open-gpdb/yagpcc/api/proto/common"
)

func TestNewPaginationRequest(t *testing.T) {
	page, err := newPaginationRequest(0, "", 50)
	require.NoError(t, err)
	assert.Equal(t, int64(50), page.PageSize)
	assert.Equal(t, int64(0), page.Offset)

	page, err = newPaginationRequest(25, "10", 50)
	require.NoError(t, err)
	assert.Equal(t, int64(25), page.PageSize)
	assert.Equal(t, int64(10), page.Offset)

	_, err = newPaginationRequest(25, "not-a-number", 50)
	require.Error(t, err)

	_, err = newPaginationRequest(25, "-1", 50)
	require.Error(t, err)
}

func TestNextPageToken(t *testing.T) {
	assert.Equal(t, "2", nextPageToken(2, 3))
	assert.Empty(t, nextPageToken(3, 3))
	assert.Empty(t, nextPageToken(4, 3))
}

func TestPaginateItems(t *testing.T) {
	items := []int{10, 20, 30, 40}

	page, token, err := paginateItems(items, 2, "", 50)
	require.NoError(t, err)
	assert.Equal(t, []int{10, 20}, page)
	assert.Equal(t, "2", token)

	page, token, err = paginateItems(items, 2, token, 50)
	require.NoError(t, err)
	assert.Equal(t, []int{30, 40}, page)
	assert.Empty(t, token)

	page, token, err = paginateItems(items, 2, "99", 50)
	require.NoError(t, err)
	assert.Empty(t, page)
	assert.Empty(t, token)
}

func TestPaginateItemsUsesDefaultPageSize(t *testing.T) {
	items := []int{1, 2, 3}

	page, token, err := paginateItems(items, 0, "", 2)
	require.NoError(t, err)
	assert.Equal(t, []int{1, 2}, page)
	assert.Equal(t, "2", token)
}

func TestPaginateProtoMessages(t *testing.T) {
	items := []*pbc.SessionState{
		{SessionKey: &pbc.SessionKey{SessId: 1}},
		{SessionKey: &pbc.SessionKey{SessId: 2}},
		{SessionKey: &pbc.SessionKey{SessId: 3}},
	}

	page, token, err := paginateProtoMessages(items, 2, "", 50, 0)
	require.NoError(t, err)
	require.Len(t, page, 2)
	assert.Equal(t, int64(1), page[0].SessionKey.SessId)
	assert.Equal(t, int64(2), page[1].SessionKey.SessId)
	assert.Equal(t, "3", token)

	page, token, err = paginateProtoMessages(items, 2, token, 50, 0)
	require.NoError(t, err)
	require.Len(t, page, 1)
	assert.Equal(t, int64(3), page[0].SessionKey.SessId)
	assert.Empty(t, token)
}

func TestPaginateProtoMessagesStopsAtMaxMessageSize(t *testing.T) {
	items := []*pbc.SessionState{
		{SessionKey: &pbc.SessionKey{SessId: 1}},
		{SessionKey: &pbc.SessionKey{SessId: 2}},
	}

	page, token, err := paginateProtoMessages(items, 50, "", 50, 1)
	require.NoError(t, err)
	assert.Empty(t, page)
	assert.Equal(t, "1", token)
}
