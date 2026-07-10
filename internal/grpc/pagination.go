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
	"fmt"
	"strconv"

	"google.golang.org/protobuf/proto"
)

type paginationRequest struct {
	PageSize int64
	Offset   int64
}

func newPaginationRequest(pageSize int64, pageToken string, defaultPageSize int64) (paginationRequest, error) {
	if pageSize <= 0 {
		pageSize = defaultPageSize
	}
	offset := int64(0)
	if pageToken != "" {
		parsed, err := strconv.ParseInt(pageToken, 10, 64)
		if err != nil || parsed < 0 {
			return paginationRequest{}, fmt.Errorf("invalid input - fail to parse page token")
		}
		offset = parsed
	}
	return paginationRequest{PageSize: pageSize, Offset: offset}, nil
}

func nextPageToken(nextOffset int64, total int) string {
	if nextOffset < int64(total) {
		return strconv.FormatInt(nextOffset, 10)
	}
	return ""
}

func paginateItems[T any](items []T, pageSize int64, pageToken string, defaultPageSize int64) ([]T, string, error) {
	page, err := newPaginationRequest(pageSize, pageToken, defaultPageSize)
	if err != nil {
		return nil, "", err
	}
	start := min(page.Offset, int64(len(items)))
	end := min(start+page.PageSize, int64(len(items)))
	return items[start:end], nextPageToken(end, len(items)), nil
}

func paginateProtoMessages[T proto.Message](items []T, pageSize int64, pageToken string, defaultPageSize int64, maxMessageSize int) ([]T, string, error) {
	page, err := newPaginationRequest(pageSize, pageToken, defaultPageSize)
	if err != nil {
		return nil, "", err
	}
	result := make([]T, 0)
	currentToken := int64(0)
	more := false
	msgSize := 0
	for _, item := range items {
		currentToken++
		if currentToken < page.Offset {
			continue
		}
		msgSize += proto.Size(item)
		if len(result) >= int(page.PageSize) {
			more = true
			break
		}
		if maxMessageSize > 0 && msgSize >= maxMessageSize {
			more = true
			break
		}
		result = append(result, item)
	}
	if more {
		return result, strconv.FormatInt(currentToken, 10), nil
	}
	return result, "", nil
}
