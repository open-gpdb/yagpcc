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

package gp

import (
	"context"
	"fmt"
)

type StatActivityColumnsConfiguration struct {
	HasWaitEvent bool `db:"has_wait_event"`
}

const StatActivityColumnsQ = `
	SELECT
		count(*) FILTER (WHERE attname = 'wait_event') > 0 AS has_wait_event
	FROM pg_attribute
	WHERE attrelid = 'pg_catalog.pg_stat_activity'::regclass
		AND attnum > 0
		AND NOT attisdropped
`

func UsesModernStatActivity(ctx context.Context) (bool, error) {
	if db == nil {
		return false, fmt.Errorf("internal - DB not initialized")
	}
	statActivityColumns := make([]StatActivityColumnsConfiguration, 0)
	err := db.ExecQuery(ctx, StatActivityColumnsQ, &statActivityColumns)
	if err != nil {
		return false, err
	}
	if len(statActivityColumns) == 0 {
		return false, fmt.Errorf("internal - empty pg_stat_activity columns query result")
	}
	return statActivityColumns[0].HasWaitEvent, nil
}
