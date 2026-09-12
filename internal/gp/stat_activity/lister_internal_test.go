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

package stat_activity

import (
	"strings"
	"testing"
)

// unit tests never run the SQL, so pin the predicates here
func TestLocksQuery_Shape(t *testing.T) {
	sql := strings.Join(strings.Fields(locksQuery), " ")

	for _, tc := range []struct {
		predicate string
		want      int
	}{
		{"l.gp_segment_id = w.gp_segment_id", 2},
		{"l.mppsessionid <> w.mppsessionid", 2},
		{"l.transactionid = w.transactionid", 1},
		{"l.transactionid is not NULL", 1},
		{"UNION ALL", 1},
	} {
		if got := strings.Count(sql, tc.predicate); got != tc.want {
			t.Errorf("locksQuery must contain %q %d time(s), found %d", tc.predicate, tc.want, got)
		}
	}
}
