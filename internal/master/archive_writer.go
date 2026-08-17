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

package master

import (
	"context"

	pbm "github.com/open-gpdb/yagpcc/api/proto/agent_master"
	"github.com/open-gpdb/yagpcc/internal/gp"
)

// ArchiveWriter is the interface for writing archived session, query, and segment metric data.
// Each method accepts a batch of messages and is expected to process them atomically.
type ArchiveWriter interface {
	// StoreSessions writes a batch of session data to the archive.
	StoreSessions(ctx context.Context, sessions []*gp.SessionDataWrite) error

	// StoreQuery writes a batch of query statistics to the archive.
	StoreQuery(ctx context.Context, queries []*pbm.QueryStatWrite) error

	// StoreSegmensMetrics writes a batch of segment metrics to the archive.
	// Note: the misspelled name is preserved for backward compatibility.
	StoreSegmensMetrics(ctx context.Context, metrics []*pbm.SegmentMetricsWrite) error

	// Close releases any resources held by the writer (open files, network
	// connections). It is called once when the owning context is cancelled so a
	// master restart or leadership change does not orphan connections.
	Close() error
}
