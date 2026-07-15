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

package master_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pbm "github.com/open-gpdb/yagpcc/api/proto/agent_master"
	pbc "github.com/open-gpdb/yagpcc/api/proto/common"
	"github.com/open-gpdb/yagpcc/internal/gp"
	"github.com/open-gpdb/yagpcc/internal/master"
	"github.com/open-gpdb/yagpcc/internal/storage"
	"go.uber.org/zap"
)

func TestBatchProcessorConfigDefaults(t *testing.T) {
	cfg := master.DefaultBatchProcessorConfig()
	assert.Equal(t, time.Second, cfg.BatchInterval)
	assert.Equal(t, time.Second, cfg.WriteTimeout)
	assert.Equal(t, 60, cfg.BatchQueueSize)
}

func TestRunSessionBatchProcessorProcessesMessages(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	sourceCh := make(chan *gp.SessionDataWrite, 10)
	var processedCount atomic.Int64

	storeFunc := func(ctx context.Context, sessions []*gp.SessionDataWrite) error {
		processedCount.Add(int64(len(sessions)))
		return nil
	}

	cfg := master.DefaultBatchProcessorConfig()
	cfg.BatchInterval = 200 * time.Millisecond // Faster batching for tests

	go master.RunSessionBatchProcessor(ctx, zap.NewNop().Sugar(), cfg, sourceCh, storeFunc)

	// Send some messages
	for i := 0; i < 5; i++ {
		sourceCh <- &gp.SessionDataWrite{
			GpStatInfo: &gp.GpStatActivity{SessID: i},
		}
	}

	// Wait for processing
	time.Sleep(600 * time.Millisecond)

	assert.Equal(t, int64(5), processedCount.Load())
}

func TestRunQueryBatchProcessorProcessesMessages(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	sourceCh := make(chan *pbm.QueryStatWrite, 10)
	var processedCount atomic.Int64

	storeFunc := func(ctx context.Context, queries []*pbm.QueryStatWrite) error {
		processedCount.Add(int64(len(queries)))
		return nil
	}

	cfg := master.DefaultBatchProcessorConfig()
	cfg.BatchInterval = 200 * time.Millisecond

	go master.RunQueryBatchProcessor(ctx, zap.NewNop().Sugar(), cfg, sourceCh, storeFunc)

	// Send some messages
	for i := 0; i < 5; i++ {
		sourceCh <- &pbm.QueryStatWrite{
			QueryKey: &pbc.QueryKey{Ssid: int32(i)},
		}
	}

	// Wait for processing
	time.Sleep(600 * time.Millisecond)

	assert.Equal(t, int64(5), processedCount.Load())
}

func TestRunSegmentBatchProcessorProcessesMessages(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	sourceCh := make(chan *pbm.SegmentMetricsWrite, 10)
	var processedCount atomic.Int64

	storeFunc := func(ctx context.Context, metrics []*pbm.SegmentMetricsWrite) error {
		processedCount.Add(int64(len(metrics)))
		return nil
	}

	cfg := master.DefaultBatchProcessorConfig()
	cfg.BatchInterval = 200 * time.Millisecond

	go master.RunSegmentBatchProcessor(ctx, zap.NewNop().Sugar(), cfg, sourceCh, storeFunc)

	// Send some messages
	for i := 0; i < 5; i++ {
		sourceCh <- &pbm.SegmentMetricsWrite{
			QueryKey: &pbc.QueryKey{Ssid: int32(i)},
		}
	}

	// Wait for processing
	time.Sleep(600 * time.Millisecond)

	assert.Equal(t, int64(5), processedCount.Load())
}

func TestBatchProcessorDropsOnFullPipe(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	sourceCh := make(chan *gp.SessionDataWrite, 100)
	var processedCount atomic.Int64
	var droppedCount atomic.Int64

	// Slow store function that simulates write failures
	storeFunc := func(ctx context.Context, sessions []*gp.SessionDataWrite) error {
		// Count dropped messages when error returned
		droppedCount.Add(int64(len(sessions)))
		return assert.AnError // Simulate write error
	}

	cfg := master.DefaultBatchProcessorConfig()
	cfg.BatchInterval = 100 * time.Millisecond
	cfg.BatchQueueSize = 2 // Very small pipe

	go master.RunSessionBatchProcessor(ctx, zap.NewNop().Sugar(), cfg, sourceCh, storeFunc)

	// Send many messages quickly to fill the pipe
	for i := 0; i < 20; i++ {
		sourceCh <- &gp.SessionDataWrite{
			GpStatInfo: &gp.GpStatActivity{SessID: i},
		}
	}

	// Wait for processing
	time.Sleep(800 * time.Millisecond)

	// Some messages should have been processed or dropped
	total := processedCount.Load() + droppedCount.Load()
	assert.Greater(t, total, int64(0), "Expected some messages to be processed or dropped")
}

func TestFileWritersCreatesWritersSuccessfully(t *testing.T) {
	tmpDir := t.TempDir()
	logger := zap.NewNop().Sugar()

	writers, err := master.NewFileWriters(
		logger,
		tmpDir+"/sessions.json",
		tmpDir+"/queries.json",
		tmpDir+"/segments.json",
		1024*1024,
	)
	require.NoError(t, err)
	require.NotNil(t, writers)
}

func TestFileWritersStoreSessions(t *testing.T) {
	tmpDir := t.TempDir()
	logger := zap.NewNop().Sugar()

	writers, err := master.NewFileWriters(
		logger,
		tmpDir+"/sessions.json",
		tmpDir+"/queries.json",
		tmpDir+"/segments.json",
		1024*1024,
	)
	require.NoError(t, err)

	ctx := context.Background()
	sessions := []*gp.SessionDataWrite{
		{
			GpStatInfo:   &gp.GpStatActivity{SessID: 1},
			RunningQuery: &storage.QueryKeyWrite{Ssid: 1},
		},
		{
			GpStatInfo:   &gp.GpStatActivity{SessID: 2},
			RunningQuery: &storage.QueryKeyWrite{Ssid: 2},
		},
	}

	err = writers.StoreSessions(ctx, sessions)
	assert.NoError(t, err)
}

func TestFileWritersStoreQuery(t *testing.T) {
	tmpDir := t.TempDir()
	logger := zap.NewNop().Sugar()

	writers, err := master.NewFileWriters(
		logger,
		tmpDir+"/sessions.json",
		tmpDir+"/queries.json",
		tmpDir+"/segments.json",
		1024*1024,
	)
	require.NoError(t, err)

	ctx := context.Background()
	queries := []*pbm.QueryStatWrite{
		{QueryKey: &pbc.QueryKey{Ssid: 1}},
		{QueryKey: &pbc.QueryKey{Ssid: 2}},
	}

	err = writers.StoreQuery(ctx, queries)
	assert.NoError(t, err)
}

func TestFileWritersStoreSegmensMetrics(t *testing.T) {
	tmpDir := t.TempDir()
	logger := zap.NewNop().Sugar()

	writers, err := master.NewFileWriters(
		logger,
		tmpDir+"/sessions.json",
		tmpDir+"/queries.json",
		tmpDir+"/segments.json",
		1024*1024,
	)
	require.NoError(t, err)

	ctx := context.Background()
	metrics := []*pbm.SegmentMetricsWrite{
		{QueryKey: &pbc.QueryKey{Ssid: 1}},
		{QueryKey: &pbc.QueryKey{Ssid: 2}},
	}

	err = writers.StoreSegmensMetrics(ctx, metrics)
	assert.NoError(t, err)
}
