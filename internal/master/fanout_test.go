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
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pbm "github.com/open-gpdb/yagpcc/api/proto/agent_master"
	"github.com/open-gpdb/yagpcc/internal/config"
	"github.com/open-gpdb/yagpcc/internal/gp"
)

func TestFanOut_DeliversToAllTargets(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	source := make(chan int, 10)
	a := make(chan int, 10)
	b := make(chan int, 10)
	go fanOut(ctx, source, []chan int{a, b}, "test", []string{"a", "b"}, nil)

	for i := 0; i < 5; i++ {
		source <- i
	}

	for i := 0; i < 5; i++ {
		select {
		case v := <-a:
			assert.Equal(t, i, v)
		case <-time.After(time.Second):
			t.Fatalf("target a did not receive message %d", i)
		}
		select {
		case v := <-b:
			assert.Equal(t, i, v)
		case <-time.After(time.Second):
			t.Fatalf("target b did not receive message %d", i)
		}
	}
}

func TestFanOut_SlowTargetDoesNotBlockFast(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	source := make(chan int, 100)
	fast := make(chan int, 100)
	slow := make(chan int, 1) // tiny buffer: fills up and drops the rest

	go fanOut(ctx, source, []chan int{fast, slow}, "test", []string{"fast", "slow"}, nil)

	const n = 50
	for i := 0; i < n; i++ {
		source <- i
	}

	// The fast target must receive every message even though the slow target is
	// never drained.
	got := 0
	deadline := time.After(2 * time.Second)
	for got < n {
		select {
		case <-fast:
			got++
		case <-deadline:
			t.Fatalf("fast target only received %d/%d messages", got, n)
		}
	}
	assert.Equal(t, n, got)
}

func newFileTarget(dir string) config.WriterTarget {
	return config.WriterTarget{
		Type:         "file",
		Enabled:      true,
		SessionsFile: dir + "/sessions.json",
		QueriesFile:  dir + "/queries.json",
		SegmentsFile: dir + "/segments.json",
		MaxFileSize:  1 << 20,
	}
}

func fastBatchConfig() config.BatchProcessorConfig {
	cfg := config.DefaultBatchProcessorConfig()
	cfg.BatchInterval = 50 * time.Millisecond
	cfg.WriteTimeout = 200 * time.Millisecond
	return cfg
}

func TestLaunchArchiveWriters_SingleFileTarget(t *testing.T) {
	dir := t.TempDir()
	bs := &BackgroundStorage{l: testLogger()}
	cfg := &config.WriterConfig{
		BatchProcessorConfig: fastBatchConfig(),
		Targets:              []config.WriterTarget{newFileTarget(dir)},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	queryChan := make(chan *pbm.QueryStatWrite, 10)
	sessChan := make(chan *gp.SessionDataWrite, 10)
	segChan := make(chan *pbm.SegmentMetricsWrite, 10)

	require.NoError(t, bs.launchArchiveWriters(ctx, cfg, queryChan, sessChan, segChan))

	sessChan <- sampleSession()
	assertFileNonEmpty(t, dir+"/sessions.json")
}

func TestLaunchArchiveWriters_MultiTargetFileStillWrites(t *testing.T) {
	dir := t.TempDir()
	bs := &BackgroundStorage{l: testLogger()}
	cfg := &config.WriterConfig{
		BatchProcessorConfig: fastBatchConfig(),
		Targets: []config.WriterTarget{
			newFileTarget(dir),
			// An unreachable ClickHouse target: it must not stop the file target
			// from receiving data. Its inserts fail per batch and get dropped.
			{Type: "clickhouse", Enabled: true, Addrs: []string{"127.0.0.1:1"}, Password: "x"},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	queryChan := make(chan *pbm.QueryStatWrite, 10)
	sessChan := make(chan *gp.SessionDataWrite, 10)
	segChan := make(chan *pbm.SegmentMetricsWrite, 10)

	require.NoError(t, bs.launchArchiveWriters(ctx, cfg, queryChan, sessChan, segChan))

	for i := 0; i < 5; i++ {
		sessChan <- sampleSession()
	}
	assertFileNonEmpty(t, dir+"/sessions.json")
}

func TestBuildArchiveWriters_SkipsDisabledAndNamesTargets(t *testing.T) {
	dir := t.TempDir()
	bs := &BackgroundStorage{l: testLogger()}
	writers, err := bs.buildArchiveWriters([]config.WriterTarget{
		newFileTarget(dir),
		{Type: "clickhouse", Enabled: false, Addrs: []string{"ch:9000"}},
		{Type: "clickhouse", Enabled: true, Addrs: []string{"ch:9000"}, Password: "x"},
	})
	require.NoError(t, err)
	require.Len(t, writers, 2)
	assert.Equal(t, "file", writers[0].name)
	assert.Equal(t, "clickhouse", writers[1].name)
}

func TestBuildArchiveWriters_NoEnabledTargets(t *testing.T) {
	bs := &BackgroundStorage{l: testLogger()}
	_, err := bs.buildArchiveWriters([]config.WriterTarget{
		{Type: "file", Enabled: false},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no enabled archive writer targets")
}

func TestNewArchiveWriter_UnknownType(t *testing.T) {
	bs := &BackgroundStorage{l: testLogger()}
	_, err := bs.newArchiveWriter(config.WriterTarget{Type: "kafka", Enabled: true})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown writer target type")
}

// assertFileNonEmpty waits briefly for the batch processor to flush and then
// asserts the file has content.
func assertFileNonEmpty(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(path)
		if err == nil && len(data) > 0 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("file %s stayed empty", path)
}
