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
	"fmt"
	"log"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	pb "github.com/open-gpdb/yagpcc/api/proto/agent_segment"
	pbc "github.com/open-gpdb/yagpcc/api/proto/common"
	"github.com/open-gpdb/yagpcc/internal/gp"
	"github.com/open-gpdb/yagpcc/internal/gp/stat_activity"
	"github.com/open-gpdb/yagpcc/internal/storage"
)

// --- mock statActivityLister ---

type mockStatActivityLister struct {
	sessions    []stat_activity.SessionPid
	sessionsErr error
	listCalled  bool
}

func (m *mockStatActivityLister) Start(context.Context) error { return nil }
func (m *mockStatActivityLister) Stop()                       {}
func (m *mockStatActivityLister) List(context.Context) ([]*gp.GpStatActivity, error) {
	return nil, nil
}
func (m *mockStatActivityLister) ListAllSessions(context.Context) ([]stat_activity.SessionPid, error) {
	m.listCalled = true
	return m.sessions, m.sessionsErr
}

// --- fake gRPC servers ---

// fakeProcStatServer returns empty responses (no PidProcData).
type fakeProcStatServer struct {
	pb.UnimplementedGetQueryInfoServer
	mu      sync.Mutex
	called  bool
	lastReq *pb.GetPidProcInfoReq
}

func (s *fakeProcStatServer) GetPidProcStat(_ context.Context, req *pb.GetPidProcInfoReq) (*pb.GetPidProcInfoResponse, error) {
	s.mu.Lock()
	s.called = true
	s.lastReq = req
	s.mu.Unlock()
	return &pb.GetPidProcInfoResponse{}, nil
}

func (s *fakeProcStatServer) snapshot() (bool, *pb.GetPidProcInfoReq) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.called, s.lastReq
}

func (s *fakeProcStatServer) GetMetricQueries(_ context.Context, _ *pb.GetQueriesInfoReq) (*pb.GetQueriesInfoResponse, error) {
	return &pb.GetQueriesInfoResponse{}, nil
}

// dataProcStatServer returns actual PidProcData echoing back the requested segments.
type dataProcStatServer struct {
	pb.UnimplementedGetQueryInfoServer
	mu      sync.Mutex
	calls   int
	allData []*pbc.GpPidProcInfo
}

func (s *dataProcStatServer) GetPidProcStat(_ context.Context, req *pb.GetPidProcInfoReq) (*pb.GetPidProcInfoResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	resp := &pb.GetPidProcInfoResponse{
		PidProcData: make([]*pbc.GpPidProcInfo, 0, len(req.SegmentProcess)),
	}
	for _, sp := range req.SegmentProcess {
		info := &pbc.GpPidProcInfo{
			GpSegmentId: sp.GpSegmentId,
			SessId:      sp.SessId,
			Pid:         sp.Pid,
			Cmdline:     fmt.Sprintf("cmd-%d", sp.Pid),
			State:       "S",
			Ccnt:        int32(sp.Pid / 10),
			SliceId:     sp.GpSegmentId + 1,
		}
		resp.PidProcData = append(resp.PidProcData, info)
		s.allData = append(s.allData, info)
	}
	return resp, nil
}

func (s *dataProcStatServer) GetMetricQueries(context.Context, *pb.GetQueriesInfoReq) (*pb.GetQueriesInfoResponse, error) {
	return &pb.GetQueriesInfoResponse{}, nil
}

func (s *dataProcStatServer) GetHostStat(context.Context, *pb.GetHostStatReq) (*pb.GetHostStatResponse, error) {
	return &pb.GetHostStatResponse{
		LoadAvg:  &pb.LoadAvg{Avg1: 1.0, Avg5: 5.0, Avg15: 15.0},
		CpuUsage: &pb.CpuUsage{CpuUsage: 0.42},
	}, nil
}

func (s *dataProcStatServer) getCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

type failingProcStatServer struct {
	pb.UnimplementedGetQueryInfoServer
}

func (s *failingProcStatServer) GetPidProcStat(context.Context, *pb.GetPidProcInfoReq) (*pb.GetPidProcInfoResponse, error) {
	return nil, fmt.Errorf("simulated gRPC error")
}

func (s *failingProcStatServer) GetMetricQueries(context.Context, *pb.GetQueriesInfoReq) (*pb.GetQueriesInfoResponse, error) {
	return &pb.GetQueriesInfoResponse{}, nil
}

// --- test helpers ---

func setupBufconnServer(t *testing.T, srv pb.GetQueryInfoServer) *bufconn.Listener {
	t.Helper()
	lis := bufconn.Listen(1024 * 1024)
	s := grpc.NewServer()
	pb.RegisterGetQueryInfoServer(s, srv)
	go func() {
		if err := s.Serve(lis); err != nil {
			log.Printf("bufconn server exited: %v", err)
		}
	}()
	t.Cleanup(func() { s.Stop() })
	return lis
}

func dialBufconn(t *testing.T, lis *bufconn.Listener) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient(
		"passthrough:///bufconn",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func newTestLogger() *zap.SugaredLogger {
	cfg := zap.NewDevelopmentConfig()
	cfg.Level = zap.NewAtomicLevelAt(zap.WarnLevel)
	l, _ := cfg.Build()
	return l.Sugar()
}

// newTestProcfsGatherStorage creates a properly initialized ProcfsGatherStorage for tests.
func newTestProcfsGatherStorage(mock statActivityLister) *ProcfsGatherStorage {
	return NewProcfsGatherStorage(newTestLogger(), mock)
}

// injectBufconn registers a bufconn connection for the given hostname in the global cache.
func injectBufconn(t *testing.T, lis *bufconn.Listener) string {
	t.Helper()
	conn := dialBufconn(t, lis)
	hostname := fmt.Sprintf("test-%d", time.Now().UnixNano())
	segConnectionLock.Lock()
	segConnections[hostname] = conn
	segConnectionLock.Unlock()
	t.Cleanup(func() {
		segConnectionLock.Lock()
		delete(segConnections, hostname)
		segConnectionLock.Unlock()
	})
	return hostname
}

// ============================================================
// Tests for NewProcfsGatherStorage
// ============================================================

func TestNewProcfsGatherStorage(t *testing.T) {
	mock := &mockStatActivityLister{}
	ps := NewProcfsGatherStorage(newTestLogger(), mock)

	require.NotNil(t, ps)
	assert.NotNil(t, ps.l)
	assert.NotNil(t, ps.statActivityLister)
}

// ============================================================
// Tests for addProcfsStat
// ============================================================

// ============================================================
// Tests for getJobsMap
// ============================================================

func TestGetJobsMap_EmptyInput(t *testing.T) {
	ps := newTestProcfsGatherStorage(nil)
	result := ps.getJobsMap(nil)
	assert.NotNil(t, result)
	assert.Empty(t, result)

	result2 := ps.getJobsMap([]stat_activity.SessionPid{})
	assert.NotNil(t, result2)
	assert.Empty(t, result2)
}

func TestGetJobsMap_SingleHost(t *testing.T) {
	storage.SetHostnameForSegindex(10, "host-a")

	sessions := []stat_activity.SessionPid{
		{GpSegmentId: 10, Pid: 100, SessId: 1},
		{GpSegmentId: 10, Pid: 200, SessId: 2},
	}

	ps := newTestProcfsGatherStorage(nil)
	result := ps.getJobsMap(sessions)

	_, exists := result["host-a"]
	assert.True(t, exists, "expected key 'host-a' in hostJobMap")
}

func TestGetJobsMap_MultipleHosts(t *testing.T) {
	storage.SetHostnameForSegindex(20, "host-b")
	storage.SetHostnameForSegindex(21, "host-c")

	sessions := []stat_activity.SessionPid{
		{GpSegmentId: 20, Pid: 100, SessId: 1},
		{GpSegmentId: 21, Pid: 200, SessId: 2},
		{GpSegmentId: 20, Pid: 300, SessId: 3},
	}

	ps := newTestProcfsGatherStorage(nil)
	result := ps.getJobsMap(sessions)

	assert.Contains(t, result, "host-b")
	assert.Contains(t, result, "host-c")
}

func TestGetJobsMap_UnknownSegindex(t *testing.T) {
	sessions := []stat_activity.SessionPid{
		{GpSegmentId: 9999, Pid: 100, SessId: 1},
	}

	ps := newTestProcfsGatherStorage(nil)
	result := ps.getJobsMap(sessions)
	_, exists := result["9999"]
	assert.True(t, exists, "expected key '9999' for unknown segindex")
}

// ============================================================
// Tests for processProcfsRequests
// ============================================================

func TestProcessProcfsRequests_Success(t *testing.T) {
	fakeSrv := &fakeProcStatServer{}
	lis := setupBufconnServer(t, fakeSrv)
	hostname := injectBufconn(t, lis)

	reqs := []stat_activity.SessionPid{
		{GpSegmentId: 1, Pid: 100, SessId: 10},
		{GpSegmentId: 2, Pid: 200, SessId: 20},
	}

	ps := newTestProcfsGatherStorage(nil)
	ctx := context.Background()
	_, err := ps.processProcfsRequests(ctx, hostname, 0, 5*time.Second, 4*1024*1024, reqs)
	require.NoError(t, err)
	called, lastReq := fakeSrv.snapshot()
	assert.True(t, called, "expected GetPidProcStat to be called")
	require.NotNil(t, lastReq)
	assert.Len(t, lastReq.SegmentProcess, 2)

	sp0 := lastReq.SegmentProcess[0]
	assert.Equal(t, int64(1), sp0.GpSegmentId)
	assert.Equal(t, int64(100), sp0.Pid)
	assert.Equal(t, int64(10), sp0.SessId)

	sp1 := lastReq.SegmentProcess[1]
	assert.Equal(t, int64(2), sp1.GpSegmentId)
	assert.Equal(t, int64(200), sp1.Pid)
	assert.Equal(t, int64(20), sp1.SessId)
}

func TestProcessProcfsRequests_SavesData(t *testing.T) {
	dataSrv := &dataProcStatServer{}
	lis := setupBufconnServer(t, dataSrv)
	hostname := injectBufconn(t, lis)

	reqs := []stat_activity.SessionPid{
		{GpSegmentId: 1, Pid: 100, SessId: 10},
		{GpSegmentId: 2, Pid: 200, SessId: 20},
	}

	ps := newTestProcfsGatherStorage(nil)
	result, err := ps.processProcfsRequests(context.Background(), hostname, 0, 5*time.Second, 4*1024*1024, reqs)
	require.NoError(t, err)

	// Verify data was returned directly
	require.Len(t, result, 2)
	assert.Equal(t, int64(1), result[0].GpSegmentId)
	assert.Equal(t, int64(100), result[0].Pid)
	assert.Equal(t, "cmd-100", result[0].Cmdline)
	assert.Equal(t, int32(10), result[0].Ccnt)
	assert.Equal(t, int64(2), result[0].SliceId)
	assert.Equal(t, int64(2), result[1].GpSegmentId)
	assert.Equal(t, int64(200), result[1].Pid)
	assert.Equal(t, "cmd-200", result[1].Cmdline)
	assert.Equal(t, int32(20), result[1].Ccnt)
	assert.Equal(t, int64(3), result[1].SliceId)
}

func TestProcessProcfsRequests_SavesDataWithBatching(t *testing.T) {
	dataSrv := &dataProcStatServer{}
	lis := setupBufconnServer(t, dataSrv)
	hostname := injectBufconn(t, lis)

	// Create jobsPerQuery + 5 requests to trigger batching
	reqs := make([]stat_activity.SessionPid, 0, jobsPerQuery+5)
	for i := 0; i < jobsPerQuery+5; i++ {
		reqs = append(reqs, stat_activity.SessionPid{
			GpSegmentId: 1,
			Pid:         100 + i,
			SessId:      i + 1,
		})
	}

	ps := newTestProcfsGatherStorage(nil)
	result, err := ps.processProcfsRequests(context.Background(), hostname, 0, 10*time.Second, 4*1024*1024, reqs)
	require.NoError(t, err)

	// Should have called the server at least twice (one batch of 1000, one of 5)
	assert.GreaterOrEqual(t, dataSrv.getCalls(), 2)

	// All data should be returned
	assert.Len(t, result, jobsPerQuery+5)
}

func TestProcessProcfsRequests_GrpcError(t *testing.T) {
	failSrv := &failingProcStatServer{}
	lis := setupBufconnServer(t, failSrv)
	hostname := injectBufconn(t, lis)

	reqs := []stat_activity.SessionPid{
		{GpSegmentId: 1, Pid: 100, SessId: 10},
	}

	ps := newTestProcfsGatherStorage(nil)
	_, err := ps.processProcfsRequests(context.Background(), hostname, 0, 5*time.Second, 4*1024*1024, reqs)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "simulated gRPC error")
}

func TestProcessProcfsRequests_CancelledContext(t *testing.T) {
	fakeSrv := &fakeProcStatServer{}
	lis := setupBufconnServer(t, fakeSrv)
	hostname := injectBufconn(t, lis)

	reqs := []stat_activity.SessionPid{
		{GpSegmentId: 1, Pid: 100, SessId: 10},
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	ps := newTestProcfsGatherStorage(nil)
	_, err := ps.processProcfsRequests(ctx, hostname, 0, 5*time.Second, 4*1024*1024, reqs)
	if err != nil {
		assert.ErrorIs(t, ctx.Err(), context.Canceled)
	}
}

func TestProcessProcfsRequests_EmptyRequests(t *testing.T) {
	fakeSrv := &fakeProcStatServer{}
	lis := setupBufconnServer(t, fakeSrv)
	hostname := injectBufconn(t, lis)

	ps := newTestProcfsGatherStorage(nil)
	result, err := ps.processProcfsRequests(context.Background(), hostname, 0, 5*time.Second, 4*1024*1024, nil)
	require.NoError(t, err)
	called, _ := fakeSrv.snapshot()
	assert.False(t, called, "GetPidProcStat should not be called with empty segment list")
	assert.Empty(t, result)
}

func TestProcessHostStatRequest_Success(t *testing.T) {
	dataSrv := &dataProcStatServer{}
	lis := setupBufconnServer(t, dataSrv)
	hostname := injectBufconn(t, lis)

	ps := newTestProcfsGatherStorage(nil)
	result, err := ps.processHostStatRequest(context.Background(), hostname, 0, 5*time.Second, 4*1024*1024)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.NotNil(t, result.LoadAvg)
	assert.Equal(t, 1.0, result.LoadAvg.Avg1)
	assert.Equal(t, 5.0, result.LoadAvg.Avg5)
	assert.Equal(t, 15.0, result.LoadAvg.Avg15)
	assert.Equal(t, 0.42, result.CPUUsage)
}

func TestGatherHostStatForHosts(t *testing.T) {
	dataSrv := &dataProcStatServer{}
	lis := setupBufconnServer(t, dataSrv)
	hostname := injectBufconn(t, lis)

	ps := newTestProcfsGatherStorage(nil)
	result := ps.GatherHostStatForHosts(context.Background(), 2, 0, 5*time.Second, 4*1024*1024, []string{hostname, "missing-host"})
	require.Contains(t, result, hostname)
	assert.Equal(t, 0.42, result[hostname].CPUUsage)
	assert.NotContains(t, result, "missing-host")
}

// ============================================================
// Tests for GatherProcfsStat
// ============================================================

func TestGatherProcfsStat_ListAllSessionsError(t *testing.T) {
	mock := &mockStatActivityLister{
		sessionsErr: fmt.Errorf("db connection failed"),
	}
	ps := newTestProcfsGatherStorage(mock)

	_, err := ps.GatherProcfsStat(context.Background(), 2, 50051, 5*time.Second, 4*1024*1024)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "db connection failed")
	assert.True(t, mock.listCalled)
}

func TestGatherProcfsStat_EmptySessions(t *testing.T) {
	mock := &mockStatActivityLister{
		sessions: []stat_activity.SessionPid{},
	}
	ps := newTestProcfsGatherStorage(mock)

	result, err := ps.GatherProcfsStat(context.Background(), 2, 50051, 5*time.Second, 4*1024*1024)
	require.NoError(t, err)
	assert.True(t, mock.listCalled)
	assert.Empty(t, result)
}

func TestGatherProcfsStat_WithSessions_SavesData(t *testing.T) {
	dataSrv := &dataProcStatServer{}
	lis := setupBufconnServer(t, dataSrv)
	hostname := injectBufconn(t, lis)

	storage.SetHostnameForSegindex(30, hostname)

	mock := &mockStatActivityLister{
		sessions: []stat_activity.SessionPid{
			{GpSegmentId: 30, Pid: 100, SessId: 1},
			{GpSegmentId: 30, Pid: 200, SessId: 2},
		},
	}
	ps := newTestProcfsGatherStorage(mock)

	result, err := ps.GatherProcfsStat(context.Background(), 2, 0, 5*time.Second, 4*1024*1024)
	require.NoError(t, err)
	assert.True(t, mock.listCalled)

	// Verify data was returned
	require.Len(t, result, 2)
	// Check that the PIDs match (order may vary due to concurrency)
	pids := make(map[int64]bool)
	for _, r := range result {
		pids[r.Pid] = true
	}
	assert.True(t, pids[100])
	assert.True(t, pids[200])
	assert.Equal(t, int64(1), ps.LastHostsExpected())
	assert.Equal(t, int64(1), ps.LastHostsResponded())
}

func TestGatherProcfsStat_ContextCancelled(t *testing.T) {
	mock := &mockStatActivityLister{
		sessions: []stat_activity.SessionPid{
			{GpSegmentId: 40, Pid: 100, SessId: 1},
		},
	}
	ps := newTestProcfsGatherStorage(mock)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := ps.GatherProcfsStat(ctx, 2, 50051, 5*time.Second, 4*1024*1024)
	_ = err // may or may not error
}

func TestGatherProcfsStat_GrpcFailure_AllHostsFail(t *testing.T) {
	failSrv := &failingProcStatServer{}
	lis := setupBufconnServer(t, failSrv)
	hostname := injectBufconn(t, lis)

	storage.SetHostnameForSegindex(50, hostname)

	mock := &mockStatActivityLister{
		sessions: []stat_activity.SessionPid{
			{GpSegmentId: 50, Pid: 100, SessId: 1},
		},
	}
	ps := newTestProcfsGatherStorage(mock)

	result, err := ps.GatherProcfsStat(context.Background(), 2, 0, 5*time.Second, 4*1024*1024)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "all 1 hosts failed")
	assert.Contains(t, err.Error(), "simulated gRPC error")
	assert.Nil(t, result)
}

func TestGatherProcfsStat_PartialFailure_ReturnsSuccessfulResults(t *testing.T) {
	// Set up a successful server
	dataSrv := &dataProcStatServer{}
	lisOk := setupBufconnServer(t, dataSrv)
	hostnameOk := injectBufconn(t, lisOk)

	// Set up a failing server
	failSrv := &failingProcStatServer{}
	lisFail := setupBufconnServer(t, failSrv)
	hostnameFail := injectBufconn(t, lisFail)

	storage.SetHostnameForSegindex(70, hostnameOk)
	storage.SetHostnameForSegindex(71, hostnameFail)

	mock := &mockStatActivityLister{
		sessions: []stat_activity.SessionPid{
			{GpSegmentId: 70, Pid: 100, SessId: 1},
			{GpSegmentId: 70, Pid: 200, SessId: 2},
			{GpSegmentId: 71, Pid: 300, SessId: 3},
		},
	}
	ps := newTestProcfsGatherStorage(mock)

	result, err := ps.GatherProcfsStat(context.Background(), 4, 0, 5*time.Second, 4*1024*1024)
	// Should NOT return error because only one host failed, not all
	require.NoError(t, err)
	assert.True(t, mock.listCalled)

	// Should have results from the successful host
	require.Len(t, result, 2)
	pids := make(map[int64]bool)
	for _, r := range result {
		pids[r.Pid] = true
	}
	assert.True(t, pids[100], "expected pid 100 from successful host")
	assert.True(t, pids[200], "expected pid 200 from successful host")
	assert.Equal(t, int64(2), ps.LastHostsExpected())
	assert.Equal(t, int64(1), ps.LastHostsResponded())
}

func TestGatherProcfsStat_AllHostsFail_MultipleHosts(t *testing.T) {
	// Set up two failing servers
	failSrv1 := &failingProcStatServer{}
	lisFail1 := setupBufconnServer(t, failSrv1)
	hostnameFail1 := injectBufconn(t, lisFail1)

	failSrv2 := &failingProcStatServer{}
	lisFail2 := setupBufconnServer(t, failSrv2)
	hostnameFail2 := injectBufconn(t, lisFail2)

	storage.SetHostnameForSegindex(80, hostnameFail1)
	storage.SetHostnameForSegindex(81, hostnameFail2)

	mock := &mockStatActivityLister{
		sessions: []stat_activity.SessionPid{
			{GpSegmentId: 80, Pid: 100, SessId: 1},
			{GpSegmentId: 81, Pid: 200, SessId: 2},
		},
	}
	ps := newTestProcfsGatherStorage(mock)

	result, err := ps.GatherProcfsStat(context.Background(), 4, 0, 5*time.Second, 4*1024*1024)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "all 2 hosts failed")
	assert.Nil(t, result)
}

func TestGatherProcfsStat_ManySessionsBatching(t *testing.T) {
	dataSrv := &dataProcStatServer{}
	lis := setupBufconnServer(t, dataSrv)
	hostname := injectBufconn(t, lis)

	storage.SetHostnameForSegindex(60, hostname)

	sessions := make([]stat_activity.SessionPid, 0, jobsPerQuery+5)
	for i := 0; i < jobsPerQuery+5; i++ {
		sessions = append(sessions, stat_activity.SessionPid{
			GpSegmentId: 60,
			Pid:         100 + i,
			SessId:      i + 1,
		})
	}

	mock := &mockStatActivityLister{sessions: sessions}
	ps := newTestProcfsGatherStorage(mock)

	result, err := ps.GatherProcfsStat(context.Background(), 4, 0, 10*time.Second, 4*1024*1024)
	require.NoError(t, err)
	assert.True(t, mock.listCalled)

	assert.Len(t, result, jobsPerQuery+5)
}

func TestGatherProcfsStat_InvalidNPullers(t *testing.T) {
	mock := &mockStatActivityLister{}
	ps := newTestProcfsGatherStorage(mock)

	for _, n := range []int{0, -1, -100} {
		_, err := ps.GatherProcfsStat(context.Background(), n, 50051, 5*time.Second, 4*1024*1024)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "nPullers must be greater than 0")
		assert.False(t, mock.listCalled, "ListAllSessions should not be called for invalid nPullers")
	}
}

// ============================================================
// Tests for constants
// ============================================================

func TestConstants(t *testing.T) {
	assert.Equal(t, 1000, jobsPerQuery)
}
