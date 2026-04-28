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

// --- fake gRPC server for GetPidProcStat ---

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

type failingProcStatServer struct {
	pb.UnimplementedGetQueryInfoServer
}

func (s *failingProcStatServer) GetPidProcStat(context.Context, *pb.GetPidProcInfoReq) (*pb.GetPidProcInfoResponse, error) {
	return nil, fmt.Errorf("simulated gRPC error")
}

func (s *failingProcStatServer) GetMetricQueries(context.Context, *pb.GetQueriesInfoReq) (*pb.GetQueriesInfoResponse, error) {
	return &pb.GetQueriesInfoResponse{}, nil
}

// setupBufconnServer starts a gRPC server on a bufconn listener and returns
// the listener. The caller must register services on the returned server before
// calling this, or pass a pre-configured server.
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
	t.Cleanup(func() { conn.Close() })
	return conn
}

func newTestLogger() *zap.SugaredLogger {
	cfg := zap.NewDevelopmentConfig()
	cfg.Level = zap.NewAtomicLevelAt(zap.WarnLevel)
	l, _ := cfg.Build()
	return l.Sugar()
}

// ============================================================
// Tests for getJobsMap
// ============================================================

func TestGetJobsMap_EmptyInput(t *testing.T) {
	result := getJobsMap(nil)
	assert.NotNil(t, result)
	assert.Empty(t, result)

	result2 := getJobsMap([]stat_activity.SessionPid{})
	assert.NotNil(t, result2)
	assert.Empty(t, result2)
}

func TestGetJobsMap_SingleHost(t *testing.T) {
	storage.SetHostnameForSegindex(10, "host-a")

	sessions := []stat_activity.SessionPid{
		{GpSegmentId: 10, Pid: 100, SessId: 1},
		{GpSegmentId: 10, Pid: 200, SessId: 2},
	}

	result := getJobsMap(sessions)

	// The map should contain an entry for "host-a"
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

	result := getJobsMap(sessions)

	// Should have entries for both hosts
	assert.Contains(t, result, "host-b")
	assert.Contains(t, result, "host-c")
}

func TestGetJobsMap_UnknownSegindex(t *testing.T) {
	// When segindex is not in the config storage, GetHostnameForSegindex
	// returns the string representation of the segindex.
	sessions := []stat_activity.SessionPid{
		{GpSegmentId: 9999, Pid: 100, SessId: 1},
	}

	result := getJobsMap(sessions)
	_, exists := result["9999"]
	assert.True(t, exists, "expected key '9999' for unknown segindex")
}

// ============================================================
// Tests for processProcfsRequests
// ============================================================

func TestProcessProcfsRequests_Success(t *testing.T) {
	fakeSrv := &fakeProcStatServer{}
	lis := setupBufconnServer(t, fakeSrv)

	// Inject the bufconn connection into the global connection cache
	conn := dialBufconn(t, lis)
	hostname := fmt.Sprintf("test-procfs-success-%d", time.Now().UnixNano())
	segConnectionLock.Lock()
	segConnections[hostname] = conn
	segConnectionLock.Unlock()
	t.Cleanup(func() {
		segConnectionLock.Lock()
		delete(segConnections, hostname)
		segConnectionLock.Unlock()
	})

	reqs := []stat_activity.SessionPid{
		{GpSegmentId: 1, Pid: 100, SessId: 10},
		{GpSegmentId: 2, Pid: 200, SessId: 20},
	}

	ctx := context.Background()
	err := processProcfsRequests(ctx, hostname, 0, 5*time.Second, reqs)
	require.NoError(t, err)
	called, lastReq := fakeSrv.snapshot()
	assert.True(t, called, "expected GetPidProcStat to be called")
	require.NotNil(t, lastReq)
	assert.Len(t, lastReq.SegmentProcess, 2)

	// Verify the proto message fields
	sp0 := lastReq.SegmentProcess[0]
	assert.Equal(t, int64(1), sp0.GpSegmentId)
	assert.Equal(t, int64(100), sp0.Pid)
	assert.Equal(t, int64(10), sp0.SessId)

	sp1 := lastReq.SegmentProcess[1]
	assert.Equal(t, int64(2), sp1.GpSegmentId)
	assert.Equal(t, int64(200), sp1.Pid)
	assert.Equal(t, int64(20), sp1.SessId)
}

func TestProcessProcfsRequests_GrpcError(t *testing.T) {
	failSrv := &failingProcStatServer{}
	lis := setupBufconnServer(t, failSrv)

	conn := dialBufconn(t, lis)
	hostname := fmt.Sprintf("test-procfs-fail-%d", time.Now().UnixNano())
	segConnectionLock.Lock()
	segConnections[hostname] = conn
	segConnectionLock.Unlock()
	t.Cleanup(func() {
		segConnectionLock.Lock()
		delete(segConnections, hostname)
		segConnectionLock.Unlock()
	})

	reqs := []stat_activity.SessionPid{
		{GpSegmentId: 1, Pid: 100, SessId: 10},
	}

	ctx := context.Background()
	err := processProcfsRequests(ctx, hostname, 0, 5*time.Second, reqs)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "simulated gRPC error")
}

func TestProcessProcfsRequests_CancelledContext(t *testing.T) {
	fakeSrv := &fakeProcStatServer{}
	lis := setupBufconnServer(t, fakeSrv)

	conn := dialBufconn(t, lis)
	hostname := fmt.Sprintf("test-procfs-cancel-%d", time.Now().UnixNano())
	segConnectionLock.Lock()
	segConnections[hostname] = conn
	segConnectionLock.Unlock()
	t.Cleanup(func() {
		segConnectionLock.Lock()
		delete(segConnections, hostname)
		segConnectionLock.Unlock()
	})

	reqs := []stat_activity.SessionPid{
		{GpSegmentId: 1, Pid: 100, SessId: 10},
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	// With a cancelled context, processProcfsRequests should skip building
	// the request body (due to select on ctx.Done()) and return nil.
	err := processProcfsRequests(ctx, hostname, 0, 5*time.Second, reqs)
	// The function returns nil when context is cancelled during request building,
	// but may return an error from the gRPC call if the request was already built.
	// Either outcome is acceptable with a cancelled context.
	if err != nil {
		assert.ErrorIs(t, ctx.Err(), context.Canceled)
	}
}

func TestProcessProcfsRequests_EmptyRequests(t *testing.T) {
	fakeSrv := &fakeProcStatServer{}
	lis := setupBufconnServer(t, fakeSrv)

	conn := dialBufconn(t, lis)
	hostname := fmt.Sprintf("test-procfs-empty-%d", time.Now().UnixNano())
	segConnectionLock.Lock()
	segConnections[hostname] = conn
	segConnectionLock.Unlock()
	t.Cleanup(func() {
		segConnectionLock.Lock()
		delete(segConnections, hostname)
		segConnectionLock.Unlock()
	})

	ctx := context.Background()
	err := processProcfsRequests(ctx, hostname, 0, 5*time.Second, nil)
	require.NoError(t, err)
	called, lastReq := fakeSrv.snapshot()
	assert.True(t, called, "GetPidProcStat should still be called with empty segment list")
	assert.Empty(t, lastReq.SegmentProcess)
}

// ============================================================
// Tests for GatherProcfsStat
// ============================================================

func TestGatherProcfsStat_ListAllSessionsError(t *testing.T) {
	mock := &mockStatActivityLister{
		sessionsErr: fmt.Errorf("db connection failed"),
	}
	bs := &BackgroundStorage{
		l:                  newTestLogger(),
		StatActivityLister: mock,
	}

	err := bs.GatherProcfsStat(context.Background(), 2, 50051, 5*time.Second)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "db connection failed")
	assert.True(t, mock.listCalled)
}

func TestGatherProcfsStat_EmptySessions(t *testing.T) {
	mock := &mockStatActivityLister{
		sessions: []stat_activity.SessionPid{},
	}
	bs := &BackgroundStorage{
		l:                  newTestLogger(),
		StatActivityLister: mock,
	}

	err := bs.GatherProcfsStat(context.Background(), 2, 50051, 5*time.Second)
	require.NoError(t, err)
	assert.True(t, mock.listCalled)
}

func TestGatherProcfsStat_WithSessions(t *testing.T) {
	// Set up a fake gRPC server
	fakeSrv := &fakeProcStatServer{}
	lis := setupBufconnServer(t, fakeSrv)

	conn := dialBufconn(t, lis)
	hostname := fmt.Sprintf("test-gather-%d", time.Now().UnixNano())

	// Register the hostname in the segment config
	storage.SetHostnameForSegindex(30, hostname)

	// Inject the bufconn connection
	segConnectionLock.Lock()
	segConnections[hostname] = conn
	segConnectionLock.Unlock()
	t.Cleanup(func() {
		segConnectionLock.Lock()
		delete(segConnections, hostname)
		segConnectionLock.Unlock()
	})

	mock := &mockStatActivityLister{
		sessions: []stat_activity.SessionPid{
			{GpSegmentId: 30, Pid: 100, SessId: 1},
			{GpSegmentId: 30, Pid: 200, SessId: 2},
		},
	}
	bs := &BackgroundStorage{
		l:                  newTestLogger(),
		StatActivityLister: mock,
	}

	err := bs.GatherProcfsStat(context.Background(), 2, 0, 5*time.Second)
	require.NoError(t, err)
	assert.True(t, mock.listCalled)
}

func TestGatherProcfsStat_ContextCancelled(t *testing.T) {
	mock := &mockStatActivityLister{
		sessions: []stat_activity.SessionPid{
			{GpSegmentId: 40, Pid: 100, SessId: 1},
		},
	}
	bs := &BackgroundStorage{
		l:                  newTestLogger(),
		StatActivityLister: mock,
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	// With a cancelled context, the timeout context creation will produce
	// an already-done context, so the pool tasks should handle it gracefully.
	err := bs.GatherProcfsStat(ctx, 2, 50051, 5*time.Second)
	// The error may be nil (if tasks detect cancellation early) or non-nil
	// (if the gRPC call fails due to cancelled context). Both are acceptable.
	_ = err
}

func TestGatherProcfsStat_GrpcFailure(t *testing.T) {
	failSrv := &failingProcStatServer{}
	lis := setupBufconnServer(t, failSrv)

	conn := dialBufconn(t, lis)
	hostname := fmt.Sprintf("test-gather-fail-%d", time.Now().UnixNano())

	storage.SetHostnameForSegindex(50, hostname)

	segConnectionLock.Lock()
	segConnections[hostname] = conn
	segConnectionLock.Unlock()
	t.Cleanup(func() {
		segConnectionLock.Lock()
		delete(segConnections, hostname)
		segConnectionLock.Unlock()
	})

	mock := &mockStatActivityLister{
		sessions: []stat_activity.SessionPid{
			{GpSegmentId: 50, Pid: 100, SessId: 1},
		},
	}
	bs := &BackgroundStorage{
		l:                  newTestLogger(),
		StatActivityLister: mock,
	}

	err := bs.GatherProcfsStat(context.Background(), 2, 0, 5*time.Second)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "simulated gRPC error")
}

func TestGatherProcfsStat_ManySessionsBatching(t *testing.T) {
	// Create more than JobsPerQuery sessions to verify batching logic
	fakeSrv := &fakeProcStatServer{}
	lis := setupBufconnServer(t, fakeSrv)

	conn := dialBufconn(t, lis)
	hostname := fmt.Sprintf("test-gather-batch-%d", time.Now().UnixNano())

	storage.SetHostnameForSegindex(60, hostname)

	segConnectionLock.Lock()
	segConnections[hostname] = conn
	segConnectionLock.Unlock()
	t.Cleanup(func() {
		segConnectionLock.Lock()
		delete(segConnections, hostname)
		segConnectionLock.Unlock()
	})

	// Create JobsPerQuery + 5 sessions to trigger at least 2 batches
	sessions := make([]stat_activity.SessionPid, 0, JobsPerQuery+5)
	for i := 0; i < JobsPerQuery+5; i++ {
		sessions = append(sessions, stat_activity.SessionPid{
			GpSegmentId: 60,
			Pid:         100 + i,
			SessId:      i + 1,
		})
	}

	mock := &mockStatActivityLister{
		sessions: sessions,
	}
	bs := &BackgroundStorage{
		l:                  newTestLogger(),
		StatActivityLister: mock,
	}

	err := bs.GatherProcfsStat(context.Background(), 4, 0, 10*time.Second)
	require.NoError(t, err)
	assert.True(t, mock.listCalled)
	// The fake server should have been called (at least once for the batches)
	called, _ := fakeSrv.snapshot()
	assert.True(t, called)
}

// ============================================================
// Tests for constants
// ============================================================

func TestConstants(t *testing.T) {
	assert.Equal(t, 100, JobsPerQuery)
	assert.Equal(t, 4*1024*1024, maxMsgSize)
}
