package grpc

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	pbm "github.com/open-gpdb/yagpcc/api/proto/agent_master"
	pbc "github.com/open-gpdb/yagpcc/api/proto/common"
	"github.com/open-gpdb/yagpcc/internal/gp"
	"github.com/open-gpdb/yagpcc/internal/master"
	"github.com/open-gpdb/yagpcc/internal/storage"
)

func newTestActionsServer(t *testing.T) *ActionsServer {
	t.Helper()
	z := zap.NewNop().Sugar()
	rq := storage.NewRunningQueriesStorage()
	sessStorage := gp.NewSessionsStorage(rq)
	agg := storage.NewAggregatedStorage(z)
	bg := master.NewBackgroundStorage(z, sessStorage, rq, agg)
	return &ActionsServer{
		Logger:            z,
		Timeout:           5 * time.Second,
		BackgroundStorage: bg,
	}
}

func actionsServerNoDB(t *testing.T) *ActionsServer {
	t.Helper()
	return &ActionsServer{
		Logger:  zap.NewNop().Sugar(),
		Timeout: 5 * time.Second,
	}
}

func TestSessionMatchesTerminateSessionsRequest(t *testing.T) {
	t.Parallel()

	req := func(db, user string, qid uint64) *pbm.TerminateSessionsRequest {
		return &pbm.TerminateSessionsRequest{
			Database: db,
			Username: user,
			QueryId:  qid,
		}
	}

	sess := func(db, user string, qid uint64) *pbc.SessionState {
		s := &pbc.SessionState{
			SessionInfo: &pbc.SessionInfo{
				Database: db,
				User:     user,
			},
		}
		if qid != 0 {
			s.RunningQueryInfo = &pbc.QueryInfo{QueryId: qid}
		}
		return s
	}

	tests := []struct {
		name    string
		session *pbc.SessionState
		in      *pbm.TerminateSessionsRequest
		want    bool
	}{
		{
			name:    "nil session",
			session: nil,
			in:      req("db", "", 0),
			want:    false,
		},
		{
			name: "nil SessionInfo",
			session: &pbc.SessionState{
				SessionInfo: nil,
			},
			in:   req("db", "", 0),
			want: false,
		},
		{
			name:    "database match",
			session: sess("appdb", "alice", 0),
			in:      req("appdb", "", 0),
			want:    true,
		},
		{
			name:    "database mismatch",
			session: sess("other", "alice", 0),
			in:      req("appdb", "", 0),
			want:    false,
		},
		{
			name:    "username match",
			session: sess("appdb", "bob", 0),
			in:      req("", "bob", 0),
			want:    true,
		},
		{
			name:    "username mismatch",
			session: sess("appdb", "bob", 0),
			in:      req("", "carol", 0),
			want:    false,
		},
		{
			name:    "database and username both must match",
			session: sess("appdb", "bob", 0),
			in:      req("appdb", "bob", 0),
			want:    true,
		},
		{
			name:    "database ok username wrong",
			session: sess("appdb", "bob", 0),
			in:      req("appdb", "carol", 0),
			want:    false,
		},
		{
			name:    "query_id only matches",
			session: sess("appdb", "bob", 42),
			in:      req("", "", 42),
			want:    true,
		},
		{
			name:    "query_id mismatch",
			session: sess("appdb", "bob", 42),
			in:      req("", "", 99),
			want:    false,
		},
		{
			name:    "query_id filter requires RunningQueryInfo",
			session: sess("appdb", "bob", 0),
			in:      req("", "", 1),
			want:    false,
		},
		{
			name:    "combined database username query_id",
			session: sess("appdb", "bob", 7),
			in:      req("appdb", "bob", 7),
			want:    true,
		},
		{
			name:    "combined fails on wrong query_id",
			session: sess("appdb", "bob", 7),
			in:      req("appdb", "bob", 8),
			want:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := sessionMatchesTerminateSessionsRequest(tt.session, tt.in)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestTerminateSessions_RequiresFilter(t *testing.T) {
	srv := newTestActionsServer(t)
	ctx := context.Background()

	_, err := srv.TerminateSessions(ctx, &pbm.TerminateSessionsRequest{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "should specify")
}

func TestTerminateSessions_NoMatchingSessions_ReturnsEmptyResponses(t *testing.T) {
	srv := newTestActionsServer(t)
	ctx := context.Background()

	resp, err := srv.TerminateSessions(ctx, &pbm.TerminateSessionsRequest{
		Database: "nonexistent_db",
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Empty(t, resp.TerminateResponse)
}

func TestTerminateSessions_NilBackgroundStorage_ReturnsError(t *testing.T) {
	srv := actionsServerNoDB(t)
	ctx := context.Background()

	_, err := srv.TerminateSessions(ctx, &pbm.TerminateSessionsRequest{
		Database: "somedb",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not initialized")
}

func TestTerminateSessions_MatchingSessions_ReturnsResponses(t *testing.T) {
	srv := newTestActionsServer(t)
	z := zap.NewNop().Sugar()

	err := srv.BackgroundStorage.SessionStorage.RefreshSessionList(z, []*gp.GpStatActivity{
		{SessID: 1, Datname: "appdb", Usename: "alice"},
		{SessID: 2, Datname: "otherdb", Usename: "bob"},
	}, false)
	require.NoError(t, err)

	ctx := context.Background()
	resp, err := srv.TerminateSessions(ctx, &pbm.TerminateSessionsRequest{
		Database: "appdb",
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	// Only the session matching "appdb" should appear in the response.
	require.Len(t, resp.TerminateResponse, 1)
	// CancelQuery fails because the database is not initialized in tests.
	assert.Equal(t, pbm.TerminateResponseStatusCode_TERMINATE_RESPONSE_STATUS_CODE_ERROR, resp.TerminateResponse[0].StatusCode)
}

func TestTerminateSessions_ForbiddenSessId_ReturnsPerSessionError(t *testing.T) {
	srv := newTestActionsServer(t)
	z := zap.NewNop().Sugar()

	// SessID == 0 is forbidden; non-system username ensures the session passes
	// the NotSystemSession filter and is returned by GetAllSessions.
	err := srv.BackgroundStorage.SessionStorage.RefreshSessionList(z, []*gp.GpStatActivity{
		{SessID: 0, Datname: "appdb", Usename: "alice"},
	}, false)
	require.NoError(t, err)

	ctx := context.Background()
	resp, err := srv.TerminateSessions(ctx, &pbm.TerminateSessionsRequest{
		Database: "appdb",
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Len(t, resp.TerminateResponse, 1)
	assert.Equal(t, pbm.TerminateResponseStatusCode_TERMINATE_RESPONSE_STATUS_CODE_ERROR, resp.TerminateResponse[0].StatusCode)
	assert.Contains(t, resp.TerminateResponse[0].StatusText, "forbidden")
}

func TestTerminateSessions_NilSessionStorage_ReturnsError(t *testing.T) {
	srv := &ActionsServer{
		Logger:            zap.NewNop().Sugar(),
		Timeout:           5 * time.Second,
		BackgroundStorage: &master.BackgroundStorage{},
	}
	ctx := context.Background()

	_, err := srv.TerminateSessions(ctx, &pbm.TerminateSessionsRequest{Database: "db"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not initialized")
}

func TestTerminateSessions_MixedSessions_ReturnsPerSessionResults(t *testing.T) {
	srv := newTestActionsServer(t)
	z := zap.NewNop().Sugar()

	// SessID=0 is forbidden; SessID=3 is a regular session that will fail CancelQuery.
	err := srv.BackgroundStorage.SessionStorage.RefreshSessionList(z, []*gp.GpStatActivity{
		{SessID: 0, Datname: "appdb", Usename: "alice"},
		{SessID: 3, Datname: "appdb", Usename: "alice"},
	}, false)
	require.NoError(t, err)

	ctx := context.Background()
	resp, err := srv.TerminateSessions(ctx, &pbm.TerminateSessionsRequest{Database: "appdb"})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Len(t, resp.TerminateResponse, 2)

	var hasForbidden, hasCancelError bool
	for _, r := range resp.TerminateResponse {
		assert.Equal(t, pbm.TerminateResponseStatusCode_TERMINATE_RESPONSE_STATUS_CODE_ERROR, r.StatusCode)
		if strings.Contains(r.StatusText, "forbidden") {
			hasForbidden = true
		}
		if strings.Contains(r.StatusText, "fail to terminate session") {
			hasCancelError = true
		}
	}
	assert.True(t, hasForbidden, "expected a forbidden response for SessID 0")
	assert.True(t, hasCancelError, "expected a cancel-error response for the regular session")
}

func TestMoveQueryToResourceGroup_NilQueryKey(t *testing.T) {
	srv := actionsServerNoDB(t)
	ctx := context.Background()

	_, err := srv.MoveQueryToResourceGroup(ctx, &pbm.MoveQueryToResourceGroupRequest{
		ResourceGroupName: "rg1",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot move empty query")
}

func TestMoveQueryToResourceGroup_EmptyResourceGroupName(t *testing.T) {
	srv := actionsServerNoDB(t)
	ctx := context.Background()

	_, err := srv.MoveQueryToResourceGroup(ctx, &pbm.MoveQueryToResourceGroupRequest{
		QueryKey:          &pbc.QueryKey{Ssid: 1, Ccnt: 1},
		ResourceGroupName: "",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot move query to empty resource group")
}

func TestMoveQueryToResourceGroup_DatabaseNotInitialized(t *testing.T) {
	srv := actionsServerNoDB(t)
	ctx := context.Background()

	_, err := srv.MoveQueryToResourceGroup(ctx, &pbm.MoveQueryToResourceGroupRequest{
		QueryKey:          &pbc.QueryKey{Ssid: 42, Ccnt: 1},
		ResourceGroupName: "default",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "fail to move query to resource group")
}

func TestTerminateQuery_NilQueryKey(t *testing.T) {
	srv := actionsServerNoDB(t)
	ctx := context.Background()

	_, err := srv.TerminateQuery(ctx, &pbm.TerminateQueryRequest{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot cancel empty query")
}

func TestTerminateQuery_CancelFails_ReturnsErrorStatus(t *testing.T) {
	srv := actionsServerNoDB(t)
	ctx := context.Background()

	resp, err := srv.TerminateQuery(ctx, &pbm.TerminateQueryRequest{
		QueryKey: &pbc.QueryKey{Ssid: 7, Ccnt: 1},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, pbm.TerminateResponseStatusCode_TERMINATE_RESPONSE_STATUS_CODE_ERROR, resp.StatusCode)
	assert.Equal(t, "fail to cancel query", resp.StatusText)
}

func TestTerminateSession_NilSessionKey(t *testing.T) {
	srv := actionsServerNoDB(t)
	ctx := context.Background()

	_, err := srv.TerminateSession(ctx, &pbm.TerminateSessionRequest{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot cancel empty session")
}

func TestTerminateSession_TerminateFails_ReturnsErrorStatus(t *testing.T) {
	srv := actionsServerNoDB(t)
	ctx := context.Background()

	resp, err := srv.TerminateSession(ctx, &pbm.TerminateSessionRequest{
		SessionKey: &pbc.SessionKey{SessId: 11},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, pbm.TerminateResponseStatusCode_TERMINATE_RESPONSE_STATUS_CODE_ERROR, resp.StatusCode)
	assert.Equal(t, "fail to terminate session", resp.StatusText)
}

func TestTerminateSessionForbiddenReason(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		session *pbc.SessionState
		want    string
	}{
		{
			name:    "nil session",
			session: nil,
			want:    "session is nil",
		},
		{
			name: "nil session key",
			session: &pbc.SessionState{
				SessionKey:  nil,
				SessionInfo: &pbc.SessionInfo{User: "alice"},
			},
			want: "session has no session key",
		},
		{
			name: "zero sess_id",
			session: &pbc.SessionState{
				SessionKey:  &pbc.SessionKey{SessId: 0},
				SessionInfo: &pbc.SessionInfo{User: "alice"},
			},
			want: "terminating session with sess_id 0 is forbidden",
		},
		{
			name: "gpadmin user",
			session: &pbc.SessionState{
				SessionKey:  &pbc.SessionKey{SessId: 1},
				SessionInfo: &pbc.SessionInfo{User: "gpadmin"},
			},
			want: "terminating gpadmin sessions is forbidden",
		},
		{
			name: "gpadmin case insensitive",
			session: &pbc.SessionState{
				SessionKey:  &pbc.SessionKey{SessId: 2},
				SessionInfo: &pbc.SessionInfo{User: "GpAdmin"},
			},
			want: "terminating gpadmin sessions is forbidden",
		},
		{
			name: "allowed",
			session: &pbc.SessionState{
				SessionKey:  &pbc.SessionKey{SessId: 3},
				SessionInfo: &pbc.SessionInfo{User: "alice"},
			},
			want: "",
		},
		{
			name: "no SessionInfo still allowed on user check",
			session: &pbc.SessionState{
				SessionKey:  &pbc.SessionKey{SessId: 4},
				SessionInfo: nil,
			},
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := terminateSessionForbiddenReason(tt.session)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestTerminateSession_ZeroSessID_ReturnsError(t *testing.T) {
	srv := actionsServerNoDB(t)
	ctx := context.Background()

	_, err := srv.TerminateSession(ctx, &pbm.TerminateSessionRequest{
		SessionKey: &pbc.SessionKey{SessId: 0},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "sess_id 0")
}

func TestTerminateSession_Gpadmin_ReturnsForbiddenStatus(t *testing.T) {
	srv := newTestActionsServer(t)
	ctx := context.Background()

	const sessID int32 = 77
	qKey := &pbc.QueryKey{Ssid: sessID, Tmid: 1, Ccnt: 1}
	srv.BackgroundStorage.SessionStorage.RegisterNewSessionQuery(nil, false, qKey, nil, 0)
	require.NoError(t, srv.BackgroundStorage.SessionStorage.UpdateSessionQuery(
		qKey,
		&pbc.QueryInfo{UserName: "gpadmin", DatabaseName: "db"},
		0,
		nil,
		false,
	))

	resp, err := srv.TerminateSession(ctx, &pbm.TerminateSessionRequest{
		SessionKey: &pbc.SessionKey{SessId: int64(sessID)},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, pbm.TerminateResponseStatusCode_TERMINATE_RESPONSE_STATUS_CODE_ERROR, resp.StatusCode)
	assert.Equal(t, "terminating gpadmin session is forbidden", resp.StatusText)
}
