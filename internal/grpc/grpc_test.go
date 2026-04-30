//go:generate mockgen -source=grpc_test.go -package=grpc_test -mock_names statActivityLister=MockStatActivityLister -destination mocks_test.go

package grpc_test

import (
	"context"

	"github.com/open-gpdb/yagpcc/internal/gp"
	"github.com/open-gpdb/yagpcc/internal/gp/stat_activity"
)

type statActivityLister interface { //nolint:unused // used by go:generate mockgen
	Start(ctx context.Context) error
	Stop()
	List(ctx context.Context) ([]*gp.GpStatActivity, error)
	ListAllSessions(context.Context) ([]stat_activity.SessionPid, error)
}
