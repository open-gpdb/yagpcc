package master

import (
	"context"

	"github.com/open-gpdb/yagpcc/internal/gp"
	"github.com/open-gpdb/yagpcc/internal/gp/stat_activity"
)

type statActivityLister interface {
	Start(ctx context.Context) error
	Stop()
	List(ctx context.Context) ([]*gp.GpStatActivity, error)
	ListAllSessions(ctx context.Context) ([]stat_activity.SessionPid, error)
}

type masterSentinel interface {
	RunUntilIsMaster(ctx context.Context) error
}
