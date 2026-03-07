package master

import (
	"context"

	"github.com/open-gpdb/yagpcc/internal/gp"
)

type statActivityLister interface {
	Start(ctx context.Context) error
	Stop()
	List(ctx context.Context) ([]*gp.GpStatActivity, error)
}

type masterSentinel interface {
	RunUntilIsMaster(ctx context.Context) error
}
