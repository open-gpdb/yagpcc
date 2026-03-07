//go:generate mockgen -source=deps.go -package=grpc_test -mock_names statActivityLister=MockStatActivityLister -destination mocks_test.go

package grpc

import (
	"context"

	"github.com/open-gpdb/yagpcc/internal/gp"
)

type statActivityLister interface {
	Start(ctx context.Context) error
	Stop()
	List(ctx context.Context) ([]*gp.GpStatActivity, error)
}
