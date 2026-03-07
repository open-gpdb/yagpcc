//go:generate mockgen -source=deps.go -package=uds_test -mock_names logger=MockLogger,connection=MockConnection,setQIServer=MockSetQIServer -destination mocks_test.go
package uds

import (
	"context"
	"time"

	pb "github.com/open-gpdb/yagpcc/api/proto/agent_segment"
)

type connection interface {
	SetReadDeadline(t time.Time) error
	Read(b []byte) (n int, err error)
	Close() error
}

type setQIServer interface {
	SetMetricQuery(ctx context.Context, in *pb.SetQueryReq) (*pb.MetricResponse, error)
}
