//go:generate mockgen -source=deps.go -package=stat_activity_test -mock_names db=MockDB,log=MockLog,metricsProvider=MockMetricsProvider,metricsTimeVec=MockMetricsTimeVec,metricsTimer=MockMetricsTimer -destination mocks_test.go

package stat_activity

import (
	"context"
)

type log interface {
	Infof(format string, args ...any)
	Warnf(format string, args ...any)
}

type db interface {
	ExecQuery(ctx context.Context, query string, dest any) error
}
