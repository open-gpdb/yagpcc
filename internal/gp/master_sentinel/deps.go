//go:generate mockgen -source=deps.go -package=master_sentinel_test -mock_names log=MockLog,db=MockDB -destination mocks_test.go
package master_sentinel

import "context"

type log interface {
	Warnf(format string, args ...any)
}

type db interface {
	ExecQueryNoRetry(ctx context.Context, query string, dest any) error
}
