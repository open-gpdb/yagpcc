package stat_activity

import "time"

type Session struct {
	DatID            int
	Datname          string
	Pid              int
	SessID           int
	TmID             int
	UsesysID         int
	Usename          string
	ApplicationName  *string
	ClientAddr       *string
	ClientHostname   *string
	ClientPort       *int
	BackendStart     *time.Time
	XactStart        *time.Time
	QueryStart       *time.Time
	StateChange      *time.Time
	Waiting          *bool
	State            *string
	BackendXid       *string
	BackendXmin      *string
	Query            *string
	WaitingReason    *string
	Rsgid            *int
	Rsgname          *string
	Rsgqueueduration *string
	WaitEvent        *string
	WaitEventType    *string
}

type SessionLock struct {
	BlockSessID     int
	BlockedBySessID int
	WaitMode        string
	LockedItem      string
	LockedMode      string
}
