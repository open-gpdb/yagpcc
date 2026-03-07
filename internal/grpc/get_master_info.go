package grpc

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"golang.org/x/exp/constraints"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	pbm "github.com/open-gpdb/yagpcc/api/proto/agent_master"
	pbc "github.com/open-gpdb/yagpcc/api/proto/common"
	"github.com/open-gpdb/yagpcc/internal/gp"
	"github.com/open-gpdb/yagpcc/internal/master"
	"github.com/open-gpdb/yagpcc/internal/metrics"
	"github.com/open-gpdb/yagpcc/internal/storage"
	"go.uber.org/zap"
)

type GetMasterInfoServer struct {
	pbm.UnimplementedGetGPInfoServer
	clusterID          string
	logger             *zap.SugaredLogger
	statActivityLister statActivityLister
	maxMessageSize     int
	backgroundStorage  *master.BackgroundStorage
}

func NewGetMasterInfoServer(clusterID string, logger *zap.SugaredLogger, statActivityLister statActivityLister, maxMessageSize int, backgroundStorage *master.BackgroundStorage) *GetMasterInfoServer {
	return &GetMasterInfoServer{
		clusterID:          clusterID,
		logger:             logger,
		statActivityLister: statActivityLister,
		maxMessageSize:     maxMessageSize,
		backgroundStorage:  backgroundStorage,
	}
}

type MultipleSorter struct {
	SessionState *[]*pbc.SessionState
	Fields       []*pbm.SessionFieldWrapper
}

func (ms *MultipleSorter) Len() int {
	return len(*ms.SessionState)
}

func (ms *MultipleSorter) Swap(i, j int) {
	(*ms.SessionState)[i], (*ms.SessionState)[j] = (*ms.SessionState)[j], (*ms.SessionState)[i]
}

func less[T constraints.Ordered](order pbm.SortOrder, a T, b T) bool {
	if order == pbm.SortOrder_SORT_ASC && a < b {
		return true
	}
	if order == pbm.SortOrder_SORT_DESC && b < a {
		return true
	}
	return false
}

func lessbool(order pbm.SortOrder, a bool, b bool) bool {
	if order == pbm.SortOrder_SORT_ASC && !a && b {
		return true
	}
	if order == pbm.SortOrder_SORT_DESC && !b && a {
		return true
	}
	return false
}

func lesstimestamp(order pbm.SortOrder, a *timestamppb.Timestamp, b *timestamppb.Timestamp) bool {
	if order == pbm.SortOrder_SORT_ASC && a.AsTime().Before(b.AsTime()) {
		return true
	}
	if order == pbm.SortOrder_SORT_DESC && b.AsTime().Before(a.AsTime()) {
		return true
	}
	return false
}

func (ms *MultipleSorter) Less(i, j int) bool {
	p, q := (*ms.SessionState)[i], (*ms.SessionState)[j]
	if p == nil {
		panic(fmt.Sprintf("session %v is nil", i))
	}
	if q == nil {
		panic(fmt.Sprintf("session %v is nil", j))
	}

	// Try all but the last comparison.
	var k int
	for k = 0; k < len(ms.Fields); k++ {
		switch {
		case ms.Fields[k].FieldName == pbm.SessionField_SESSION_FIELD_KEY:
			if less(ms.Fields[k].Order, p.SessionKey.SessId, q.SessionKey.SessId) {
				return true
			}
			if less(ms.Fields[k].Order, q.SessionKey.SessId, p.SessionKey.SessId) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_SESSION_FIELD_HOST:
			if less(ms.Fields[k].Order, p.Hostname, q.Hostname) {
				return true
			}
			if less(ms.Fields[k].Order, q.Hostname, p.Hostname) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_SESSION_FIELD_PID:

			left, right := int64(0.0), int64(0.0)
			if p.SessionInfo != nil {
				left = p.SessionInfo.Pid
			}
			if q.SessionInfo != nil {
				right = q.SessionInfo.Pid
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_SESSION_FIELD_DATABASE:

			left, right := "", ""
			if p.SessionInfo != nil {
				left = p.SessionInfo.Database
			}
			if q.SessionInfo != nil {
				right = q.SessionInfo.Database
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_SESSION_FIELD_USER:

			left, right := "", ""
			if p.SessionInfo != nil {
				left = p.SessionInfo.User
			}
			if q.SessionInfo != nil {
				right = q.SessionInfo.User
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_SESSION_FIELD_APPLICATION_NAME:

			left, right := "", ""
			if p.SessionInfo != nil {
				left = p.SessionInfo.ApplicationName
			}
			if q.SessionInfo != nil {
				right = q.SessionInfo.ApplicationName
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_SESSION_FIELD_CLIENT_ADDR:

			left, right := "", ""
			if p.SessionInfo != nil {
				left = p.SessionInfo.ClientAddr
			}
			if q.SessionInfo != nil {
				right = q.SessionInfo.ClientAddr
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_SESSION_FIELD_CLIENT_HOSTNAME:

			left, right := "", ""
			if p.SessionInfo != nil {
				left = p.SessionInfo.ClientHostname
			}
			if q.SessionInfo != nil {
				right = q.SessionInfo.ClientHostname
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_SESSION_FIELD_CLIENT_PORT:

			left, right := int64(0.0), int64(0.0)
			if p.SessionInfo != nil {
				left = p.SessionInfo.ClientPort
			}
			if q.SessionInfo != nil {
				right = q.SessionInfo.ClientPort
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_SESSION_FIELD_BACKEND_START:

			left, right := &timestamppb.Timestamp{}, &timestamppb.Timestamp{}
			if p.SessionInfo != nil {
				left = p.SessionInfo.BackendStart
			}
			if q.SessionInfo != nil {
				right = q.SessionInfo.BackendStart
			}
			if lesstimestamp(ms.Fields[k].Order, left, right) {
				return true
			}
			if lesstimestamp(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_SESSION_FIELD_XACT_START:

			left, right := &timestamppb.Timestamp{}, &timestamppb.Timestamp{}
			if p.SessionInfo != nil {
				left = p.SessionInfo.XactStart
			}
			if q.SessionInfo != nil {
				right = q.SessionInfo.XactStart
			}
			if lesstimestamp(ms.Fields[k].Order, left, right) {
				return true
			}
			if lesstimestamp(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_SESSION_FIELD_QUERY_START:

			left, right := &timestamppb.Timestamp{}, &timestamppb.Timestamp{}
			if p.SessionInfo != nil {
				left = p.SessionInfo.QueryStart
			}
			if q.SessionInfo != nil {
				right = q.SessionInfo.QueryStart
			}
			if lesstimestamp(ms.Fields[k].Order, left, right) {
				return true
			}
			if lesstimestamp(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_SESSION_FIELD_STATE_CHANGE:

			left, right := &timestamppb.Timestamp{}, &timestamppb.Timestamp{}
			if p.SessionInfo != nil {
				left = p.SessionInfo.StateChange
			}
			if q.SessionInfo != nil {
				right = q.SessionInfo.StateChange
			}
			if lesstimestamp(ms.Fields[k].Order, left, right) {
				return true
			}
			if lesstimestamp(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_SESSION_FIELD_WAITING_REASON:

			left, right := "", ""
			if p.SessionInfo != nil {
				left = p.SessionInfo.WaitingReason
			}
			if q.SessionInfo != nil {
				right = q.SessionInfo.WaitingReason
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_SESSION_FIELD_WAITING:

			left, right := true, true
			if p.SessionInfo != nil {
				left = p.SessionInfo.Waiting
			}
			if q.SessionInfo != nil {
				right = q.SessionInfo.Waiting
			}
			if lessbool(ms.Fields[k].Order, left, right) {
				return true
			}
			if lessbool(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_SESSION_FIELD_STATE:

			left, right := "", ""
			if p.SessionInfo != nil {
				left = p.SessionInfo.State
			}
			if q.SessionInfo != nil {
				right = q.SessionInfo.State
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_SESSION_FIELD_BACKEND_XID:

			left, right := "", ""
			if p.SessionInfo != nil {
				left = p.SessionInfo.BackendXid
			}
			if q.SessionInfo != nil {
				right = q.SessionInfo.BackendXid
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_SESSION_FIELD_BACKEND_XMIN:

			left, right := "", ""
			if p.SessionInfo != nil {
				left = p.SessionInfo.BackendXmin
			}
			if q.SessionInfo != nil {
				right = q.SessionInfo.BackendXmin
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_SESSION_FIELD_RSGID:

			left, right := int64(0.0), int64(0.0)
			if p.SessionInfo != nil {
				left = p.SessionInfo.Rsgid
			}
			if q.SessionInfo != nil {
				right = q.SessionInfo.Rsgid
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_SESSION_FIELD_RSGNAME:

			left, right := "", ""
			if p.SessionInfo != nil {
				left = p.SessionInfo.Rsgname
			}
			if q.SessionInfo != nil {
				right = q.SessionInfo.Rsgname
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_SESSION_FIELD_RSGQUEUEDURATION:

			left, right := "", ""
			if p.SessionInfo != nil {
				left = p.SessionInfo.Rsgqueueduration
			}
			if q.SessionInfo != nil {
				right = q.SessionInfo.Rsgqueueduration
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_SESSION_FIELD_BLOCKED_BY:

			left, right := int64(0.0), int64(0.0)
			if p.SessionInfo != nil {
				left = p.SessionInfo.BlockedBySessId
			}
			if q.SessionInfo != nil {
				right = q.SessionInfo.BlockedBySessId
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_SESSION_FIELD_BLOCKED_REASON:

			left, right := "", ""
			if p.SessionInfo != nil {
				left = p.SessionInfo.LockedItem
			}
			if q.SessionInfo != nil {
				right = q.SessionInfo.LockedItem
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_SESSION_FIELD_RUNNING_QUERY:

			left := int64(p.RunningQuery.Ssid)*1000000 + int64(p.RunningQuery.Ccnt)
			right := int64(q.RunningQuery.Ssid)*1000000 + int64(q.RunningQuery.Ccnt)
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_SESSION_FIELD_RUNNING_QUERY_STATUS:

			left := int64(p.RunningQueryStatus)
			right := int64(q.RunningQueryStatus)
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_QUERY_GENERATOR:

			left, right := int64(0.0), int64(0.0)
			if p.RunningQueryInfo != nil {
				left = int64(p.RunningQueryInfo.Generator)
			}
			if q.RunningQueryInfo != nil {
				right = int64(q.RunningQueryInfo.Generator)
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_QUERY_QUERY_ID:

			left, right := uint64(0.0), uint64(0.0)
			if p.RunningQueryInfo != nil {
				left = p.RunningQueryInfo.QueryId
			}
			if q.RunningQueryInfo != nil {
				right = q.RunningQueryInfo.QueryId
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_QUERY_PLAN_ID:

			left, right := uint64(0.0), uint64(0.0)
			if p.RunningQueryInfo != nil {
				left = p.RunningQueryInfo.PlanId
			}
			if q.RunningQueryInfo != nil {
				right = q.RunningQueryInfo.PlanId
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_QUERY_QUERY_TEXT:

			left, right := "", ""
			if p.RunningQueryInfo != nil {
				left = p.RunningQueryInfo.QueryText
			}
			if q.RunningQueryInfo != nil {
				right = q.RunningQueryInfo.QueryText
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_QUERY_PLAN_TEXT:

			left, right := "", ""
			if p.RunningQueryInfo != nil {
				left = p.RunningQueryInfo.PlanText
			}
			if q.RunningQueryInfo != nil {
				right = q.RunningQueryInfo.PlanText
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_QUERY_USERNAME:

			left, right := "", ""
			if p.RunningQueryInfo != nil {
				left = p.RunningQueryInfo.UserName
			}
			if q.RunningQueryInfo != nil {
				right = q.RunningQueryInfo.UserName
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_QUERY_DATABASENAME:

			left, right := "", ""
			if p.RunningQueryInfo != nil {
				left = p.RunningQueryInfo.DatabaseName
			}
			if q.RunningQueryInfo != nil {
				right = q.RunningQueryInfo.DatabaseName
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_QUERY_RSGNAME:

			left, right := "", ""
			if p.RunningQueryInfo != nil {
				left = p.RunningQueryInfo.Rsgname
			}
			if q.RunningQueryInfo != nil {
				right = q.RunningQueryInfo.Rsgname
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_TOTAL_RUNNINGTIMESECONDS:

			left, right := 0.0, 0.0
			if p.TotalMetrics != nil && p.TotalMetrics.SystemStat != nil {
				left = p.TotalMetrics.SystemStat.RunningTimeSeconds
			}
			if q.TotalMetrics != nil && q.TotalMetrics.SystemStat != nil {
				right = q.TotalMetrics.SystemStat.RunningTimeSeconds
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_TOTAL_USERTIMESECONDS:

			left, right := 0.0, 0.0
			if p.TotalMetrics != nil && p.TotalMetrics.SystemStat != nil {
				left = p.TotalMetrics.SystemStat.UserTimeSeconds
			}
			if q.TotalMetrics != nil && q.TotalMetrics.SystemStat != nil {
				right = q.TotalMetrics.SystemStat.UserTimeSeconds
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_TOTAL_KERNELTIMESECONDS:

			left, right := 0.0, 0.0
			if p.TotalMetrics != nil && p.TotalMetrics.SystemStat != nil {
				left = p.TotalMetrics.SystemStat.KernelTimeSeconds
			}
			if q.TotalMetrics != nil && q.TotalMetrics.SystemStat != nil {
				right = q.TotalMetrics.SystemStat.KernelTimeSeconds
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_TOTAL_VSIZE:

			left, right := uint64(0.0), uint64(0.0)
			if p.TotalMetrics != nil && p.TotalMetrics.SystemStat != nil {
				left = p.TotalMetrics.SystemStat.Vsize
			}
			if q.TotalMetrics != nil && q.TotalMetrics.SystemStat != nil {
				right = q.TotalMetrics.SystemStat.Vsize
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_TOTAL_RSS:

			left, right := uint64(0.0), uint64(0.0)
			if p.TotalMetrics != nil && p.TotalMetrics.SystemStat != nil {
				left = p.TotalMetrics.SystemStat.Rss
			}
			if q.TotalMetrics != nil && q.TotalMetrics.SystemStat != nil {
				right = q.TotalMetrics.SystemStat.Rss
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_TOTAL_VMPEAKKB:

			left, right := uint64(0.0), uint64(0.0)
			if p.TotalMetrics != nil && p.TotalMetrics.SystemStat != nil {
				left = p.TotalMetrics.SystemStat.VmPeakKb
			}
			if q.TotalMetrics != nil && q.TotalMetrics.SystemStat != nil {
				right = q.TotalMetrics.SystemStat.VmPeakKb
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_TOTAL_RCHAR:

			left, right := uint64(0.0), uint64(0.0)
			if p.TotalMetrics != nil && p.TotalMetrics.SystemStat != nil {
				left = p.TotalMetrics.SystemStat.Rchar
			}
			if q.TotalMetrics != nil && q.TotalMetrics.SystemStat != nil {
				right = q.TotalMetrics.SystemStat.Rchar
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_TOTAL_WCHAR:

			left, right := uint64(0.0), uint64(0.0)
			if p.TotalMetrics != nil && p.TotalMetrics.SystemStat != nil {
				left = p.TotalMetrics.SystemStat.Wchar
			}
			if q.TotalMetrics != nil && q.TotalMetrics.SystemStat != nil {
				right = q.TotalMetrics.SystemStat.Wchar
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_TOTAL_SYSCR:

			left, right := uint64(0.0), uint64(0.0)
			if p.TotalMetrics != nil && p.TotalMetrics.SystemStat != nil {
				left = p.TotalMetrics.SystemStat.Syscr
			}
			if q.TotalMetrics != nil && q.TotalMetrics.SystemStat != nil {
				right = q.TotalMetrics.SystemStat.Syscr
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_TOTAL_SYSCW:

			left, right := uint64(0.0), uint64(0.0)
			if p.TotalMetrics != nil && p.TotalMetrics.SystemStat != nil {
				left = p.TotalMetrics.SystemStat.Syscw
			}
			if q.TotalMetrics != nil && q.TotalMetrics.SystemStat != nil {
				right = q.TotalMetrics.SystemStat.Syscw
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_TOTAL_READ_BYTES:

			left, right := uint64(0.0), uint64(0.0)
			if p.TotalMetrics != nil && p.TotalMetrics.SystemStat != nil {
				left = p.TotalMetrics.SystemStat.ReadBytes
			}
			if q.TotalMetrics != nil && q.TotalMetrics.SystemStat != nil {
				right = q.TotalMetrics.SystemStat.ReadBytes
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_TOTAL_WRITE_BYTES:

			left, right := uint64(0.0), uint64(0.0)
			if p.TotalMetrics != nil && p.TotalMetrics.SystemStat != nil {
				left = p.TotalMetrics.SystemStat.WriteBytes
			}
			if q.TotalMetrics != nil && q.TotalMetrics.SystemStat != nil {
				right = q.TotalMetrics.SystemStat.WriteBytes
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_TOTAL_CANCELLED_WRITE_BYTES:

			left, right := uint64(0.0), uint64(0.0)
			if p.TotalMetrics != nil && p.TotalMetrics.SystemStat != nil {
				left = p.TotalMetrics.SystemStat.CancelledWriteBytes
			}
			if q.TotalMetrics != nil && q.TotalMetrics.SystemStat != nil {
				right = q.TotalMetrics.SystemStat.CancelledWriteBytes
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_TOTAL_NTUPLES:

			left, right := uint64(0.0), uint64(0.0)
			if p.TotalMetrics != nil && p.TotalMetrics.Instrumentation != nil {
				left = p.TotalMetrics.Instrumentation.Ntuples
			}
			if q.TotalMetrics != nil && q.TotalMetrics.Instrumentation != nil {
				right = q.TotalMetrics.Instrumentation.Ntuples
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_TOTAL_NLOOPS:

			left, right := uint64(0.0), uint64(0.0)
			if p.TotalMetrics != nil && p.TotalMetrics.Instrumentation != nil {
				left = p.TotalMetrics.Instrumentation.Nloops
			}
			if q.TotalMetrics != nil && q.TotalMetrics.Instrumentation != nil {
				right = q.TotalMetrics.Instrumentation.Nloops
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_TOTAL_TUPLECOUNT:

			left, right := uint64(0.0), uint64(0.0)
			if p.TotalMetrics != nil && p.TotalMetrics.Instrumentation != nil {
				left = p.TotalMetrics.Instrumentation.Tuplecount
			}
			if q.TotalMetrics != nil && q.TotalMetrics.Instrumentation != nil {
				right = q.TotalMetrics.Instrumentation.Tuplecount
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_TOTAL_FIRSTTUPLE:

			left, right := 0.0, 0.0
			if p.TotalMetrics != nil && p.TotalMetrics.Instrumentation != nil {
				left = p.TotalMetrics.Instrumentation.Firsttuple
			}
			if q.TotalMetrics != nil && q.TotalMetrics.Instrumentation != nil {
				right = q.TotalMetrics.Instrumentation.Firsttuple
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_TOTAL_STARTUP:

			left, right := 0.0, 0.0
			if p.TotalMetrics != nil && p.TotalMetrics.Instrumentation != nil {
				left = p.TotalMetrics.Instrumentation.Startup
			}
			if q.TotalMetrics != nil && q.TotalMetrics.Instrumentation != nil {
				right = q.TotalMetrics.Instrumentation.Startup
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_TOTAL_TOTAL:

			left, right := 0.0, 0.0
			if p.TotalMetrics != nil && p.TotalMetrics.Instrumentation != nil {
				left = p.TotalMetrics.Instrumentation.Total
			}
			if q.TotalMetrics != nil && q.TotalMetrics.Instrumentation != nil {
				right = q.TotalMetrics.Instrumentation.Total
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_TOTAL_SHARED_BLKS_HIT:

			left, right := uint64(0.0), uint64(0.0)
			if p.TotalMetrics != nil && p.TotalMetrics.Instrumentation != nil {
				left = p.TotalMetrics.Instrumentation.SharedBlksHit
			}
			if q.TotalMetrics != nil && q.TotalMetrics.Instrumentation != nil {
				right = q.TotalMetrics.Instrumentation.SharedBlksHit
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_TOTAL_SHARED_BLKS_READ:

			left, right := uint64(0.0), uint64(0.0)
			if p.TotalMetrics != nil && p.TotalMetrics.Instrumentation != nil {
				left = p.TotalMetrics.Instrumentation.SharedBlksRead
			}
			if q.TotalMetrics != nil && q.TotalMetrics.Instrumentation != nil {
				right = q.TotalMetrics.Instrumentation.SharedBlksRead
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_TOTAL_SHARED_BLKS_DIRTIED:

			left, right := uint64(0.0), uint64(0.0)
			if p.TotalMetrics != nil && p.TotalMetrics.Instrumentation != nil {
				left = p.TotalMetrics.Instrumentation.SharedBlksDirtied
			}
			if q.TotalMetrics != nil && q.TotalMetrics.Instrumentation != nil {
				right = q.TotalMetrics.Instrumentation.SharedBlksDirtied
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_TOTAL_SHARED_BLKS_WRITTEN:

			left, right := uint64(0.0), uint64(0.0)
			if p.TotalMetrics != nil && p.TotalMetrics.Instrumentation != nil {
				left = p.TotalMetrics.Instrumentation.SharedBlksWritten
			}
			if q.TotalMetrics != nil && q.TotalMetrics.Instrumentation != nil {
				right = q.TotalMetrics.Instrumentation.SharedBlksWritten
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_TOTAL_LOCAL_BLKS_HIT:

			left, right := uint64(0.0), uint64(0.0)
			if p.TotalMetrics != nil && p.TotalMetrics.Instrumentation != nil {
				left = p.TotalMetrics.Instrumentation.LocalBlksHit
			}
			if q.TotalMetrics != nil && q.TotalMetrics.Instrumentation != nil {
				right = q.TotalMetrics.Instrumentation.LocalBlksHit
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_TOTAL_LOCAL_BLKS_READ:

			left, right := uint64(0.0), uint64(0.0)
			if p.TotalMetrics != nil && p.TotalMetrics.Instrumentation != nil {
				left = p.TotalMetrics.Instrumentation.LocalBlksRead
			}
			if q.TotalMetrics != nil && q.TotalMetrics.Instrumentation != nil {
				right = q.TotalMetrics.Instrumentation.LocalBlksRead
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_TOTAL_LOCAL_BLKS_DIRTIED:

			left, right := uint64(0.0), uint64(0.0)
			if p.TotalMetrics != nil && p.TotalMetrics.Instrumentation != nil {
				left = p.TotalMetrics.Instrumentation.LocalBlksDirtied
			}
			if q.TotalMetrics != nil && q.TotalMetrics.Instrumentation != nil {
				right = q.TotalMetrics.Instrumentation.LocalBlksDirtied
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_TOTAL_LOCAL_BLKS_WRITTEN:

			left, right := uint64(0.0), uint64(0.0)
			if p.TotalMetrics != nil && p.TotalMetrics.Instrumentation != nil {
				left = p.TotalMetrics.Instrumentation.LocalBlksWritten
			}
			if q.TotalMetrics != nil && q.TotalMetrics.Instrumentation != nil {
				right = q.TotalMetrics.Instrumentation.LocalBlksWritten
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_TOTAL_TEMP_BLKS_READ:

			left, right := uint64(0.0), uint64(0.0)
			if p.TotalMetrics != nil && p.TotalMetrics.Instrumentation != nil {
				left = p.TotalMetrics.Instrumentation.TempBlksRead
			}
			if q.TotalMetrics != nil && q.TotalMetrics.Instrumentation != nil {
				right = q.TotalMetrics.Instrumentation.TempBlksRead
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_TOTAL_TEMP_BLKS_WRITTEN:

			left, right := uint64(0.0), uint64(0.0)
			if p.TotalMetrics != nil && p.TotalMetrics.Instrumentation != nil {
				left = p.TotalMetrics.Instrumentation.TempBlksWritten
			}
			if q.TotalMetrics != nil && q.TotalMetrics.Instrumentation != nil {
				right = q.TotalMetrics.Instrumentation.TempBlksWritten
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_TOTAL_BLK_READ_TIME:

			left, right := 0.0, 0.0
			if p.TotalMetrics != nil && p.TotalMetrics.Instrumentation != nil {
				left = p.TotalMetrics.Instrumentation.BlkReadTime
			}
			if q.TotalMetrics != nil && q.TotalMetrics.Instrumentation != nil {
				right = q.TotalMetrics.Instrumentation.BlkReadTime
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_TOTAL_BLK_WRITE_TIME:

			left, right := 0.0, 0.0
			if p.TotalMetrics != nil && p.TotalMetrics.Instrumentation != nil {
				left = p.TotalMetrics.Instrumentation.BlkWriteTime
			}
			if q.TotalMetrics != nil && q.TotalMetrics.Instrumentation != nil {
				right = q.TotalMetrics.Instrumentation.BlkWriteTime
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_TOTAL_SPILL_FILECOUNT:

			left, right := int64(0.0), int64(0.0)
			if p.TotalMetrics != nil && p.TotalMetrics.Spill != nil {
				left = int64(p.TotalMetrics.Spill.FileCount)
			}
			if q.TotalMetrics != nil && q.TotalMetrics.Spill != nil {
				right = int64(q.TotalMetrics.Spill.FileCount)
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_TOTAL_SPILL_TOTALBYTES:

			left, right := int64(0.0), int64(0.0)
			if p.TotalMetrics != nil && p.TotalMetrics.Spill != nil {
				left = p.TotalMetrics.Spill.TotalBytes
			}
			if q.TotalMetrics != nil && q.TotalMetrics.Spill != nil {
				right = q.TotalMetrics.Spill.TotalBytes
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_TOTAL_NET_SENT_TOTAL_BYTES:

			left, right := uint32(0.0), uint32(0.0)
			if p.TotalMetrics != nil && p.TotalMetrics.Instrumentation != nil && p.TotalMetrics.Instrumentation.Sent != nil {
				left = p.TotalMetrics.Instrumentation.Sent.TotalBytes
			}
			if q.TotalMetrics != nil && q.TotalMetrics.Instrumentation != nil && q.TotalMetrics.Instrumentation.Sent != nil {
				right = q.TotalMetrics.Instrumentation.Sent.TotalBytes
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_TOTAL_NET_SENT_TUPLE_BYTES:

			left, right := uint32(0.0), uint32(0.0)
			if p.TotalMetrics != nil && p.TotalMetrics.Instrumentation != nil && p.TotalMetrics.Instrumentation.Sent != nil {
				left = p.TotalMetrics.Instrumentation.Sent.TupleBytes
			}
			if q.TotalMetrics != nil && q.TotalMetrics.Instrumentation != nil && q.TotalMetrics.Instrumentation.Sent != nil {
				right = q.TotalMetrics.Instrumentation.Sent.TupleBytes
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_TOTAL_NET_SENT_CHUNKS:

			left, right := uint32(0.0), uint32(0.0)
			if p.TotalMetrics != nil && p.TotalMetrics.Instrumentation != nil && p.TotalMetrics.Instrumentation.Sent != nil {
				left = p.TotalMetrics.Instrumentation.Sent.Chunks
			}
			if q.TotalMetrics != nil && q.TotalMetrics.Instrumentation != nil && q.TotalMetrics.Instrumentation.Sent != nil {
				right = q.TotalMetrics.Instrumentation.Sent.Chunks
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_TOTAL_NET_RECV_TOTAL_BYTES:

			left, right := uint32(0.0), uint32(0.0)
			if p.TotalMetrics != nil && p.TotalMetrics.Instrumentation != nil && p.TotalMetrics.Instrumentation.Received != nil {
				left = p.TotalMetrics.Instrumentation.Received.TotalBytes
			}
			if q.TotalMetrics != nil && q.TotalMetrics.Instrumentation != nil && q.TotalMetrics.Instrumentation.Received != nil {
				right = q.TotalMetrics.Instrumentation.Received.TotalBytes
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_TOTAL_NET_RECV_TUPLE_BYTES:

			left, right := uint32(0.0), uint32(0.0)
			if p.TotalMetrics != nil && p.TotalMetrics.Instrumentation != nil && p.TotalMetrics.Instrumentation.Received != nil {
				left = p.TotalMetrics.Instrumentation.Received.TupleBytes
			}
			if q.TotalMetrics != nil && q.TotalMetrics.Instrumentation != nil && q.TotalMetrics.Instrumentation.Received != nil {
				right = q.TotalMetrics.Instrumentation.Received.TupleBytes
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_TOTAL_NET_RECV_CHUNKS:

			left, right := uint32(0.0), uint32(0.0)
			if p.TotalMetrics != nil && p.TotalMetrics.Instrumentation != nil && p.TotalMetrics.Instrumentation.Received != nil {
				left = p.TotalMetrics.Instrumentation.Received.Chunks
			}
			if q.TotalMetrics != nil && q.TotalMetrics.Instrumentation != nil && q.TotalMetrics.Instrumentation.Received != nil {
				right = q.TotalMetrics.Instrumentation.Received.Chunks
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_LAST_RUNNINGTIMESECONDS:

			left, right := 0.0, 0.0
			if p.LastMetrics != nil && p.LastMetrics.SystemStat != nil {
				left = p.LastMetrics.SystemStat.RunningTimeSeconds
			}
			if q.LastMetrics != nil && q.LastMetrics.SystemStat != nil {
				right = q.LastMetrics.SystemStat.RunningTimeSeconds
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_LAST_USERTIMESECONDS:

			left, right := 0.0, 0.0
			if p.LastMetrics != nil && p.LastMetrics.SystemStat != nil {
				left = p.LastMetrics.SystemStat.UserTimeSeconds
			}
			if q.LastMetrics != nil && q.LastMetrics.SystemStat != nil {
				right = q.LastMetrics.SystemStat.UserTimeSeconds
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_LAST_KERNELTIMESECONDS:

			left, right := 0.0, 0.0
			if p.LastMetrics != nil && p.LastMetrics.SystemStat != nil {
				left = p.LastMetrics.SystemStat.KernelTimeSeconds
			}
			if q.LastMetrics != nil && q.LastMetrics.SystemStat != nil {
				right = q.LastMetrics.SystemStat.KernelTimeSeconds
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_LAST_VSIZE:

			left, right := uint64(0.0), uint64(0.0)
			if p.LastMetrics != nil && p.LastMetrics.SystemStat != nil {
				left = p.LastMetrics.SystemStat.Vsize
			}
			if q.LastMetrics != nil && q.LastMetrics.SystemStat != nil {
				right = q.LastMetrics.SystemStat.Vsize
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_LAST_RSS:

			left, right := uint64(0.0), uint64(0.0)
			if p.LastMetrics != nil && p.LastMetrics.SystemStat != nil {
				left = p.LastMetrics.SystemStat.Rss
			}
			if q.LastMetrics != nil && q.LastMetrics.SystemStat != nil {
				right = q.LastMetrics.SystemStat.Rss
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_LAST_VMPEAKKB:

			left, right := uint64(0.0), uint64(0.0)
			if p.LastMetrics != nil && p.LastMetrics.SystemStat != nil {
				left = p.LastMetrics.SystemStat.VmPeakKb
			}
			if q.LastMetrics != nil && q.LastMetrics.SystemStat != nil {
				right = q.LastMetrics.SystemStat.VmPeakKb
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_LAST_RCHAR:

			left, right := uint64(0.0), uint64(0.0)
			if p.LastMetrics != nil && p.LastMetrics.SystemStat != nil {
				left = p.LastMetrics.SystemStat.Rchar
			}
			if q.LastMetrics != nil && q.LastMetrics.SystemStat != nil {
				right = q.LastMetrics.SystemStat.Rchar
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_LAST_WCHAR:

			left, right := uint64(0.0), uint64(0.0)
			if p.LastMetrics != nil && p.LastMetrics.SystemStat != nil {
				left = p.LastMetrics.SystemStat.Wchar
			}
			if q.LastMetrics != nil && q.LastMetrics.SystemStat != nil {
				right = q.LastMetrics.SystemStat.Wchar
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_LAST_SYSCR:

			left, right := uint64(0.0), uint64(0.0)
			if p.LastMetrics != nil && p.LastMetrics.SystemStat != nil {
				left = p.LastMetrics.SystemStat.Syscr
			}
			if q.LastMetrics != nil && q.LastMetrics.SystemStat != nil {
				right = q.LastMetrics.SystemStat.Syscr
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_LAST_SYSCW:

			left, right := uint64(0.0), uint64(0.0)
			if p.LastMetrics != nil && p.LastMetrics.SystemStat != nil {
				left = p.LastMetrics.SystemStat.Syscw
			}
			if q.LastMetrics != nil && q.LastMetrics.SystemStat != nil {
				right = q.LastMetrics.SystemStat.Syscw
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_LAST_READ_BYTES:

			left, right := uint64(0.0), uint64(0.0)
			if p.LastMetrics != nil && p.LastMetrics.SystemStat != nil {
				left = p.LastMetrics.SystemStat.ReadBytes
			}
			if q.LastMetrics != nil && q.LastMetrics.SystemStat != nil {
				right = q.LastMetrics.SystemStat.ReadBytes
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_LAST_WRITE_BYTES:

			left, right := uint64(0.0), uint64(0.0)
			if p.LastMetrics != nil && p.LastMetrics.SystemStat != nil {
				left = p.LastMetrics.SystemStat.WriteBytes
			}
			if q.LastMetrics != nil && q.LastMetrics.SystemStat != nil {
				right = q.LastMetrics.SystemStat.WriteBytes
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_LAST_CANCELLED_WRITE_BYTES:

			left, right := uint64(0.0), uint64(0.0)
			if p.LastMetrics != nil && p.LastMetrics.SystemStat != nil {
				left = p.LastMetrics.SystemStat.CancelledWriteBytes
			}
			if q.LastMetrics != nil && q.LastMetrics.SystemStat != nil {
				right = q.LastMetrics.SystemStat.CancelledWriteBytes
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_LAST_NTUPLES:

			left, right := uint64(0.0), uint64(0.0)
			if p.LastMetrics != nil && p.LastMetrics.Instrumentation != nil {
				left = p.LastMetrics.Instrumentation.Ntuples
			}
			if q.LastMetrics != nil && q.LastMetrics.Instrumentation != nil {
				right = q.LastMetrics.Instrumentation.Ntuples
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_LAST_NLOOPS:

			left, right := uint64(0.0), uint64(0.0)
			if p.LastMetrics != nil && p.LastMetrics.Instrumentation != nil {
				left = p.LastMetrics.Instrumentation.Nloops
			}
			if q.LastMetrics != nil && q.LastMetrics.Instrumentation != nil {
				right = q.LastMetrics.Instrumentation.Nloops
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_LAST_TUPLECOUNT:

			left, right := uint64(0.0), uint64(0.0)
			if p.LastMetrics != nil && p.LastMetrics.Instrumentation != nil {
				left = p.LastMetrics.Instrumentation.Tuplecount
			}
			if q.LastMetrics != nil && q.LastMetrics.Instrumentation != nil {
				right = q.LastMetrics.Instrumentation.Tuplecount
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_LAST_FIRSTTUPLE:

			left, right := 0.0, 0.0
			if p.LastMetrics != nil && p.LastMetrics.Instrumentation != nil {
				left = p.LastMetrics.Instrumentation.Firsttuple
			}
			if q.LastMetrics != nil && q.LastMetrics.Instrumentation != nil {
				right = q.LastMetrics.Instrumentation.Firsttuple
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_LAST_STARTUP:

			left, right := 0.0, 0.0
			if p.LastMetrics != nil && p.LastMetrics.Instrumentation != nil {
				left = p.LastMetrics.Instrumentation.Startup
			}
			if q.LastMetrics != nil && q.LastMetrics.Instrumentation != nil {
				right = q.LastMetrics.Instrumentation.Startup
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_LAST_TOTAL:

			left, right := 0.0, 0.0
			if p.LastMetrics != nil && p.LastMetrics.Instrumentation != nil {
				left = p.LastMetrics.Instrumentation.Total
			}
			if q.LastMetrics != nil && q.LastMetrics.Instrumentation != nil {
				right = q.LastMetrics.Instrumentation.Total
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_LAST_SHARED_BLKS_HIT:

			left, right := uint64(0.0), uint64(0.0)
			if p.LastMetrics != nil && p.LastMetrics.Instrumentation != nil {
				left = p.LastMetrics.Instrumentation.SharedBlksHit
			}
			if q.LastMetrics != nil && q.LastMetrics.Instrumentation != nil {
				right = q.LastMetrics.Instrumentation.SharedBlksHit
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_LAST_SHARED_BLKS_READ:

			left, right := uint64(0.0), uint64(0.0)
			if p.LastMetrics != nil && p.LastMetrics.Instrumentation != nil {
				left = p.LastMetrics.Instrumentation.SharedBlksRead
			}
			if q.LastMetrics != nil && q.LastMetrics.Instrumentation != nil {
				right = q.LastMetrics.Instrumentation.SharedBlksRead
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_LAST_SHARED_BLKS_DIRTIED:

			left, right := uint64(0.0), uint64(0.0)
			if p.LastMetrics != nil && p.LastMetrics.Instrumentation != nil {
				left = p.LastMetrics.Instrumentation.SharedBlksDirtied
			}
			if q.LastMetrics != nil && q.LastMetrics.Instrumentation != nil {
				right = q.LastMetrics.Instrumentation.SharedBlksDirtied
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_LAST_SHARED_BLKS_WRITTEN:

			left, right := uint64(0.0), uint64(0.0)
			if p.LastMetrics != nil && p.LastMetrics.Instrumentation != nil {
				left = p.LastMetrics.Instrumentation.SharedBlksWritten
			}
			if q.LastMetrics != nil && q.LastMetrics.Instrumentation != nil {
				right = q.LastMetrics.Instrumentation.SharedBlksWritten
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_LAST_LOCAL_BLKS_HIT:

			left, right := uint64(0.0), uint64(0.0)
			if p.LastMetrics != nil && p.LastMetrics.Instrumentation != nil {
				left = p.LastMetrics.Instrumentation.LocalBlksHit
			}
			if q.LastMetrics != nil && q.LastMetrics.Instrumentation != nil {
				right = q.LastMetrics.Instrumentation.LocalBlksHit
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_LAST_LOCAL_BLKS_READ:

			left, right := uint64(0.0), uint64(0.0)
			if p.LastMetrics != nil && p.LastMetrics.Instrumentation != nil {
				left = p.LastMetrics.Instrumentation.LocalBlksRead
			}
			if q.LastMetrics != nil && q.LastMetrics.Instrumentation != nil {
				right = q.LastMetrics.Instrumentation.LocalBlksRead
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_LAST_LOCAL_BLKS_DIRTIED:

			left, right := uint64(0.0), uint64(0.0)
			if p.LastMetrics != nil && p.LastMetrics.Instrumentation != nil {
				left = p.LastMetrics.Instrumentation.LocalBlksDirtied
			}
			if q.LastMetrics != nil && q.LastMetrics.Instrumentation != nil {
				right = q.LastMetrics.Instrumentation.LocalBlksDirtied
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_LAST_LOCAL_BLKS_WRITTEN:

			left, right := uint64(0.0), uint64(0.0)
			if p.LastMetrics != nil && p.LastMetrics.Instrumentation != nil {
				left = p.LastMetrics.Instrumentation.LocalBlksWritten
			}
			if q.LastMetrics != nil && q.LastMetrics.Instrumentation != nil {
				right = q.LastMetrics.Instrumentation.LocalBlksWritten
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_LAST_TEMP_BLKS_READ:

			left, right := uint64(0.0), uint64(0.0)
			if p.LastMetrics != nil && p.LastMetrics.Instrumentation != nil {
				left = p.LastMetrics.Instrumentation.TempBlksRead
			}
			if q.LastMetrics != nil && q.LastMetrics.Instrumentation != nil {
				right = q.LastMetrics.Instrumentation.TempBlksRead
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_LAST_TEMP_BLKS_WRITTEN:

			left, right := uint64(0.0), uint64(0.0)
			if p.LastMetrics != nil && p.LastMetrics.Instrumentation != nil {
				left = p.LastMetrics.Instrumentation.TempBlksWritten
			}
			if q.LastMetrics != nil && q.LastMetrics.Instrumentation != nil {
				right = q.LastMetrics.Instrumentation.TempBlksWritten
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_LAST_BLK_READ_TIME:

			left, right := 0.0, 0.0
			if p.LastMetrics != nil && p.LastMetrics.Instrumentation != nil {
				left = p.LastMetrics.Instrumentation.BlkReadTime
			}
			if q.LastMetrics != nil && q.LastMetrics.Instrumentation != nil {
				right = q.LastMetrics.Instrumentation.BlkReadTime
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_LAST_BLK_WRITE_TIME:

			left, right := 0.0, 0.0
			if p.LastMetrics != nil && p.LastMetrics.Instrumentation != nil {
				left = p.LastMetrics.Instrumentation.BlkWriteTime
			}
			if q.LastMetrics != nil && q.LastMetrics.Instrumentation != nil {
				right = q.LastMetrics.Instrumentation.BlkWriteTime
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_LAST_SPILL_FILECOUNT:

			left, right := int64(0.0), int64(0.0)
			if p.LastMetrics != nil && p.LastMetrics.Spill != nil {
				left = int64(p.LastMetrics.Spill.FileCount)
			}
			if q.LastMetrics != nil && q.LastMetrics.Spill != nil {
				right = int64(q.LastMetrics.Spill.FileCount)
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_LAST_SPILL_TOTALBYTES:

			left, right := int64(0.0), int64(0.0)
			if p.LastMetrics != nil && p.LastMetrics.Spill != nil {
				left = p.LastMetrics.Spill.TotalBytes
			}
			if q.LastMetrics != nil && q.LastMetrics.Spill != nil {
				right = q.LastMetrics.Spill.TotalBytes
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_LAST_NET_SENT_TOTAL_BYTES:

			left, right := uint32(0.0), uint32(0.0)
			if p.LastMetrics != nil && p.LastMetrics.Instrumentation != nil && p.LastMetrics.Instrumentation.Sent != nil {
				left = p.LastMetrics.Instrumentation.Sent.TotalBytes
			}
			if q.LastMetrics != nil && q.LastMetrics.Instrumentation != nil && q.LastMetrics.Instrumentation.Sent != nil {
				right = q.LastMetrics.Instrumentation.Sent.TotalBytes
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_LAST_NET_SENT_TUPLE_BYTES:

			left, right := uint32(0.0), uint32(0.0)
			if p.LastMetrics != nil && p.LastMetrics.Instrumentation != nil && p.LastMetrics.Instrumentation.Sent != nil {
				left = p.LastMetrics.Instrumentation.Sent.TupleBytes
			}
			if q.LastMetrics != nil && q.LastMetrics.Instrumentation != nil && q.LastMetrics.Instrumentation.Sent != nil {
				right = q.LastMetrics.Instrumentation.Sent.TupleBytes
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_LAST_NET_SENT_CHUNKS:

			left, right := uint32(0.0), uint32(0.0)
			if p.LastMetrics != nil && p.LastMetrics.Instrumentation != nil && p.LastMetrics.Instrumentation.Sent != nil {
				left = p.LastMetrics.Instrumentation.Sent.Chunks
			}
			if q.LastMetrics != nil && q.LastMetrics.Instrumentation != nil && q.LastMetrics.Instrumentation.Sent != nil {
				right = q.LastMetrics.Instrumentation.Sent.Chunks
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_LAST_NET_RECV_TOTAL_BYTES:

			left, right := uint32(0.0), uint32(0.0)
			if p.LastMetrics != nil && p.LastMetrics.Instrumentation != nil && p.LastMetrics.Instrumentation.Received != nil {
				left = p.LastMetrics.Instrumentation.Received.TotalBytes
			}
			if q.LastMetrics != nil && q.LastMetrics.Instrumentation != nil && q.LastMetrics.Instrumentation.Received != nil {
				right = q.LastMetrics.Instrumentation.Received.TotalBytes
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_LAST_NET_RECV_TUPLE_BYTES:

			left, right := uint32(0.0), uint32(0.0)
			if p.LastMetrics != nil && p.LastMetrics.Instrumentation != nil && p.LastMetrics.Instrumentation.Received != nil {
				left = p.LastMetrics.Instrumentation.Received.TupleBytes
			}
			if q.LastMetrics != nil && q.LastMetrics.Instrumentation != nil && q.LastMetrics.Instrumentation.Received != nil {
				right = q.LastMetrics.Instrumentation.Received.TupleBytes
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_LAST_NET_RECV_CHUNKS:

			left, right := uint32(0.0), uint32(0.0)
			if p.LastMetrics != nil && p.LastMetrics.Instrumentation != nil && p.LastMetrics.Instrumentation.Received != nil {
				left = p.LastMetrics.Instrumentation.Received.Chunks
			}
			if q.LastMetrics != nil && q.LastMetrics.Instrumentation != nil && q.LastMetrics.Instrumentation.Received != nil {
				right = q.LastMetrics.Instrumentation.Received.Chunks
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_QUERY_RUNNINGTIMESECONDS:

			left, right := 0.0, 0.0
			if p.QueryMetrics != nil && p.QueryMetrics.SystemStat != nil {
				left = p.QueryMetrics.SystemStat.RunningTimeSeconds
			}
			if q.QueryMetrics != nil && q.QueryMetrics.SystemStat != nil {
				right = q.QueryMetrics.SystemStat.RunningTimeSeconds
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_QUERY_USERTIMESECONDS:

			left, right := 0.0, 0.0
			if p.QueryMetrics != nil && p.QueryMetrics.SystemStat != nil {
				left = p.QueryMetrics.SystemStat.UserTimeSeconds
			}
			if q.QueryMetrics != nil && q.QueryMetrics.SystemStat != nil {
				right = q.QueryMetrics.SystemStat.UserTimeSeconds
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_QUERY_KERNELTIMESECONDS:

			left, right := 0.0, 0.0
			if p.QueryMetrics != nil && p.QueryMetrics.SystemStat != nil {
				left = p.QueryMetrics.SystemStat.KernelTimeSeconds
			}
			if q.QueryMetrics != nil && q.QueryMetrics.SystemStat != nil {
				right = q.QueryMetrics.SystemStat.KernelTimeSeconds
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_QUERY_VSIZE:

			left, right := uint64(0.0), uint64(0.0)
			if p.QueryMetrics != nil && p.QueryMetrics.SystemStat != nil {
				left = p.QueryMetrics.SystemStat.Vsize
			}
			if q.QueryMetrics != nil && q.QueryMetrics.SystemStat != nil {
				right = q.QueryMetrics.SystemStat.Vsize
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_QUERY_RSS:

			left, right := uint64(0.0), uint64(0.0)
			if p.QueryMetrics != nil && p.QueryMetrics.SystemStat != nil {
				left = p.QueryMetrics.SystemStat.Rss
			}
			if q.QueryMetrics != nil && q.QueryMetrics.SystemStat != nil {
				right = q.QueryMetrics.SystemStat.Rss
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_QUERY_VMPEAKKB:

			left, right := uint64(0.0), uint64(0.0)
			if p.QueryMetrics != nil && p.QueryMetrics.SystemStat != nil {
				left = p.QueryMetrics.SystemStat.VmPeakKb
			}
			if q.QueryMetrics != nil && q.QueryMetrics.SystemStat != nil {
				right = q.QueryMetrics.SystemStat.VmPeakKb
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_QUERY_RCHAR:

			left, right := uint64(0.0), uint64(0.0)
			if p.QueryMetrics != nil && p.QueryMetrics.SystemStat != nil {
				left = p.QueryMetrics.SystemStat.Rchar
			}
			if q.QueryMetrics != nil && q.QueryMetrics.SystemStat != nil {
				right = q.QueryMetrics.SystemStat.Rchar
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_QUERY_WCHAR:

			left, right := uint64(0.0), uint64(0.0)
			if p.QueryMetrics != nil && p.QueryMetrics.SystemStat != nil {
				left = p.QueryMetrics.SystemStat.Wchar
			}
			if q.QueryMetrics != nil && q.QueryMetrics.SystemStat != nil {
				right = q.QueryMetrics.SystemStat.Wchar
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_QUERY_SYSCR:

			left, right := uint64(0.0), uint64(0.0)
			if p.QueryMetrics != nil && p.QueryMetrics.SystemStat != nil {
				left = p.QueryMetrics.SystemStat.Syscr
			}
			if q.QueryMetrics != nil && q.QueryMetrics.SystemStat != nil {
				right = q.QueryMetrics.SystemStat.Syscr
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_QUERY_SYSCW:

			left, right := uint64(0.0), uint64(0.0)
			if p.QueryMetrics != nil && p.QueryMetrics.SystemStat != nil {
				left = p.QueryMetrics.SystemStat.Syscw
			}
			if q.QueryMetrics != nil && q.QueryMetrics.SystemStat != nil {
				right = q.QueryMetrics.SystemStat.Syscw
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_QUERY_READ_BYTES:

			left, right := uint64(0.0), uint64(0.0)
			if p.QueryMetrics != nil && p.QueryMetrics.SystemStat != nil {
				left = p.QueryMetrics.SystemStat.ReadBytes
			}
			if q.QueryMetrics != nil && q.QueryMetrics.SystemStat != nil {
				right = q.QueryMetrics.SystemStat.ReadBytes
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_QUERY_WRITE_BYTES:

			left, right := uint64(0.0), uint64(0.0)
			if p.QueryMetrics != nil && p.QueryMetrics.SystemStat != nil {
				left = p.QueryMetrics.SystemStat.WriteBytes
			}
			if q.QueryMetrics != nil && q.QueryMetrics.SystemStat != nil {
				right = q.QueryMetrics.SystemStat.WriteBytes
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_QUERY_CANCELLED_WRITE_BYTES:

			left, right := uint64(0.0), uint64(0.0)
			if p.QueryMetrics != nil && p.QueryMetrics.SystemStat != nil {
				left = p.QueryMetrics.SystemStat.CancelledWriteBytes
			}
			if q.QueryMetrics != nil && q.QueryMetrics.SystemStat != nil {
				right = q.QueryMetrics.SystemStat.CancelledWriteBytes
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_QUERY_NTUPLES:

			left, right := uint64(0.0), uint64(0.0)
			if p.QueryMetrics != nil && p.QueryMetrics.Instrumentation != nil {
				left = p.QueryMetrics.Instrumentation.Ntuples
			}
			if q.QueryMetrics != nil && q.QueryMetrics.Instrumentation != nil {
				right = q.QueryMetrics.Instrumentation.Ntuples
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_QUERY_NLOOPS:

			left, right := uint64(0.0), uint64(0.0)
			if p.QueryMetrics != nil && p.QueryMetrics.Instrumentation != nil {
				left = p.QueryMetrics.Instrumentation.Nloops
			}
			if q.QueryMetrics != nil && q.QueryMetrics.Instrumentation != nil {
				right = q.QueryMetrics.Instrumentation.Nloops
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_QUERY_TUPLECOUNT:

			left, right := uint64(0.0), uint64(0.0)
			if p.QueryMetrics != nil && p.QueryMetrics.Instrumentation != nil {
				left = p.QueryMetrics.Instrumentation.Tuplecount
			}
			if q.QueryMetrics != nil && q.QueryMetrics.Instrumentation != nil {
				right = q.QueryMetrics.Instrumentation.Tuplecount
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_QUERY_FIRSTTUPLE:

			left, right := 0.0, 0.0
			if p.QueryMetrics != nil && p.QueryMetrics.Instrumentation != nil {
				left = p.QueryMetrics.Instrumentation.Firsttuple
			}
			if q.QueryMetrics != nil && q.QueryMetrics.Instrumentation != nil {
				right = q.QueryMetrics.Instrumentation.Firsttuple
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_QUERY_STARTUP:

			left, right := 0.0, 0.0
			if p.QueryMetrics != nil && p.QueryMetrics.Instrumentation != nil {
				left = p.QueryMetrics.Instrumentation.Startup
			}
			if q.QueryMetrics != nil && q.QueryMetrics.Instrumentation != nil {
				right = q.QueryMetrics.Instrumentation.Startup
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_QUERY_TOTAL:

			left, right := 0.0, 0.0
			if p.QueryMetrics != nil && p.QueryMetrics.Instrumentation != nil {
				left = p.QueryMetrics.Instrumentation.Total
			}
			if q.QueryMetrics != nil && q.QueryMetrics.Instrumentation != nil {
				right = q.QueryMetrics.Instrumentation.Total
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_QUERY_SHARED_BLKS_HIT:

			left, right := uint64(0.0), uint64(0.0)
			if p.QueryMetrics != nil && p.QueryMetrics.Instrumentation != nil {
				left = p.QueryMetrics.Instrumentation.SharedBlksHit
			}
			if q.QueryMetrics != nil && q.QueryMetrics.Instrumentation != nil {
				right = q.QueryMetrics.Instrumentation.SharedBlksHit
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_QUERY_SHARED_BLKS_READ:

			left, right := uint64(0.0), uint64(0.0)
			if p.QueryMetrics != nil && p.QueryMetrics.Instrumentation != nil {
				left = p.QueryMetrics.Instrumentation.SharedBlksRead
			}
			if q.QueryMetrics != nil && q.QueryMetrics.Instrumentation != nil {
				right = q.QueryMetrics.Instrumentation.SharedBlksRead
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_QUERY_SHARED_BLKS_DIRTIED:

			left, right := uint64(0.0), uint64(0.0)
			if p.QueryMetrics != nil && p.QueryMetrics.Instrumentation != nil {
				left = p.QueryMetrics.Instrumentation.SharedBlksDirtied
			}
			if q.QueryMetrics != nil && q.QueryMetrics.Instrumentation != nil {
				right = q.QueryMetrics.Instrumentation.SharedBlksDirtied
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_QUERY_SHARED_BLKS_WRITTEN:

			left, right := uint64(0.0), uint64(0.0)
			if p.QueryMetrics != nil && p.QueryMetrics.Instrumentation != nil {
				left = p.QueryMetrics.Instrumentation.SharedBlksWritten
			}
			if q.QueryMetrics != nil && q.QueryMetrics.Instrumentation != nil {
				right = q.QueryMetrics.Instrumentation.SharedBlksWritten
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_QUERY_LOCAL_BLKS_HIT:

			left, right := uint64(0.0), uint64(0.0)
			if p.QueryMetrics != nil && p.QueryMetrics.Instrumentation != nil {
				left = p.QueryMetrics.Instrumentation.LocalBlksHit
			}
			if q.QueryMetrics != nil && q.QueryMetrics.Instrumentation != nil {
				right = q.QueryMetrics.Instrumentation.LocalBlksHit
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_QUERY_LOCAL_BLKS_READ:

			left, right := uint64(0.0), uint64(0.0)
			if p.QueryMetrics != nil && p.QueryMetrics.Instrumentation != nil {
				left = p.QueryMetrics.Instrumentation.LocalBlksRead
			}
			if q.QueryMetrics != nil && q.QueryMetrics.Instrumentation != nil {
				right = q.QueryMetrics.Instrumentation.LocalBlksRead
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_QUERY_LOCAL_BLKS_DIRTIED:

			left, right := uint64(0.0), uint64(0.0)
			if p.QueryMetrics != nil && p.QueryMetrics.Instrumentation != nil {
				left = p.QueryMetrics.Instrumentation.LocalBlksDirtied
			}
			if q.QueryMetrics != nil && q.QueryMetrics.Instrumentation != nil {
				right = q.QueryMetrics.Instrumentation.LocalBlksDirtied
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_QUERY_LOCAL_BLKS_WRITTEN:

			left, right := uint64(0.0), uint64(0.0)
			if p.QueryMetrics != nil && p.QueryMetrics.Instrumentation != nil {
				left = p.QueryMetrics.Instrumentation.LocalBlksWritten
			}
			if q.QueryMetrics != nil && q.QueryMetrics.Instrumentation != nil {
				right = q.QueryMetrics.Instrumentation.LocalBlksWritten
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_QUERY_TEMP_BLKS_READ:

			left, right := uint64(0.0), uint64(0.0)
			if p.QueryMetrics != nil && p.QueryMetrics.Instrumentation != nil {
				left = p.QueryMetrics.Instrumentation.TempBlksRead
			}
			if q.QueryMetrics != nil && q.QueryMetrics.Instrumentation != nil {
				right = q.QueryMetrics.Instrumentation.TempBlksRead
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_QUERY_TEMP_BLKS_WRITTEN:

			left, right := uint64(0.0), uint64(0.0)
			if p.QueryMetrics != nil && p.QueryMetrics.Instrumentation != nil {
				left = p.QueryMetrics.Instrumentation.TempBlksWritten
			}
			if q.QueryMetrics != nil && q.QueryMetrics.Instrumentation != nil {
				right = q.QueryMetrics.Instrumentation.TempBlksWritten
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_QUERY_BLK_READ_TIME:

			left, right := 0.0, 0.0
			if p.QueryMetrics != nil && p.QueryMetrics.Instrumentation != nil {
				left = p.QueryMetrics.Instrumentation.BlkReadTime
			}
			if q.QueryMetrics != nil && q.QueryMetrics.Instrumentation != nil {
				right = q.QueryMetrics.Instrumentation.BlkReadTime
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_QUERY_BLK_WRITE_TIME:

			left, right := 0.0, 0.0
			if p.QueryMetrics != nil && p.QueryMetrics.Instrumentation != nil {
				left = p.QueryMetrics.Instrumentation.BlkWriteTime
			}
			if q.QueryMetrics != nil && q.QueryMetrics.Instrumentation != nil {
				right = q.QueryMetrics.Instrumentation.BlkWriteTime
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_QUERY_SPILL_FILECOUNT:

			left, right := int64(0.0), int64(0.0)
			if p.QueryMetrics != nil && p.QueryMetrics.Spill != nil {
				left = int64(p.QueryMetrics.Spill.FileCount)
			}
			if q.QueryMetrics != nil && q.QueryMetrics.Spill != nil {
				right = int64(q.QueryMetrics.Spill.FileCount)
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_QUERY_SPILL_TOTALBYTES:

			left, right := int64(0.0), int64(0.0)
			if p.QueryMetrics != nil && p.QueryMetrics.Spill != nil {
				left = p.QueryMetrics.Spill.TotalBytes
			}
			if q.QueryMetrics != nil && q.QueryMetrics.Spill != nil {
				right = q.QueryMetrics.Spill.TotalBytes
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_QUERY_NET_SENT_TOTAL_BYTES:

			left, right := uint32(0.0), uint32(0.0)
			if p.QueryMetrics != nil && p.QueryMetrics.Instrumentation != nil && p.QueryMetrics.Instrumentation.Sent != nil {
				left = p.QueryMetrics.Instrumentation.Sent.TotalBytes
			}
			if q.QueryMetrics != nil && q.QueryMetrics.Instrumentation != nil && q.QueryMetrics.Instrumentation.Sent != nil {
				right = q.QueryMetrics.Instrumentation.Sent.TotalBytes
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_QUERY_NET_SENT_TUPLE_BYTES:

			left, right := uint32(0.0), uint32(0.0)
			if p.QueryMetrics != nil && p.QueryMetrics.Instrumentation != nil && p.QueryMetrics.Instrumentation.Sent != nil {
				left = p.QueryMetrics.Instrumentation.Sent.TupleBytes
			}
			if q.QueryMetrics != nil && q.QueryMetrics.Instrumentation != nil && q.QueryMetrics.Instrumentation.Sent != nil {
				right = q.QueryMetrics.Instrumentation.Sent.TupleBytes
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_QUERY_NET_SENT_CHUNKS:

			left, right := uint32(0.0), uint32(0.0)
			if p.QueryMetrics != nil && p.QueryMetrics.Instrumentation != nil && p.QueryMetrics.Instrumentation.Sent != nil {
				left = p.QueryMetrics.Instrumentation.Sent.Chunks
			}
			if q.QueryMetrics != nil && q.QueryMetrics.Instrumentation != nil && q.QueryMetrics.Instrumentation.Sent != nil {
				right = q.QueryMetrics.Instrumentation.Sent.Chunks
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_QUERY_NET_RECV_TOTAL_BYTES:

			left, right := uint32(0.0), uint32(0.0)
			if p.QueryMetrics != nil && p.QueryMetrics.Instrumentation != nil && p.QueryMetrics.Instrumentation.Received != nil {
				left = p.QueryMetrics.Instrumentation.Received.TotalBytes
			}
			if q.QueryMetrics != nil && q.QueryMetrics.Instrumentation != nil && q.QueryMetrics.Instrumentation.Received != nil {
				right = q.QueryMetrics.Instrumentation.Received.TotalBytes
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_QUERY_NET_RECV_TUPLE_BYTES:

			left, right := uint32(0), uint32(0)
			if p.QueryMetrics != nil && p.QueryMetrics.Instrumentation != nil && p.QueryMetrics.Instrumentation.Received != nil {
				left = p.QueryMetrics.Instrumentation.Received.TupleBytes
			}
			if q.QueryMetrics != nil && q.QueryMetrics.Instrumentation != nil && q.QueryMetrics.Instrumentation.Received != nil {
				right = q.QueryMetrics.Instrumentation.Received.TupleBytes
			}
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		case ms.Fields[k].FieldName == pbm.SessionField_RUNNING_QUERY_SLICES:
			left, right := p.RunningQuerySlices, q.RunningQuerySlices
			if less(ms.Fields[k].Order, left, right) {
				return true
			}
			if less(ms.Fields[k].Order, right, left) {
				return false
			}
		}
	}
	return false
}

func getSortFields(fields []*pbm.SessionFieldWrapper) []*pbm.SessionFieldWrapper {
	result := make([]*pbm.SessionFieldWrapper, 0)
	for _, field := range fields {
		if field.Order != pbm.SortOrder_SORT_ORDER_UNSPECIFIED {
			result = append(result, field)
		}
	}
	return result
}

func SortResult(sessionsState *[]*pbc.SessionState, fields []*pbm.SessionFieldWrapper) error {
	sortFields := getSortFields(fields)
	if len(sortFields) == 0 {
		return fmt.Errorf("invalid input - zero sort field after match")
	}
	ms := &MultipleSorter{
		SessionState: sessionsState,
		Fields:       sortFields,
	}
	sort.Sort(ms)
	return nil
}

func FilterOutSession(filters []*pbm.SessionFilter, sessionState *pbc.SessionState) bool {
	sortMap := make(map[pbm.SessionFilterEnum]bool)
	for _, filter := range filters {
		sortMap[filter.FieldName] = false
	}
	for _, filter := range filters {
		switch {
		case filter.FieldName == pbm.SessionFilterEnum_SESSION_FILTER_HOST:
			if sessionState.Hostname == filter.Value {
				sortMap[pbm.SessionFilterEnum_SESSION_FILTER_HOST] = true
			}
		case filter.FieldName == pbm.SessionFilterEnum_SESSION_FILTER_USER:
			if sessionState.SessionInfo.User == filter.Value {
				sortMap[pbm.SessionFilterEnum_SESSION_FILTER_USER] = true
			}
		case filter.FieldName == pbm.SessionFilterEnum_SESSION_FILTER_DATABASE:
			if sessionState.SessionInfo.Database == filter.Value {
				sortMap[pbm.SessionFilterEnum_SESSION_FILTER_DATABASE] = true
			}
		case filter.FieldName == pbm.SessionFilterEnum_SESSION_FILTER_APPLICATION_NAME:
			if sessionState.SessionInfo.ApplicationName == filter.Value {
				sortMap[pbm.SessionFilterEnum_SESSION_FILTER_APPLICATION_NAME] = true
			}
		case filter.FieldName == pbm.SessionFilterEnum_SESSION_FILTER_CLIENT_HOSTNAME:
			if sessionState.SessionInfo.ClientHostname == filter.Value {
				sortMap[pbm.SessionFilterEnum_SESSION_FILTER_CLIENT_HOSTNAME] = true
			}
		case filter.FieldName == pbm.SessionFilterEnum_SESSION_FILTER_STATE:
			switch filterValue := filter.Value; filterValue {
			case "SESSION_STATUS_IDLE":
				if strings.EqualFold(sessionState.SessionInfo.State, "IDLE") {
					sortMap[pbm.SessionFilterEnum_SESSION_FILTER_STATE] = true
				}
			case "SESSION_STATUS_BLOCKED", "blocked":
				if sessionState.SessionInfo.BlockedBySessId != 0 {
					sortMap[pbm.SessionFilterEnum_SESSION_FILTER_STATE] = true
				}
			case "SESSION_STATUS_ACTIVE":
				if strings.EqualFold(sessionState.SessionInfo.State, "ACTIVE") {
					sortMap[pbm.SessionFilterEnum_SESSION_FILTER_STATE] = true
				}
			case "SESSION_STATUS_IDLE_TRANSACTION":
				if strings.EqualFold(sessionState.SessionInfo.State, "IDLE IN TRANSACTION") {
					sortMap[pbm.SessionFilterEnum_SESSION_FILTER_STATE] = true
				}
			case "SESSION_STATUS_WAITING":
				if strings.EqualFold(sessionState.SessionInfo.State, "WAITING") {
					sortMap[pbm.SessionFilterEnum_SESSION_FILTER_STATE] = true
				}
			default:
				if strings.EqualFold(sessionState.SessionInfo.State, filter.Value) {
					sortMap[pbm.SessionFilterEnum_SESSION_FILTER_STATE] = true
				}
			}
		case filter.FieldName == pbm.SessionFilterEnum_SESSION_FILTER_RSGNAME:
			if sessionState.SessionInfo.Rsgname == filter.Value {
				sortMap[pbm.SessionFilterEnum_SESSION_FILTER_RSGNAME] = true
			}
		case filter.FieldName == pbm.SessionFilterEnum_SESSION_FILTER_SESS_ID:
			i, err := strconv.ParseInt(filter.Value, 10, 64)
			if err != nil {
				return true
			}
			if sessionState.SessionKey.SessId == i {
				sortMap[pbm.SessionFilterEnum_SESSION_FILTER_SESS_ID] = true
			}
		case filter.FieldName == pbm.SessionFilterEnum_SESSION_FILTER_TM_ID:
			i, err := strconv.ParseInt(filter.Value, 10, 64)
			if err != nil {
				return true
			}
			if sessionState.SessionKey.TmId == i {
				sortMap[pbm.SessionFilterEnum_SESSION_FILTER_TM_ID] = true
			}
		}
	}
	for _, f := range sortMap {
		if !f {
			return true
		}
	}
	return false
}

func (s *GetMasterInfoServer) GetGPSessions(ctx context.Context, in *pbm.GetGPSessionsReq) (*pbm.GetGPSessionsResponse, error) {
	s.logger.Debugf("got get sessions request %v", in)
	start := time.Now()
	if in.HideEmptyQueries {
		return s.GetGPQueries(ctx, &pbm.GetGPQueriesReq{
			Field:     in.Field,
			Filter:    in.Filter,
			PageSize:  in.PageSize,
			PageToken: in.PageToken,
		})
	}
	queryType := in.ShowQueryType
	if queryType == pbm.RunningQueryType_RQT_UNSPECIFIED {
		// by default show top queries
		queryType = pbm.RunningQueryType_RQT_TOP
	}
	// refresh list of sessions
	err := s.backgroundStorage.TryRefreshSessionsFromGP(ctx, s.statActivityLister, true)
	if err != nil {
		s.logger.Errorf("error while refreshing session list: %v", err)
	}
	responseI, err := s.backgroundStorage.SessionStorage.GetAllSessions(in.ShowSystem, queryType)
	s.logger.Debugf("got session list len %v", len(responseI.SessionsState))
	if err != nil {
		s.logger.Errorf("error while get session list: %v", err)
		return nil, fmt.Errorf("error while get session list: %w", err)
	}
	if len(responseI.SessionsState) > 0 {
		s.logger.Debugf("first session state data is %v len is %v", responseI.SessionsState[0], len(responseI.SessionsState))
	}
	filteredState := make([]*pbc.SessionState, 0)
	for _, sessionState := range responseI.SessionsState {
		if FilterOutSession(in.Filter, sessionState) {
			continue
		}
		filteredState = append(filteredState, sessionState)
	}
	if len(filteredState) > 0 {
		s.logger.Debugf("first filtered state data is %v len is %v", filteredState[0], len(filteredState))
	}
	s.logger.Debugf("Get session data took %v", time.Since(start))
	result := &pbm.GetGPSessionsResponse{
		SessionsState: make([]*pbc.SessionState, 0),
	}
	err = SortResult(&filteredState, in.Field)
	if err != nil {
		s.logger.Debugf("data not sorted: no sort field specified %v", err)
	}
	if len(filteredState) > 0 {
		s.logger.Debugf("first filtered state data is %v", filteredState[0])
	}
	currentToken := int64(0)
	queryToken := int64(0)
	if in.PageToken != "" {
		var err error
		queryToken, err = strconv.ParseInt(in.PageToken, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid input - fail to parse page token")
		}
	}
	more := false
	pageSize := in.PageSize
	if pageSize == 0 {
		pageSize = 100
	}
	msgSize := 0
	for _, sessionState := range filteredState {
		currentToken += 1
		if currentToken < queryToken {
			continue
		}
		msgSize += proto.Size(sessionState)
		if len(result.SessionsState) >= int(pageSize) {
			more = true
			break
		}
		if msgSize >= s.maxMessageSize {
			more = true
			s.logger.Infof("current sizes %d more then max %d", msgSize, s.maxMessageSize)
			break
		}
		result.SessionsState = append(result.SessionsState, sessionState)
	}
	if len(result.SessionsState) > 0 {
		s.logger.Debugf("first result state data is %v", result.SessionsState[0])
	}
	if more {
		result.NextPageToken = fmt.Sprintf("%d", currentToken)
	}
	s.logger.Debugf("Get session total processing time took %v", time.Since(start))
	if metrics.YagpccMetrics != nil {
		metrics.YagpccMetrics.HandleLatencies.With(map[string]string{"method": "GetGPSessions"}).Observe(time.Since(start).Seconds())
	}
	return result, nil
}

func (s *GetMasterInfoServer) GetGPSession(ctx context.Context, in *pbm.GetGPSessionReq) (*pbm.GetGPSessionResponse, error) {
	s.logger.Debugf("got get gp session data request %v", in)
	if in.SessionKey == nil {
		return nil, fmt.Errorf("invalid unput - session key cannot be nil")
	}
	start := time.Now()
	sKey := gp.SessionKey{
		SessID: int(in.SessionKey.SessId),
	}
	sesI, ok := s.backgroundStorage.SessionStorage.GetSession(sKey)
	res := &pbm.GetGPSessionResponse{}
	if ok {
		sessionState, err := s.backgroundStorage.SessionStorage.GetSessionDesc(
			sKey,
			sesI,
			false,
			pbm.RunningQueryType_RQT_LAST,
			-1)
		if err != nil {
			return nil, fmt.Errorf("fail to get session desc %w", err)
		}
		res.SessionsState = sessionState
	}
	if metrics.YagpccMetrics != nil {
		metrics.YagpccMetrics.HandleLatencies.With(map[string]string{"method": "GetGPSession"}).Observe(time.Since(start).Seconds())
	}
	return res, nil
}

func (s *GetMasterInfoServer) GetTotalSessionsStat(ctx context.Context, in *pbm.GetTotalSessionsReq) (*pbm.GetTotalSessionsResponse, error) {
	s.logger.Debugf("got total sessionы stat request %v", in)
	start := time.Now()
	totalResponse := pbm.GetTotalSessionsResponse{}
	statMap := make(map[string]int)
	sMap := s.backgroundStorage.SessionStorage.GetSessions()
	for _, valSO := range sMap {
		valSO.SessionLock.RLock()
		state := "Unknown"
		if valSO.SessionData.GpStatInfo != nil && valSO.SessionData.GpStatInfo.State != nil {
			state = strings.ToLower(*valSO.SessionData.GpStatInfo.State)
		}
		val, ok := statMap[state]
		if ok {
			statMap[state] = val + 1
		} else {
			statMap[state] = 1
		}
		valSO.SessionLock.RUnlock()
	}
	for key, val := range statMap {
		totalResponse.SessionsStat = append(totalResponse.SessionsStat, &pbm.SessionStat{
			State: key,
			Count: float64(val),
		})
	}
	s.logger.Debugf("Get total sessionы stat took %v", time.Since(start))
	if metrics.YagpccMetrics != nil {
		metrics.YagpccMetrics.HandleLatencies.With(map[string]string{"method": "GetTotalSessionsStat"}).Observe(time.Since(start).Seconds())
	}
	return &totalResponse, nil
}

func (s *GetMasterInfoServer) GetGPQueries(ctx context.Context, in *pbm.GetGPQueriesReq) (*pbm.GetGPSessionsResponse, error) {
	s.logger.Debugf("got running queries request %v", in)
	start := time.Now()
	rQueries := s.backgroundStorage.RQStorage.GetQueries()
	filteredState := make([]*pbc.SessionState, 0)
	for qKeyI := range rQueries {
		sessionState, err := s.backgroundStorage.SessionStorage.GetQueryDesc(&qKeyI, true)
		if err != nil {
			s.logger.Errorf("Fail to get queryState %v", err)
			continue
		}
		if FilterOutSession(in.Filter, sessionState) {
			continue
		}
		filteredState = append(filteredState, sessionState)
	}
	s.logger.Debugf("Get session data len %v of running queries took %v", len(filteredState), time.Since(start))
	result := &pbm.GetGPSessionsResponse{
		SessionsState: make([]*pbc.SessionState, 0),
	}
	err := SortResult(&filteredState, in.Field)
	if err != nil {
		s.logger.Debugf("data not sorted: no sort field specified")
	}
	s.logger.Debugf("Sort data len %v took %v", len(filteredState), time.Since(start))
	currentToken := int64(0)
	queryToken := int64(0)
	if in.PageToken != "" {
		var err error
		queryToken, err = strconv.ParseInt(in.PageToken, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid input - fail to parse page token")
		}
	}
	more := false
	pageSize := in.PageSize
	if pageSize == 0 {
		pageSize = 100
	}
	msgSize := 0
	for _, sessionState := range filteredState {
		currentToken += 1
		if currentToken < queryToken {
			continue
		}
		msgSize += proto.Size(sessionState)
		if len(result.SessionsState) >= int(pageSize) {
			more = true
			break
		}
		if msgSize >= s.maxMessageSize {
			more = true
			s.logger.Infof("current sizes %d more then max %d", msgSize, s.maxMessageSize)
			break
		}
		result.SessionsState = append(result.SessionsState, sessionState)
	}
	if more {
		result.NextPageToken = fmt.Sprintf("%d", currentToken)
	}
	s.logger.Debugf("Result dataset size is %v", len(result.SessionsState))
	s.logger.Debugf("Get running queries took %v", time.Since(start))
	if metrics.YagpccMetrics != nil {
		metrics.YagpccMetrics.HandleLatencies.With(map[string]string{"method": "GetGPQueries"}).Observe(time.Since(start).Seconds())
	}
	return result, nil
}

func (s *GetMasterInfoServer) GetGPQuery(ctx context.Context, in *pbm.GetGPQueryReq) (*pbm.GetGPQueryResponse, error) {
	s.logger.Debugf("got get query data request %v", in)
	if in.QueryKey == nil {
		return nil, fmt.Errorf("invalid inout - query key cannot be nil")
	}
	start := time.Now()
	queryResponse := pbm.GetGPQueryResponse{}
	qKey := storage.QueryKey{
		Ssid: in.QueryKey.Ssid,
		Ccnt: in.QueryKey.Ccnt,
	}

	tmID := atomic.LoadInt64(&gp.DiscoveredTmID)
	queryData, err := s.backgroundStorage.RQStorage.GetQueryData(qKey, tmID)
	if err != nil {
		return nil, err
	}
	queryResponse.QueriesData = queryData

	s.logger.Debugf("Get query data took %v", time.Since(start))
	if metrics.YagpccMetrics != nil {
		metrics.YagpccMetrics.HandleLatencies.With(map[string]string{"method": "GetGPQuery"}).Observe(time.Since(start).Seconds())
	}
	return &queryResponse, nil
}
