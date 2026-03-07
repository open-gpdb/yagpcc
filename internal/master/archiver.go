package master

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	pbm "github.com/open-gpdb/yagpcc/api/proto/agent_master"
	pbc "github.com/open-gpdb/yagpcc/api/proto/common"
	"github.com/open-gpdb/yagpcc/internal/metrics"
	"github.com/open-gpdb/yagpcc/internal/storage"
	"github.com/open-gpdb/yagpcc/internal/utils"
)

type (
	EndedQuery struct {
		QKey *storage.QueryKey
		QVal *storage.RunningQuery
	}
)

func getQueryStatForWrite(qT *pbm.TotalQueryData, clusterID string, hostname string) *pbm.QueryStatWrite {
	return &pbm.QueryStatWrite{
		ClusterId:         clusterID,
		Hostname:          hostname,
		CollectTime:       utils.GetTimeAsString(utils.GetTimeForTimestamp(qT.QueryStat.CollectTime)),
		QueryKey:          qT.QueryStat.QueryKey,
		QueryInfo:         qT.QueryStat.QueryInfo,
		StatKind:          qT.QueryStat.StatKind,
		QueryStatus:       qT.QueryStat.QueryStatus,
		StartTime:         utils.GetTimeAsString(utils.GetTimeForTimestamp(qT.QueryStat.StartTime)),
		EndTime:           utils.GetTimeAsString(utils.GetTimeForTimestamp(qT.QueryStat.EndTime)),
		Completed:         qT.QueryStat.Completed,
		TotalQueryMetrics: qT.QueryStat.TotalQueryMetrics,
		AggregatedMetrics: qT.QueryStat.AggregatedMetrics,
		BlockedBySessId:   qT.QueryStat.BlockedBySessId,
		WaitMode:          qT.QueryStat.WaitMode,
		LockedItem:        qT.QueryStat.LockedItem,
		LockedMode:        qT.QueryStat.LockedMode,
		SessionState:      qT.QueryStat.SessionState,
		Message:           qT.QueryStat.Message,
		Slices:            qT.QueryStat.Slices,
	}
}

func getSegStatForWrite(qS *pbm.SegmentMetrics, clusterID string, qI *pbc.QueryInfo, qKey *pbc.QueryKey, collectTime time.Time) *pbm.SegmentMetricsWrite {
	return &pbm.SegmentMetricsWrite{
		ClusterId:      clusterID,
		Hostname:       qS.Hostname,
		CollectTime:    utils.GetTimeAsString(collectTime),
		SegmentKey:     qS.SegmentKey,
		QueryStatus:    qS.QueryStatus,
		StartTime:      utils.GetTimeAsString(utils.GetTimeForTimestamp(qS.StartTime)),
		EndTime:        utils.GetTimeAsString(utils.GetTimeForTimestamp(qS.EndTime)),
		SegmentMetrics: qS.SegmentMetrics,
		QueryKey:       qKey,
		QueryInfo:      qI,
	}
}

func ArchiveQuery(qT *pbm.TotalQueryData, clusterID string, queryChan chan *pbm.QueryStatWrite,
	segChan chan *pbm.SegmentMetricsWrite,
	hostname string) {
	if qT == nil || qT.QueryStat == nil {
		return
	}
	queryChan <- getQueryStatForWrite(qT, clusterID, hostname)
	qI := pbc.QueryInfo{}
	if qT.QueryStat.QueryInfo != nil {
		qI.QueryId = qT.QueryStat.QueryInfo.QueryId
		qI.PlanId = qT.QueryStat.QueryInfo.PlanId
		qI.UserName = qT.QueryStat.QueryInfo.UserName
		qI.DatabaseName = qT.QueryStat.QueryInfo.DatabaseName
		qI.Generator = qT.QueryStat.QueryInfo.Generator
	}
	for _, segStat := range qT.SegmentQueryMetrics {
		segChan <- getSegStatForWrite(segStat, clusterID, &qI, qT.QueryStat.QueryKey, utils.GetTimeForTimestamp(qT.QueryStat.CollectTime))
	}
}

func (bs *BackgroundStorage) AggtregateDataToQueryAndSession(qKey storage.QueryKey, qVal *storage.RunningQuery) (*pbm.TotalQueryData, error) {
	// create empty structure for store result
	qT := &pbm.TotalQueryData{
		QueryStat:           &pbm.QueryStat{},
		SegmentQueryMetrics: make([]*pbm.SegmentMetrics, 0),
	}
	// get total stat
	err := storage.AggregateQueryStat(qT, qKey, qVal, 0, storage.AggSegmentHost)
	if err != nil {
		return nil, fmt.Errorf("fail to convert running query %v", err)
	}
	// sanity check - StartTime and EndTime should be set
	if !qT.QueryStat.StartTime.IsValid() {
		qT.QueryStat.StartTime = timestamppb.New(time.Now())
	}
	if qT.QueryStat.StartTime.GetSeconds() == 0 {
		qT.QueryStat.StartTime = timestamppb.New(time.Now())
	}
	if !qT.QueryStat.EndTime.IsValid() {
		qT.QueryStat.EndTime = qT.QueryStat.StartTime
	}
	if qT.QueryStat.EndTime.GetSeconds() < qT.QueryStat.StartTime.GetSeconds() {
		qT.QueryStat.EndTime = qT.QueryStat.StartTime
	}
	qT.QueryStat.CollectTime = qT.QueryStat.EndTime
	// update session statistics
	err = bs.SessionStorage.UpdateSessionStat(qT.QueryStat)
	if err != nil {
		return nil, fmt.Errorf("fail to aggregate session data %v", err)
	}

	return qT, nil
}

func (bs *BackgroundStorage) ArchiveOrAggregate(ctx context.Context,
	longQuery time.Duration,
	clusterID string,
	archChan chan *EndedQuery,
	queryChan chan *pbm.QueryStatWrite,
	segChan chan *pbm.SegmentMetricsWrite,
	hostname string) {
	for {
		select {
		case archS := <-archChan:
			qT, err := bs.AggtregateDataToQueryAndSession(*archS.QKey, archS.QVal)
			if err != nil {
				// just log error here
				bs.l.Errorf("fail gather aggregated data %v", err)
				continue
			}

			eTime := utils.GetTimeForTimestamp(qT.QueryStat.EndTime)
			sTime := utils.GetTimeForTimestamp(qT.QueryStat.StartTime)

			// record execution time in histogram
			if metrics.YagpccMetrics != nil {
				metrics.YagpccMetrics.QueryLatencies.With(map[string]string{}).Observe(eTime.Sub(sTime).Seconds())
				if storage.CheckQueryErrored(int32(qT.QueryStat.QueryStatus)) {
					metrics.YagpccMetrics.FailedQueries.Inc()
				}
			}

			// 5. sent data to archive database
			if eTime.Sub(sTime) < longQuery {
				bs.l.Debugf("Aggregate query %v", *archS.QKey)
				err := bs.AggStorage.AggQuery(qT)
				if err != nil {
					bs.l.Errorf("fail to add query to aggregated storage %v", err)
				}
			} else {
				bs.l.Debugf("Archive query %v", *archS.QKey)
				// once again check context
				select {
				case <-ctx.Done():
					bs.l.Warn("Done ArchiveOrAggregate")
					return
				default:
					ArchiveQuery(qT, clusterID, queryChan, segChan, hostname)
				}
			}

		case <-ctx.Done():
			bs.l.Warn("Done ArchiveOrAggregate")
			return
		}
	}
}
