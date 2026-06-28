// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright
// ownership. The ASF licenses this file to You under the Apache
// License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the
// License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package master

import (
	"context"
	"fmt"
	"os"
	"sort"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	pbm "github.com/open-gpdb/yagpcc/api/proto/agent_master"
	pb "github.com/open-gpdb/yagpcc/api/proto/agent_segment"
	pbc "github.com/open-gpdb/yagpcc/api/proto/common"
	"github.com/open-gpdb/yagpcc/internal/config"
	"github.com/open-gpdb/yagpcc/internal/gp"
	"github.com/open-gpdb/yagpcc/internal/metrics"
	"github.com/open-gpdb/yagpcc/internal/sink/clickhouse"
	"github.com/open-gpdb/yagpcc/internal/storage"
	"github.com/open-gpdb/yagpcc/internal/utils"
	"go.uber.org/zap"
)

const (
	segChanSize = 1000
)

type (
	segmentAddr struct {
		hostname  string
		port      uint32
		queueTime time.Time
	}
	segmentMap map[string]*segmentAddr

	// ClickhouseSink is the subset of *clickhouse.ClickhouseWriter that the
	// master needs at runtime: a query-event submit, an aggregated-bucket
	// flush. The interface keeps the field testable (a fake satisfying just
	// these two methods is enough for unit tests) while the real
	// *clickhouse.ClickhouseWriter satisfies it via existing methods.
	ClickhouseSink interface {
		Submit(qT *pbm.TotalQueryData)
		FlushAggregates(ctx context.Context, buckets []clickhouse.AggregatedBucket) (int, error)
	}

	BackgroundStorage struct {
		l                  *zap.SugaredLogger
		SessionStorage     *gp.SessionsStorage
		AggStorage         *storage.AggregatedStorage
		RQStorage          *storage.RunningQueriesStorage
		procfsStorage      *storage.ProcfsStorage
		statActivityLister statActivityLister
		chWriter           ClickhouseSink

		// segRefreshTimes tracks the last successful data-gathering time per segment hostname.
		segRefreshMu    sync.RWMutex
		segRefreshTimes map[string]time.Time
	}
)

var (
	segChan chan segmentAddr
)

func NewBackgroundStorage(l *zap.SugaredLogger,
	sessionStorage *gp.SessionsStorage,
	rqStorage *storage.RunningQueriesStorage,
	aggStorage *storage.AggregatedStorage,
	procfsStorage *storage.ProcfsStorage,
	sActivityLister statActivityLister) *BackgroundStorage {
	return &BackgroundStorage{
		l:                  l,
		SessionStorage:     sessionStorage,
		AggStorage:         aggStorage,
		RQStorage:          rqStorage,
		procfsStorage:      procfsStorage,
		statActivityLister: sActivityLister,
		segRefreshTimes:    make(map[string]time.Time),
	}
}

func (bs *BackgroundStorage) SendSegmentRefreshMessages(ctx context.Context, pullRateSec float64, configCacheDurability time.Duration, portn uint32, customSegmentList *config.SegmentList) error {
	durationBetweenLoop := time.Duration(pullRateSec * float64(time.Second))
	for {
		start := time.Now()
		select {
		case <-ctx.Done():
			bs.l.Warn("Done SendSegmentRefreshMessages")
			return fmt.Errorf("done context with %w", ctx.Err())
		// add segments to channel
		default:
			var segConfig gp.GpSegmentsConfiguration
			if customSegmentList == nil {
				bs.l.Debugf("Start refresh segment config")
				ctxTimeout, ctxCancel := context.WithTimeout(ctx, time.Duration(float64(time.Second)*pullRateSec))
				var err error
				segConfig, err = gp.GetSegmentConfig(ctxTimeout, configCacheDurability)
				ctxCancel()
				if err != nil {
					bs.l.Errorf("fail to get segment config %v", err)
					return err
				}
				bs.l.Debugf("Finish refresh segment config")
			} else {
				bs.l.Debugf("Custom segments list - use them")
				segConfig = make([]*gp.GpSegmentConfiguration, 0)
				for _, customSegment := range *customSegmentList {
					segC := &gp.GpSegmentConfiguration{
						DBID:     customSegment.DBID,
						Content:  customSegment.Content,
						Hostname: customSegment.Hostname,
						Port:     customSegment.PortN,
						Status:   "u",
					}
					segConfig = append(segConfig, segC)
				}
			}
			// empty channel
			localSegMap := make(segmentMap)
		L:
			for {
				select {
				case data := <-segChan:
					bs.l.Debugf("got unprocessed host %v from channel", data)
					localSegMap[data.hostname] = &data
				default:
					break L
				}
			}

			// make list of segments
			segProcessed := make(map[string]bool, 0)
			segments := make([]*segmentAddr, 0)
			for _, segHost := range segConfig {
				if segHost.Status != "u" || segHost.Role == "m" || segHost.Content == -1 {
					continue
				}
				if _, alreadyP := segProcessed[segHost.Hostname]; alreadyP {
					continue
				}
				segProcessed[segHost.Hostname] = true
				segAddr, ok := localSegMap[segHost.Hostname]
				if ok {
					segments = append(segments, segAddr)
					continue
				}
				segments = append(segments, &segmentAddr{hostname: segHost.Hostname, port: portn, queueTime: time.Now()})
			}
			sort.Slice(segments, func(i, j int) bool { return segments[i].queueTime.Before(segments[j].queueTime) })

			// Prune segRefreshTimes to stay in sync with gp_segment_configuration.
			// Hosts that no longer run segments are removed so their stale timestamps
			// don't block MinSegmentRefreshTime from advancing.
			bs.syncSegmentHosts(segProcessed)

			// add new hosts to channel
			for _, segmentO := range segments {
				segmentI := segmentO
				// pnce again check context
				select {
				case <-ctx.Done():
					bs.l.Warn("Done SendSegmentRefreshMessages")
					return fmt.Errorf("done context with %w", ctx.Err())
				// add segments to channel
				default:
					segChan <- *segmentI
				}
			}
		}

		// sleep to the next iteration
		elapsed := time.Since(start)
		if elapsed < durationBetweenLoop {
			err := utils.Delay(ctx, durationBetweenLoop-elapsed)
			if err != nil {
				return err
			}
		}
	}
}

func (bs *BackgroundStorage) processSegment(ctx context.Context, segmentName string, portn uint32, segConnectTimeoutSec float64, segGetTimeout float64, msgSize int) {
	bs.l.Debugf("Start processing %v", segmentName)
	start := time.Now()
	grpcConn, err := getGrpcClientConnection(ctx, segmentName, portn, segConnectTimeoutSec)
	if err != nil {
		bs.l.Infof("Failed to get data from %s with error %v", segmentName, err)
		return
	}
	cGet := pb.NewGetQueryInfoClient(grpcConn)
	ctxTimeout, ctxCancel := context.WithTimeout(ctx, time.Second*time.Duration(segGetTimeout))
	defer ctxCancel()
	maxSizeOption := grpc.MaxCallRecvMsgSize(msgSize)
	rGet, errGet := cGet.GetMetricQueries(ctxTimeout, &pb.GetQueriesInfoReq{ClearSent: true}, maxSizeOption)
	if errGet != nil {
		bs.l.Infof("could not perform req: %v %v", errGet, segmentName)
		return
	}
	err = bs.RQStorage.MergeSegmentData(rGet)
	if err != nil {
		bs.l.Infof("Failed to merge data for %s with error %v", segmentName, err)
		return
	}
	bs.recordSegmentRefresh(segmentName)
	if metrics.YagpccMetrics != nil {
		metrics.YagpccMetrics.HandleLatencies.With(map[string]string{"method": "processSegment"}).Observe(time.Since(start).Seconds())
	}
	bs.l.Debugf("Finish processing %v", segmentName)
}

// recordSegmentRefresh stores the current time as the last successful refresh for the given segment.
func (bs *BackgroundStorage) recordSegmentRefresh(hostname string) {
	bs.segRefreshMu.Lock()
	bs.segRefreshTimes[hostname] = time.Now()
	bs.segRefreshMu.Unlock()
}

// MinSegmentRefreshTime returns the earliest last-refresh time across all known segments.
// If no segments have been refreshed yet, it returns the zero time.
func (bs *BackgroundStorage) MinSegmentRefreshTime() time.Time {
	bs.segRefreshMu.RLock()
	defer bs.segRefreshMu.RUnlock()
	var minTime time.Time
	initialized := false
	for _, t := range bs.segRefreshTimes {
		if !initialized || t.Before(minTime) {
			minTime = t
			initialized = true
		}
	}
	return minTime
}

// syncSegmentHosts removes entries from segRefreshTimes for hosts that are no
// longer present in the active segment configuration. This keeps the map in sync
// with gp_segment_configuration so that hosts which no longer run segments don't
// hold back MinSegmentRefreshTime with stale timestamps.
func (bs *BackgroundStorage) syncSegmentHosts(activeHosts map[string]bool) {
	bs.segRefreshMu.Lock()
	defer bs.segRefreshMu.Unlock()
	for host := range bs.segRefreshTimes {
		if !activeHosts[host] {
			delete(bs.segRefreshTimes, host)
		}
	}
	for host := range activeHosts {
		if _, ok := bs.segRefreshTimes[host]; !ok {
			// Zero time marks a newly active host that has not refreshed yet.
			bs.segRefreshTimes[host] = time.Time{}
		}
	}
}

func (bs *BackgroundStorage) launchSegmentPullers(ctx context.Context, nPullers uint32, segConnectTimeoutSec float64, segGetTimeout float64, msgSize int) {
	for i := 0; i < int(nPullers); i++ {
		go func() {
			for {
				select {
				case seg := <-segChan:
					bs.processSegment(ctx, seg.hostname, seg.port, segConnectTimeoutSec, segGetTimeout, msgSize)
				case <-ctx.Done():
					bs.l.Warn("Done launchSegmentPullers")
					return
				}
			}
		}()
	}
}

func (bs *BackgroundStorage) launchArchiveWriters(ctx context.Context,
	archConfig config.ArchiverConfigType,
	queryChan chan *pbm.QueryStatWrite,
	sessChan chan *gp.SessionDataWrite,
	segChan chan *pbm.SegmentMetricsWrite,
	maxFileSize int64,
) error {
	fileSession, err := NewRotateWriter(archConfig.SessionsFile, maxFileSize)
	if err != nil {
		bs.l.Errorf("could not create output file %v with error %v", archConfig.SessionsFile, err)
		return err
	}
	fileQuery, err := NewRotateWriter(archConfig.QueriesFile, maxFileSize)
	if err != nil {
		bs.l.Errorf("could not create output file %v with error %v", archConfig.QueriesFile, err)
		return err
	}
	fileSegments, err := NewRotateWriter(archConfig.SegmentsFile, maxFileSize)
	if err != nil {
		bs.l.Errorf("could not create output file %v with error %v", archConfig.SegmentsFile, err)
		return err
	}
	go StoreQuery(ctx, bs.l, queryChan, fileQuery)
	go StoreSessions(ctx, bs.l, sessChan, fileSession)
	go StoreSegmensMetrics(ctx, bs.l, segChan, fileSegments)
	return nil
}

func (bs *BackgroundStorage) launchArchivers(ctx context.Context,
	qDurationSec uint32,
	nProcesses uint32,
	clusterID string,
	archChan chan *EndedQuery,
	queryChan chan *pbm.QueryStatWrite,
	segChan chan *pbm.SegmentMetricsWrite,
	hostname string,
) {
	qDuration := time.Second * time.Duration(qDurationSec)
	for i := uint32(0); i < nProcesses; i++ {
		go bs.ArchiveOrAggregate(ctx, qDuration, clusterID, archChan, queryChan, segChan, hostname)
	}
}

func (bs *BackgroundStorage) SendSessionMetrics(ctx context.Context, sessChan chan *gp.SessionDataWrite, sessionSendMetricInterval time.Duration, clusterID string, hostname string) error {
	ticker := time.NewTicker(sessionSendMetricInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			bs.l.Warn("Done SendSessionMetrics")
			return fmt.Errorf("done context with %w", ctx.Err())
		case <-ticker.C:
			currTime := time.Now()
			// send session stat
			sMap := bs.SessionStorage.GetSessions()
			for keySO, valSO := range sMap {
				valSO.SessionLock.RLock()
				notSystemSession := gp.NotSystemSession(valSO)
				valSO.SessionLock.RUnlock()
				if notSystemSession {
					sessData, err := bs.SessionStorage.GetSessionDataForWrite(clusterID, hostname, keySO, valSO, pbm.RunningQueryType_RQT_LAST)
					if err != nil {
						bs.l.Errorf("got error in getting session data for write %v", err)
						continue
					}
					// once again check context
					select {
					case <-ctx.Done():
						bs.l.Warn("Done SendSessionMetrics")
						// unlock session and exit
						return fmt.Errorf("done context with %w", ctx.Err())
					default:
						bs.l.Debugf("sent %v", *sessData)
						sessChan <- sessData
					}
				}
			}

			if metrics.YagpccMetrics != nil {
				metrics.YagpccMetrics.TotalSessions.Set(float64(bs.SessionStorage.SessionsCount()))
			}
			if metrics.YagpccMetrics != nil {
				metrics.YagpccMetrics.HandleLatencies.With(map[string]string{"method": "SendSessionMetrics"}).Observe(time.Since(currTime).Seconds())
			}
		}
	}
}

// archiveReason explains why a completed query is (or is not yet) being archived.
// It is reported so the archive consumer and Prometheus can distinguish a fully
// collected query from one that was archived with possibly partial segment data.
type archiveReason int

const (
	reasonNone          archiveReason = iota // not archiving yet (running or still collecting)
	reasonAllSegments                        // archived: every segment reported after query end
	reasonTimeout                            // archived: segment timeout fired, data may be partial
	reasonSessionFailed                      // archived: session disappeared before completion
)

// queryCompleted decides whether a query can be archived and, if so, why.
// It returns (decision, reason) where decision is:
//
//	0 - keep waiting (still running, or completed but segments not yet collected)
//	1 - archive the query
//
// The reason is meaningful only when decision == 1 (reasonAllSegments or reasonTimeout).
// The session-failed case is handled by the caller (it archives with reasonSessionFailed).
// Otherwise it is reasonNone.
func (bs *BackgroundStorage) queryCompleted(qKey *storage.QueryKey, qVal *storage.RunningQuery, segmentGetTimeoutSec float64) (int, archiveReason) {
	now := time.Now()
	qVal.QueryLock.RLock()
	qCompleted := qVal.Completed
	qValEnded := qVal.QueryEnd
	qVal.QueryLock.RUnlock()
	if qCompleted {
		if now.Sub(qValEnded) > time.Duration(segmentGetTimeoutSec*float64(time.Second)) {
			bs.l.Debugf("Query %v completed and exceeded segment timeout", *qKey)
			return 1, reasonTimeout
		}
		// Check if all segments have been refreshed since the query ended.
		// If the minimum segment refresh time is after the query end, every segment
		// has had a chance to report metrics for this query.
		minRefresh := bs.MinSegmentRefreshTime()
		if !minRefresh.IsZero() && minRefresh.After(qValEnded) {
			bs.l.Debugf("Query %v completed, all segments refreshed after query end", *qKey)
			return 1, reasonAllSegments
		}
		bs.l.Debugf("Query %v completed, waiting for segment refresh", *qKey)
		return 0, reasonNone
	}

	return 0, reasonNone
}

// countArchived increments the archival-reason counter for a query that was just
// sent to the archiver. Called exactly once per archived query (guarded by the
// MarkSessionSent check) so pinned queries are not double-counted.
func countArchived(reason archiveReason) {
	if metrics.YagpccMetrics == nil {
		return
	}
	switch reason {
	case reasonAllSegments:
		metrics.YagpccMetrics.QueriesArchivedComplete.Inc()
	case reasonTimeout:
		metrics.YagpccMetrics.QueriesArchivedTimeout.Inc()
	case reasonSessionFailed:
		metrics.YagpccMetrics.QueriesArchivedSessionFailed.Inc()
	}
}

// forwardGCQueries reads GC-evicted completed queries from gcChan and forwards
// them to archChan so they get archived instead of being silently dropped.
// On context cancellation it calls rqStorage.CloseArchChan() to signal in-flight
// GC sender goroutines and drains the channel so they don't block forever.
func forwardGCQueries(ctx context.Context, l *zap.SugaredLogger, rqStorage *storage.RunningQueriesStorage, gcChan chan *storage.GCQuery, archChan chan *EndedQuery) {
	defer func() {
		// Signal GC senders to stop and drain the channel so no goroutine hangs.
		rqStorage.CloseArchChan()
		l.Warn("Done forwardGCQueries")
	}()
	for {
		select {
		case gcQ := <-gcChan:
			l.Debugf("Forwarding GC-evicted query %v to archive", *gcQ.QKey)
			select {
			case archChan <- &EndedQuery{QKey: gcQ.QKey, QVal: gcQ.QVal}:
			case <-ctx.Done():
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

func (bs *BackgroundStorage) TryRefreshSessionsFromGP(
	ctx context.Context,
	clearDeletedSessions bool,
) error {
	newSesList, err := bs.statActivityLister.List(ctx)
	if err != nil {
		return fmt.Errorf("error getting sessions: %w", err)
	}

	bs.l.Debugf("got %v sessions from gp", len(newSesList))
	err = bs.SessionStorage.RefreshSessionList(newSesList, clearDeletedSessions)
	if err != nil {
		return fmt.Errorf("error refreshing sessions: %w", err)
	}

	err = bs.SessionStorage.RecalculateProcfsUsage()
	if err != nil {
		// do not stop working, just store error
		bs.l.Warnf("error recalculating procfs usage: %v", err)
	}
	bs.l.Debugf("refreshed session list")
	return nil
}

func (bs *BackgroundStorage) ClearCompletedQueries(ctx context.Context,
	archChan chan *EndedQuery,
	segmentGetTimeoutSec float64,
	clearDeletedSessions bool,
) error {
	// send queries to archive
	var keysToDelete []*storage.QueryKey
	rQueries := bs.RQStorage.GetQueries()
	for qKeyO, qValO := range rQueries {
		qKeyI := qKeyO
		qValI := qValO
		sessKey := gp.SessionKey{
			SessID: int(qKeyI.Ssid),
		}
		valS, okS := bs.SessionStorage.GetSession(sessKey)
		var qCompleted int
		var reason archiveReason
		if !okS {
			qCompleted = 2
			reason = reasonSessionFailed
			// should archive query
		} else {
			qCompleted, reason = bs.queryCompleted(&qKeyI, qValI, segmentGetTimeoutSec)
		}
		// qCompleted == 1 - archive query, but check if session has links on it
		// qCompleted == 2 - delete query permanently, not wait anymore
		if (qCompleted == 1) || (qCompleted == 2) {
			bs.l.Debugf("Archive and delete query %v", qKeyI)
			if qValI.QueryEnd.IsZero() {
				// session was deleted and we do not get QUERY_DONE message
				qValI.QueryLock.Lock()
				qValI.QueryEnd = time.Now()
				qValI.QueryStatus = int32(pbc.QueryStatus_QUERY_STATUS_ERROR)
				qValI.QueryMessage = "Session failed"
				qValI.QueryLock.Unlock()
			}
			// check if we could archive query
			canBeDeleted := true
			lastQuery := int32(-1)
			if okS {
				valS.SessionLock.RLock()
				lastQuery = valS.SessionData.GetLastQuery()
				valS.SessionLock.RUnlock()
			}
			if (qCompleted == 1) && lastQuery == qKeyI.Ccnt {
				bs.l.Debugf("Query cannot be deleted because have links in session %v", qKeyI)
				canBeDeleted = false
			}
			// once again check context and archive query
			select {
			case <-ctx.Done():
				bs.l.Warn("Done ClearCompletedQueries")
				return fmt.Errorf("done context with %w", ctx.Err())
			default:
				// prevent double-sent session market as LastQuery since it is not deleted until new query start execution
				qValI.QueryLock.RLock()
				markSent := qValI.MarkSessionSent
				qValI.QueryLock.RUnlock()
				if !markSent {
					archChan <- &EndedQuery{QKey: &qKeyI, QVal: qValI}
					// Count the archived query by reason exactly once. Pinned
					// queries (canBeDeleted == false) re-enter this loop on later
					// cycles but are skipped here because MarkSessionSent is set.
					countArchived(reason)
				}
				qValI.QueryLock.Lock()
				qValI.MarkSessionSent = true
				qValI.QueryLock.Unlock()
			}
			if canBeDeleted {
				if okS {
					valS.SessionLock.Lock()
					// decrease queries counter
					valS.RefCounter--
					valS.SessionLock.Unlock()
				}
				keysToDelete = append(keysToDelete, &qKeyI)
			}
		}
	}

	rqSize := bs.RQStorage.DeleteQueries(keysToDelete)
	if metrics.YagpccMetrics != nil {
		metrics.YagpccMetrics.TotalQueries.Set(float64(rqSize))
	}
	return nil
}

func (bs *BackgroundStorage) RefreshSessions(ctx context.Context, sessionRefreshInterval time.Duration, clearDeletedSessions bool) error {
	ticker := time.NewTicker(sessionRefreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			bs.l.Warn("Done RefreshSessions")
			return fmt.Errorf("done context with %w", ctx.Err())
		case <-ticker.C:
			currTime := time.Now()
			bs.l.Info("Refresh session List")
			err := bs.TryRefreshSessionsFromGP(ctx, clearDeletedSessions)
			if err != nil {
				bs.l.Errorf("fail to refresh session list %v", err)
				return err
			}
			if metrics.YagpccMetrics != nil {
				metrics.YagpccMetrics.HandleLatencies.With(map[string]string{"method": "RefreshSessions"}).Observe(time.Since(currTime).Seconds())
			}
		}
	}
}

func (bs *BackgroundStorage) RefreshQueries(ctx context.Context,
	archChan chan *EndedQuery,
	queriesRefreshInterval time.Duration,
	segmentGetTimeoutSec float64,
	clearDeletedSessions bool) error {
	ticker := time.NewTicker(queriesRefreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			bs.l.Warn("Done RefreshQueries")
			return fmt.Errorf("done context with %w", ctx.Err())
		case <-ticker.C:
			currTime := time.Now()
			bs.l.Debug("Clear queries list")
			err := bs.ClearCompletedQueries(
				ctx,
				archChan,
				segmentGetTimeoutSec,
				clearDeletedSessions,
			)
			if err != nil {
				bs.l.Errorf("fail to clear queries list %v", err)
				return err
			}
			if metrics.YagpccMetrics != nil {
				metrics.YagpccMetrics.HandleLatencies.With(map[string]string{"method": "RefreshQueries"}).Observe(time.Since(currTime).Seconds())
			}
		}
	}
}

func (bs *BackgroundStorage) RefreshProcfs(ctx context.Context, procfsRefreshInterval time.Duration, nPullers int, portn uint32, msgSize int) error {
	if procfsRefreshInterval <= 0 {
		return fmt.Errorf("procfsRefreshInterval must be > 0, got %v", procfsRefreshInterval)
	}
	ticker := time.NewTicker(procfsRefreshInterval)
	defer ticker.Stop()

	procfsGatherer := NewProcfsGatherStorage(bs.l, bs.statActivityLister)

	for {
		select {
		case <-ctx.Done():
			bs.l.Warn("Done RefreshProcfs")
			return fmt.Errorf("done context with %w", ctx.Err())
		case <-ticker.C:
			currTime := time.Now()
			bs.l.Debugf("Refresh procfs stat %v", currTime)
			result, err := procfsGatherer.GatherProcfsStat(ctx, nPullers, portn, procfsRefreshInterval, msgSize)
			if err != nil {
				// just log error, do not fail the whole service
				bs.l.Errorf("fail to get procfs data %v", err)
				continue
			}
			bs.procfsStorage.RegisterProcfsStat(currTime, result)
			// measure only successful latencies
			if metrics.YagpccMetrics != nil {
				metrics.YagpccMetrics.HandleLatencies.With(map[string]string{"method": "RefreshProcfs"}).Observe(time.Since(currTime).Seconds())
			}
		}
	}
}

func InitConnection(ctx context.Context, l *zap.SugaredLogger, cfg *config.Config, firstTry bool) error {
	tries := int(cfg.MasterConnectionTries)
	if firstTry {
		tries = int(cfg.MasterConnectionFirstTries)
	}
	err := gp.Init(ctx, l, &cfg.MasterConnection, tries)
	if err != nil && !cfg.IgnoreDatabaseError {
		return err
	}
	return nil
}

// SetClickhouseWriter wires an optional ClickHouse sink onto the background
// storage. Passing nil leaves the sink disabled. Calls go through ClickhouseSink
// (the local interface) so tests can inject a stub instead of a real
// clickhouse.ClickhouseWriter.
func (bs *BackgroundStorage) SetClickhouseWriter(w ClickhouseSink) {
	bs.chWriter = w
}

// ClickhouseWriter returns the configured sink (may be nil). Used by app.go
// to drive Run/Close on the underlying writer without exposing the field.
func (bs *BackgroundStorage) ClickhouseWriter() ClickhouseSink {
	return bs.chWriter
}

// submitToClickhouse forwards a finalised query record to the CH sink when
// configured. Called from ArchiveOrAggregate for both archived and aggregated
// queries — short queries are filtered out by the sink itself via
// min_duration_ms, so this entry point passes everything through.
func (bs *BackgroundStorage) submitToClickhouse(qT *pbm.TotalQueryData) {
	if bs.chWriter == nil {
		return
	}
	bs.chWriter.Submit(qT)
}

// aggSnapshotsToClickhouse converts the storage-level snapshot batch into
// the clickhouse.AggregatedBucket shape and forwards it to the sink. Wired
// into AggregatedStorage via WithCycleHook in app.go when the sink is
// enabled. Errors are logged but otherwise swallowed: at-most-once is the
// CH contract for v1, and the JSON archiver path has already accepted the
// same batch via queryChan.
func (bs *BackgroundStorage) aggSnapshotsToClickhouse(ctx context.Context, snapshots []storage.AggBucketSnapshot) {
	if bs.chWriter == nil || len(snapshots) == 0 {
		return
	}
	buckets := make([]clickhouse.AggregatedBucket, 0, len(snapshots))
	for _, s := range snapshots {
		buckets = append(buckets, snapshotToBucket(s))
	}
	if _, err := bs.chWriter.FlushAggregates(ctx, buckets); err != nil {
		bs.l.Warnf("clickhouse aggregated flush failed: %v", err)
	}
}

// snapshotToBucket converts one mature aggregated bucket from storage
// representation (proto-derived nanoseconds, raw GPMetrics) into the
// clickhouse.AggregatedBucket shape declared by 0001_init.up.sql. AggregatedMetrics
// times in the proto are float64(time.Duration) — i.e. nanoseconds — so we
// divide by 1e6 to get the millisecond-flavoured columns the DDL expects.
func snapshotToBucket(s storage.AggBucketSnapshot) clickhouse.AggregatedBucket {
	bucket := clickhouse.AggregatedBucket{
		BucketTime:    s.Key.StartTime,
		QueryID:       s.Key.QueryID,
		PlanID:        s.Key.PlanID,
		User:          s.Key.UserName,
		Database:      s.Key.DatabaseName,
		ResourceGroup: s.Key.Rsgname,
		Executions:    uint64(s.Calls),
	}
	if m := s.TotalMetrics; m != nil {
		if sys := m.SystemStat; sys != nil {
			bucket.TotalCPUSec = sys.UserTimeSeconds + sys.KernelTimeSeconds
			bucket.TotalRunningSec = sys.RunningTimeSeconds
			bucket.TotalRSSBytes = sys.Rss
			bucket.TotalIOBytes = sys.ReadBytes + sys.WriteBytes
		}
		if i := m.Instrumentation; i != nil {
			bucket.TotalNTuples = i.Ntuples
		}
	}
	const nsPerMs = 1_000_000.0
	bucket.AvgDurationMs = s.MeanTimeNs / nsPerMs
	if s.MaxTimeNs > 0 {
		bucket.MaxDurationMs = uint64(s.MaxTimeNs / nsPerMs)
	}
	return bucket
}

// AggregatedSnapshotHook returns a storage cycle hook that forwards every
// drained batch of aggregated buckets to the CH sink. Returns nil when the
// sink is not configured so callers can skip installing the hook entirely.
func (bs *BackgroundStorage) AggregatedSnapshotHook() storage.AggCycleHook {
	if bs.chWriter == nil {
		return nil
	}
	return bs.aggSnapshotsToClickhouse
}

func InitBG(
	ctx context.Context,
	l *zap.SugaredLogger,
	masterSentinel masterSentinel,
	cfg *config.Config,
	backgroundStorage *BackgroundStorage,
) error {
	l.Info("Start init BG processes")

	errG, ctxI := errgroup.WithContext(ctx)
	hostname, err := os.Hostname()
	if err != nil {
		return err
	}
	segChan = make(chan segmentAddr, segChanSize)
	archChan := make(chan *EndedQuery, cfg.ArchiverConfig.ArchiverQueueSize)
	queryChan := make(chan *pbm.QueryStatWrite, cfg.ArchiverConfig.QueriesQueueSize)
	sessChan := make(chan *gp.SessionDataWrite, cfg.ArchiverConfig.SessionsQueueSize)
	segMetricsChan := make(chan *pbm.SegmentMetricsWrite, cfg.ArchiverConfig.SegmentsQueueSize)

	// Create GC archive channel and wire it to the storage so that
	// garbage-collected completed queries are forwarded to the archiver
	// instead of being silently dropped.
	gcArchChan := make(chan *storage.GCQuery, cfg.ArchiverConfig.ArchiverQueueSize)
	backgroundStorage.RQStorage.SetArchChan(gcArchChan)
	go forwardGCQueries(ctxI, l, backgroundStorage.RQStorage, gcArchChan, archChan)

	errG.Go(func() error {
		if sentinelErr := masterSentinel.RunUntilIsMaster(ctxI); sentinelErr != nil {
			l.Errorf("the current instance is not considered to be the active master anymore due to an error: %s", sentinelErr.Error())
			return sentinelErr
		}

		l.Warnf("the current instance is not considered to be the active master anymore")
		return nil
	})

	if backgroundStorage.statActivityLister == nil {
		return fmt.Errorf("stat activity lister is nil")
	}
	if err = backgroundStorage.statActivityLister.Start(ctx); err != nil {
		return fmt.Errorf("error starting stat activity lister: %w", err)
	}

	errG.Go(func() error {
		bgErr := backgroundStorage.SendSegmentRefreshMessages(ctxI,
			cfg.SegmentPullRateSec,
			time.Duration(cfg.ConfigCacheDurabilitySec*float64(time.Second)),
			cfg.ListenPort,
			cfg.CustomSegmentList,
		)
		l.Errorf("got %v in segment refresh", bgErr)
		return bgErr
	},
	)
	backgroundStorage.launchSegmentPullers(ctxI, cfg.SegmentPullThreads, cfg.SegmentConnectTimeoutSec, cfg.SegmentGetTimeoutSec, int(cfg.MaxMessageSize))
	backgroundStorage.launchArchivers(ctxI, cfg.MinimumQueryDurationSec, cfg.ArchiverConfig.ArciverProcesses, cfg.ClusterID, archChan, queryChan, segMetricsChan, hostname)
	err = backgroundStorage.launchArchiveWriters(ctxI, cfg.ArchiverConfig, queryChan, sessChan, segMetricsChan, cfg.ArchiverConfig.MaxFileSize)
	if err != nil {
		return err
	}
	errG.Go(func() error {
		bgErr := backgroundStorage.AggStorage.ArchiveAggQuery(ctxI, queryChan, cfg.ClusterID, hostname)
		l.Errorf("got %v in archive agg query", bgErr)
		return bgErr
	},
	)
	errG.Go(func() error {
		bgErr := backgroundStorage.SendSessionMetrics(ctxI, sessChan, cfg.SessionSendMetricInterval, cfg.ClusterID, hostname)
		l.Errorf("got %v in send session metrics", bgErr)
		return bgErr
	},
	)
	errG.Go(func() error {
		bgErr := backgroundStorage.RefreshSessions(ctxI, cfg.SessionRefreshInterval, cfg.ClearDeletedSessions)
		l.Errorf("got %v refresh session and queries", bgErr)
		return bgErr
	},
	)
	errG.Go(func() error {
		bgErr := backgroundStorage.RefreshQueries(ctxI, archChan, cfg.QueriesRefreshInterval, cfg.SegmentGetTimeoutSec, cfg.ClearDeletedSessions)
		l.Errorf("got %v refresh session and queries", bgErr)
		return bgErr
	},
	)
	if cfg.ProcfsEnabled {
		errG.Go(func() error {
			bgErr := backgroundStorage.RefreshProcfs(ctxI, cfg.ProcfsRefreshInterval, int(cfg.SegmentPullThreads), cfg.ListenPort, int(cfg.MaxMessageSize))
			l.Errorf("got %v in RefreshProcfs", bgErr)
			return bgErr
		},
		)
	} else {
		l.Info("Procfs gathering is disabled")
	}
	err = errG.Wait()
	if err != nil {
		backgroundStorage.statActivityLister.Stop()
		l.Errorf("Fail in background precesses - done work with %v", err)
		return err
	}
	l.Info("Done work")
	return nil
}
