package master

import (
	"context"
	"fmt"
	"net"
	"os"
	"sort"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"

	pbm "github.com/open-gpdb/yagpcc/api/proto/agent_master"
	pb "github.com/open-gpdb/yagpcc/api/proto/agent_segment"
	pbc "github.com/open-gpdb/yagpcc/api/proto/common"
	"github.com/open-gpdb/yagpcc/internal/config"
	"github.com/open-gpdb/yagpcc/internal/gp"
	"github.com/open-gpdb/yagpcc/internal/metrics"
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

	BackgroundStorage struct {
		l              *zap.SugaredLogger
		SessionStorage *gp.SessionsStorage
		AggStorage     *storage.AggregatedStorage
		RQStorage      *storage.RunningQueriesStorage
	}
)

var (
	segChan           chan segmentAddr
	segConnections    map[string]*grpc.ClientConn = make(map[string]*grpc.ClientConn)
	segConnectionLock sync.Mutex
	segCount          int
	segCountLock      sync.Mutex
)

func getSegAddr(hostname string, portn uint32) string {
	return fmt.Sprintf("%s:%d", hostname, portn)
}

func getGrpcClientConnection(ctx context.Context, hostname string, portn uint32, segConnectTimeoutSec float64) (*grpc.ClientConn, error) {
	var err error
	segConnectionLock.Lock()
	defer segConnectionLock.Unlock()
	conn, ok := segConnections[hostname]
	if ok {
		if conn.GetState() == connectivity.Ready {
			return conn, nil
		}
	}
	ctxT, ctxTCancel := context.WithTimeout(ctx, time.Second*time.Duration(segConnectTimeoutSec))
	defer ctxTCancel()
	if portn > 0 {
		conn, err = grpc.DialContext(ctxT, getSegAddr(hostname, portn), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
		if err != nil {
			return nil, err
		}
	} else {
		dialer := func(addr string, t time.Duration) (net.Conn, error) {
			return net.Dial("unix", addr)
		}
		conn, err = grpc.DialContext(ctxT, hostname, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock(), grpc.WithDialer(dialer))
		if err != nil {
			return nil, err
		}
	}
	segConnections[hostname] = conn
	return conn, nil
}

func (bs *BackgroundStorage) SendSegmentRefreshMessages(ctx context.Context, pullRateSec float64, configCacheDurability time.Duration, portn uint32, customSegmentList *config.SegmentList) error {

	durationBetweenLoop := time.Second * time.Duration(pullRateSec)
	for {
		start := time.Now()
		select {
		case <-ctx.Done():
			bs.l.Warn("Done SendSegmentRefreshMessages")
			return fmt.Errorf("done context with %v", ctx.Err())
		// add segments to channel
		default:
			var segConfig gp.GpSegmentsConfiguration
			if customSegmentList == nil {
				bs.l.Debugf("Start refresh segment config")
				ctxTimeout, ctxCancel := context.WithTimeout(ctx, time.Duration(float64(time.Second)*pullRateSec))
				defer ctxCancel()
				var err error
				segConfig, err = gp.GetSegmentConfig(ctxTimeout, configCacheDurability)
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
			segCountLock.Lock()
			segCount = len(segConfig)
			segCountLock.Unlock()
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

			// add new hosts to channel
			for _, segmentO := range segments {
				segmentI := segmentO
				// pnce again check context
				select {
				case <-ctx.Done():
					bs.l.Warn("Done SendSegmentRefreshMessages")
					return fmt.Errorf("done context with %v", ctx.Err())
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
	if metrics.YagpccMetrics != nil {
		metrics.YagpccMetrics.HandleLatencies.With(map[string]string{"method": "processSegment"}).Observe(time.Since(start).Seconds())
	}
	bs.l.Debugf("Finish processing %v", segmentName)
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
	qDuration := time.Duration(time.Second * time.Duration(qDurationSec))
	for i := uint32(0); i < nProcesses; i++ {
		go bs.ArchiveOrAggregate(ctx, qDuration, clusterID, archChan, queryChan, segChan, hostname)
	}
}

func (bs *BackgroundStorage) SendSessionMetrics(ctx context.Context, sessChan chan *gp.SessionDataWrite, sessionSendMetricInterval time.Duration, clusterID string, hostname string) error {
	for {
		currTime := time.Now().Truncate(time.Second)
		nextTime := currTime.Add(sessionSendMetricInterval)
		select {
		case <-ctx.Done():
			bs.l.Warn("Done SendSessionMetrics")
			return fmt.Errorf("done context with %v", ctx.Err())
		default:
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
						return fmt.Errorf("done context with %v", ctx.Err())
					default:
						bs.l.Debugf("sent %v", *sessData)
						sessChan <- sessData
					}
				}

			}

			if metrics.YagpccMetrics != nil {
				metrics.YagpccMetrics.TotalSessions.Set(float64(bs.SessionStorage.SessionsCount()))
			}
		}
		if metrics.YagpccMetrics != nil {
			metrics.YagpccMetrics.HandleLatencies.With(map[string]string{"method": "SendSessionMetrics"}).Observe(time.Since(currTime).Seconds())
		}
		time.Sleep(time.Until(nextTime))
	}
}

func queryCompleted(qKey *storage.QueryKey, qVal *storage.RunningQuery, segmentGetTimeoutSec float64, l *zap.SugaredLogger) int {
	now := time.Now()
	qVal.QueryLock.RLock()
	qCompleted := qVal.Completed
	qLenNodes := len(qVal.SegmentNodes)
	qValEnded := qVal.QueryEnd
	qVal.QueryLock.RUnlock()
	if qCompleted {
		segCountLock.Lock()
		sCount := segCount
		segCountLock.Unlock()
		if qLenNodes >= sCount {
			l.Debugf("Query %v comleted and got metrics from all segments", *qKey)
			return 1
		}
		if now.Sub(qValEnded) > time.Duration(time.Second*time.Duration(segmentGetTimeoutSec)) {
			l.Debugf("Query %v comleted and exceeded segment timeout", *qKey)
			return 1
		}
	}

	return 0
}

func (bs *BackgroundStorage) TryRefreshSessionsFromGP(
	ctx context.Context,
	statActivityLister statActivityLister,
	clearDeletedSessions bool,
) error {
	newSesList, err := statActivityLister.List(ctx)
	if err != nil {
		return fmt.Errorf("error getting sessions: %w", err)
	}

	bs.l.Debugf("got %v sessions from gp", len(newSesList))
	err = bs.SessionStorage.RefreshSessionList(bs.l, newSesList, clearDeletedSessions)
	if err != nil {
		return fmt.Errorf("error refreshing sessions: %w", err)
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
		if !okS {
			qCompleted = 2
			// should archive query
		} else {
			qCompleted = queryCompleted(&qKeyI, qValI, segmentGetTimeoutSec, bs.l)
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
				return fmt.Errorf("done context with %v", ctx.Err())
			default:
				// prevent double-sent session market as LastQuery since it is not deleted until new query start execution
				qValI.QueryLock.RLock()
				markSent := qValI.MarkSessionSent
				qValI.QueryLock.RUnlock()
				if !markSent {
					archChan <- &EndedQuery{QKey: &qKeyI, QVal: qValI}
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

func (bs *BackgroundStorage) RefreshSessions(ctx context.Context,
	statActivityLister statActivityLister,
	sessionRefreshInterval time.Duration,
	clearDeletedSessions bool) error {
	for {
		currTime := time.Now()
		nextTime := currTime.Truncate(sessionRefreshInterval).Add(sessionRefreshInterval)
		select {
		case <-ctx.Done():
			bs.l.Warn("Done RefreshSessions")
			return fmt.Errorf("done context with %v", ctx.Err())
		default:
			bs.l.Info("Refresh session List")
			err := bs.TryRefreshSessionsFromGP(ctx, statActivityLister, clearDeletedSessions)
			if err != nil {
				bs.l.Errorf("fail to refresh session list %v", err)
				return err
			}
		}
		if metrics.YagpccMetrics != nil {
			metrics.YagpccMetrics.HandleLatencies.With(map[string]string{"method": "RefreshSessions"}).Observe(time.Since(currTime).Seconds())
		}
		time.Sleep(time.Until(nextTime))
	}
}

func (bs *BackgroundStorage) RefreshQueries(ctx context.Context,
	archChan chan *EndedQuery,
	queriesRefreshInterval time.Duration,
	segmentGetTimeoutSec float64,
	clearDeletedSessions bool) error {
	for {
		currTime := time.Now()
		nextTime := currTime.Add(queriesRefreshInterval)
		select {
		case <-ctx.Done():
			bs.l.Warn("Done RefreshQueries")
			return fmt.Errorf("done context with %v", ctx.Err())
		default:
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
		}
		if metrics.YagpccMetrics != nil {
			metrics.YagpccMetrics.HandleLatencies.With(map[string]string{"method": "RefreshQueries"}).Observe(time.Since(currTime).Seconds())
		}
		time.Sleep(time.Until(nextTime))
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

func NewBackgroundStorage(l *zap.SugaredLogger, sessionStorage *gp.SessionsStorage, rqStorage *storage.RunningQueriesStorage, aggStorage *storage.AggregatedStorage) *BackgroundStorage {
	return &BackgroundStorage{
		l:              l,
		SessionStorage: sessionStorage,
		AggStorage:     aggStorage,
		RQStorage:      rqStorage,
	}
}

func InitBG(
	ctx context.Context,
	l *zap.SugaredLogger,
	masterSentinel masterSentinel,
	statActivityLister statActivityLister,
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
	segChan := make(chan *pbm.SegmentMetricsWrite, cfg.ArchiverConfig.SegmentsQueueSize)

	errG.Go(func() error {
		if err := masterSentinel.RunUntilIsMaster(ctxI); err != nil {
			l.Errorf("the current instance is not considered to be the active master anymore due to an error: %s", err.Error())
			return err
		}

		l.Warnf("the current instance is not considered to be the active master anymore")
		return nil
	})

	if err = statActivityLister.Start(ctx); err != nil {
		return fmt.Errorf("error starting stat activity lister")
	}

	errG.Go(func() error {
		err := backgroundStorage.SendSegmentRefreshMessages(ctxI,
			cfg.SegmentPullRateSec,
			time.Duration(cfg.ConfigCacheDurabilitySec*float64(time.Second)),
			cfg.ListenPort,
			cfg.CustomSegmentList,
		)
		l.Errorf("got %v in segment refresh", err)
		return err
	},
	)
	backgroundStorage.launchSegmentPullers(ctxI, cfg.SegmentPullThreads, cfg.SegmentConnectTimeoutSec, cfg.SegmentGetTimeoutSec, int(cfg.MaxMessageSize))
	backgroundStorage.launchArchivers(ctxI, cfg.MinimumQueryDurationSec, cfg.ArchiverConfig.ArciverProcesses, cfg.ClusterID, archChan, queryChan, segChan, hostname)
	err = backgroundStorage.launchArchiveWriters(ctxI, cfg.ArchiverConfig, queryChan, sessChan, segChan, cfg.ArchiverConfig.MaxFileSize)
	if err != nil {
		return err
	}
	errG.Go(func() error {
		err := backgroundStorage.AggStorage.ArchiveAggQuery(ctxI, queryChan, cfg.ClusterID, hostname)
		l.Errorf("got %v in archive agg query", err)
		return err
	},
	)
	errG.Go(func() error {
		err := backgroundStorage.SendSessionMetrics(ctxI, sessChan, cfg.SessionSendMetricInterval, cfg.ClusterID, hostname)
		l.Errorf("got %v in send session metrics", err)
		return err
	},
	)
	errG.Go(func() error {
		err := backgroundStorage.RefreshSessions(ctxI, statActivityLister, cfg.SessionRefreshInterval, cfg.ClearDeletedSessions)
		l.Errorf("got %v refresh session and queries", err)
		return err
	},
	)
	errG.Go(func() error {
		err := backgroundStorage.RefreshQueries(ctxI, archChan, cfg.QueriesRefreshInterval, cfg.SegmentGetTimeoutSec, cfg.ClearDeletedSessions)
		l.Errorf("got %v refresh session and queries", err)
		return err
	},
	)
	err = errG.Wait()
	if err != nil {
		statActivityLister.Stop()
		l.Errorf("Fail in background precesses - done work with %v", err)
		return err
	}
	l.Info("Done work")
	return nil
}
