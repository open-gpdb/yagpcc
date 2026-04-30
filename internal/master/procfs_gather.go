package master

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"google.golang.org/grpc"

	"github.com/open-gpdb/yagpcc/internal/gp/stat_activity"
	"github.com/open-gpdb/yagpcc/internal/storage"

	pb "github.com/open-gpdb/yagpcc/api/proto/agent_segment"
	pbc "github.com/open-gpdb/yagpcc/api/proto/common"
	"go.uber.org/zap"
)

const (
	jobsPerQuery = 1000
)

type (
	hostJobMap = map[string][]stat_activity.SessionPid

	ProcfsGatherStorage struct {
		mx                 *sync.RWMutex
		procfsStat         []*pbc.GpPidProcInfo
		l                  *zap.SugaredLogger
		statActivityLister statActivityLister
		gatherTime         time.Time
	}
)

func NewProcfsGatherStorage(l *zap.SugaredLogger, sActivityLister statActivityLister, gTime time.Time) *ProcfsGatherStorage {
	return &ProcfsGatherStorage{
		mx:                 &sync.RWMutex{},
		l:                  l,
		statActivityLister: sActivityLister,
		gatherTime:         gTime,
		procfsStat:         make([]*pbc.GpPidProcInfo, 0, 10),
	}
}

func (ps *ProcfsGatherStorage) getJobsMap(sessions []stat_activity.SessionPid) hostJobMap {
	hostJobs := make(hostJobMap)
	// make work for each host
	for _, process := range sessions {
		segHost := storage.GetHostnameForSegindex(int32(process.GpSegmentId))
		jobList, ok := hostJobs[segHost]
		if !ok {
			jobList = make([]stat_activity.SessionPid, 0, 10)
		}
		jobList = append(jobList, stat_activity.SessionPid{
			GpSegmentId: process.GpSegmentId,
			Pid:         process.Pid,
			SessId:      process.SessId,
		})
		hostJobs[segHost] = jobList
	}
	return hostJobs
}

func (ps *ProcfsGatherStorage) addProcfsStat(procfsStat []*pbc.GpPidProcInfo) {
	ps.mx.Lock()
	defer ps.mx.Unlock()

	ps.procfsStat = append(ps.procfsStat, procfsStat...)
}

func (ps *ProcfsGatherStorage) processProcfsRequests(ctx context.Context, hostname string, portn uint32, gatherTimeout time.Duration, maxMsgSize int, reqs []stat_activity.SessionPid) error {
	grpcConn, err := getGrpcClientConnection(ctx, hostname, portn, gatherTimeout.Seconds())
	if err != nil {
		return fmt.Errorf("grpc client connection error: %v", err)
	}
	cGet := pb.NewGetQueryInfoClient(grpcConn)
	ctxTimeout, ctxCancel := context.WithTimeout(ctx, gatherTimeout)
	defer ctxCancel()
	maxSizeOption := grpc.MaxCallRecvMsgSize(maxMsgSize)
	msgReq := &pb.GetPidProcInfoReq{
		SegmentProcess: make([]*pb.SegmentProcess, 0, 10),
	}
	for _, req := range reqs {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			msgReq.SegmentProcess = append(msgReq.SegmentProcess, &pb.SegmentProcess{
				GpSegmentId: int64(req.GpSegmentId),
				Pid:         int64(req.Pid),
				SessId:      int64(req.SessId),
			})
			if len(msgReq.SegmentProcess) >= jobsPerQuery {
				segResponse, errGet := cGet.GetPidProcStat(ctxTimeout, msgReq, maxSizeOption)
				if errGet != nil {
					return fmt.Errorf("grpc get pid proc stat error: %v", errGet)
				}
				ps.addProcfsStat(segResponse.GetPidProcData())
				msgReq.SegmentProcess = make([]*pb.SegmentProcess, 0, 10)
			}
		}
	}
	if len(msgReq.SegmentProcess) > 0 {
		segResponse, errGet := cGet.GetPidProcStat(ctxTimeout, msgReq, maxSizeOption)
		if errGet != nil {
			return fmt.Errorf("grpc get pid proc stat error: %v", errGet)
		}
		ps.addProcfsStat(segResponse.GetPidProcData())
	}
	return nil
}

func (ps *ProcfsGatherStorage) GatherProcfsStat(ctx context.Context, nPullers int, portn uint32, gatherTimeout time.Duration, maxMsgSize int) error {
	if nPullers <= 0 {
		return fmt.Errorf("nPullers must be greater than 0, got %d", nPullers)
	}
	ps.l.Debug("GatherProcfsStat")
	sessions, err := ps.statActivityLister.ListAllSessions(ctx)
	if err != nil {
		return fmt.Errorf("error listing sessions pids: %v", err)
	}
	hostJobs := ps.getJobsMap(sessions)

	ctxT, ctxTC := context.WithTimeout(ctx, gatherTimeout)
	defer ctxTC()

	g, ctxG := errgroup.WithContext(ctxT)

	for hostname, procfsProcesses := range hostJobs {
		g.Go(func() error {
			return ps.processProcfsRequests(ctxG, hostname, portn, gatherTimeout, maxMsgSize, procfsProcesses)
		})
	}

	return g.Wait()
}

func (ps *ProcfsGatherStorage) GetProcfsStat() []*pbc.GpPidProcInfo {
	ps.mx.RLock()
	defer ps.mx.RUnlock()

	return slices.Clone(ps.procfsStat)
}
