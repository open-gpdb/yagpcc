package master

import (
	"context"
	"fmt"
	"time"

	"golang.org/x/sync/errgroup"

	"google.golang.org/grpc"

	"github.com/open-gpdb/yagpcc/internal/gp/stat_activity"
	"github.com/open-gpdb/yagpcc/internal/storage"

	pb "github.com/open-gpdb/yagpcc/api/proto/agent_segment"
)

const (
	jobsPerQuery = 1000
)

type (
	hostJobMap = map[string][]stat_activity.SessionPid
)

func (bs *BackgroundStorage) getJobsMap(sessions []stat_activity.SessionPid) hostJobMap {
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

func (bs *BackgroundStorage) processProcfsRequests(ctx context.Context, hostname string, portn uint32, gatherTimeout time.Duration, maxMsgSize int, reqs []stat_activity.SessionPid) error {
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
				_, errGet := cGet.GetPidProcStat(ctxTimeout, msgReq, maxSizeOption)
				if errGet != nil {
					return fmt.Errorf("grpc get pid proc stat error: %v", errGet)
				}
				msgReq.SegmentProcess = make([]*pb.SegmentProcess, 0, 10)
			}
		}
	}
	if len(msgReq.SegmentProcess) > 0 {
		_, errGet := cGet.GetPidProcStat(ctxTimeout, msgReq, maxSizeOption)
		if errGet != nil {
			return fmt.Errorf("grpc get pid proc stat error: %v", errGet)
		}

	}
	return nil
}

func (bs *BackgroundStorage) GatherProcfsStat(ctx context.Context, nPullers int, portn uint32, gatherTimeout time.Duration, maxMsgSize int) error {
	if nPullers <= 0 {
		return fmt.Errorf("nPullers must be greater than 0, got %d", nPullers)
	}
	bs.l.Debug("GatherProcfsStat")
	sessions, err := bs.statActivityLister.ListAllSessions(ctx)
	if err != nil {
		return fmt.Errorf("error listing sessions pids: %v", err)
	}
	hostJobs := bs.getJobsMap(sessions)

	ctxT, ctxTC := context.WithTimeout(ctx, gatherTimeout)
	defer ctxTC()

	g, ctxG := errgroup.WithContext(ctxT)

	for hostname, procfsProcesses := range hostJobs {
		g.Go(func() error {
			return bs.processProcfsRequests(ctxG, hostname, portn, gatherTimeout, maxMsgSize, procfsProcesses)
		})
	}

	return g.Wait()
}
