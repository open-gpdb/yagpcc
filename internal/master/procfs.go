package master

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"

	"github.com/open-gpdb/yagpcc/internal/gp/stat_activity"
	"github.com/open-gpdb/yagpcc/internal/storage"

	"github.com/alitto/pond"

	pb "github.com/open-gpdb/yagpcc/api/proto/agent_segment"
)

const (
	JobsPerQuery = 100
)

type (
	hostJobMap = map[string][]stat_activity.SessionPid
)

func getJobsMap(sessions []stat_activity.SessionPid) hostJobMap {
	hostJobMap := make(hostJobMap)
	// make work for each host
	for _, process := range sessions {
		segHost := storage.GetHostnameForSegindex(int32(process.GpSegmentId))
		jobList, ok := hostJobMap[segHost]
		if !ok {
			jobList = make([]stat_activity.SessionPid, 0)
		}
		jobList = append(jobList, stat_activity.SessionPid{
			GpSegmentId: process.GpSegmentId,
			Pid:         process.Pid,
			SessId:      process.SessId,
		})
		hostJobMap[segHost] = jobList
	}
	return hostJobMap
}

func processProcfsRequests(ctx context.Context, hostname string, portn uint32, gatherTimeout time.Duration, maxMsgSize int, reqs []stat_activity.SessionPid) error {
	grpcConn, err := getGrpcClientConnection(ctx, hostname, portn, gatherTimeout.Seconds())
	if err != nil {
		return err
	}
	msgReq := &pb.GetPidProcInfoReq{
		SegmentProcess: make([]*pb.SegmentProcess, 0),
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
		}
	}
	cGet := pb.NewGetQueryInfoClient(grpcConn)
	ctxTimeout, ctxCancel := context.WithTimeout(ctx, gatherTimeout)
	defer ctxCancel()
	maxSizeOption := grpc.MaxCallRecvMsgSize(maxMsgSize)
	_, errGet := cGet.GetPidProcStat(ctxTimeout, msgReq, maxSizeOption)
	if errGet != nil {
		return errGet
	}
	return nil
}

func (bs *BackgroundStorage) GatherProcfsStat(ctx context.Context, nPullers int, portn uint32, gatherTimeout time.Duration, maxMsgSize int) error {
	if nPullers <= 0 {
		return fmt.Errorf("nPullers must be greater than 0, got %d", nPullers)
	}
	bs.l.Debug("GatherProcfsStat")
	sessions, err := bs.StatActivityLister.ListAllSessions(ctx)
	if err != nil {
		return err
	}
	hostJobMap := getJobsMap(sessions)

	pool := pond.New(nPullers, nPullers*2)
	defer pool.StopAndWait()

	ctxT, ctxTC := context.WithTimeout(ctx, gatherTimeout)
	defer ctxTC()

	group, ctxG := pool.GroupContext(ctxT)

	for hostname, processes := range hostJobMap {
		host := hostname
		jobProcesses := make([]stat_activity.SessionPid, 0, JobsPerQuery)
		for _, process := range processes {
			jobProcesses = append(jobProcesses, process)
			if len(jobProcesses) >= JobsPerQuery {
				batch := append([]stat_activity.SessionPid(nil), jobProcesses...)
				group.Submit(func() error {
					return processProcfsRequests(ctxG, host, portn, gatherTimeout, maxMsgSize, batch)
				})
				jobProcesses = make([]stat_activity.SessionPid, 0, JobsPerQuery)
			}
		}
		if len(jobProcesses) > 0 {
			batch := append([]stat_activity.SessionPid(nil), jobProcesses...)
			group.Submit(func() error {
				return processProcfsRequests(ctxG, host, portn, gatherTimeout, maxMsgSize, batch)
			})
		}
	}

	return group.Wait()
}
