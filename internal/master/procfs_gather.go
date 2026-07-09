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
	"strings"
	"sync"
	"time"

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
		l                  *zap.SugaredLogger
		statActivityLister statActivityLister
		qualityMu          sync.RWMutex
		lastHostsExpected  int64
		lastHostsResponded int64
	}
)

func NewProcfsGatherStorage(l *zap.SugaredLogger, sActivityLister statActivityLister) *ProcfsGatherStorage {
	return &ProcfsGatherStorage{
		l:                  l,
		statActivityLister: sActivityLister,
	}
}

func (ps *ProcfsGatherStorage) setLastQuality(hostsExpected, hostsResponded int64) {
	ps.qualityMu.Lock()
	ps.lastHostsExpected = hostsExpected
	ps.lastHostsResponded = hostsResponded
	ps.qualityMu.Unlock()
}

func (ps *ProcfsGatherStorage) LastHostsExpected() int64 {
	ps.qualityMu.RLock()
	defer ps.qualityMu.RUnlock()
	return ps.lastHostsExpected
}

func (ps *ProcfsGatherStorage) LastHostsResponded() int64 {
	ps.qualityMu.RLock()
	defer ps.qualityMu.RUnlock()
	return ps.lastHostsResponded
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

func (ps *ProcfsGatherStorage) processProcfsRequests(ctx context.Context, hostname string, portn uint32, gatherTimeout time.Duration, maxMsgSize int, reqs []stat_activity.SessionPid) ([]*pbc.GpPidProcInfo, error) {
	grpcConn, err := getGrpcClientConnection(ctx, hostname, portn, gatherTimeout.Seconds())
	if err != nil {
		return nil, fmt.Errorf("grpc client connection error: %w", err)
	}
	cGet := pb.NewGetQueryInfoClient(grpcConn)
	ctxTimeout, ctxCancel := context.WithTimeout(ctx, gatherTimeout)
	defer ctxCancel()
	maxSizeOption := grpc.MaxCallRecvMsgSize(maxMsgSize)
	msgReq := &pb.GetPidProcInfoReq{
		SegmentProcess: make([]*pb.SegmentProcess, 0, 10),
	}
	var result []*pbc.GpPidProcInfo
	for _, req := range reqs {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			msgReq.SegmentProcess = append(msgReq.SegmentProcess, &pb.SegmentProcess{
				GpSegmentId: int64(req.GpSegmentId),
				Pid:         int64(req.Pid),
				SessId:      int64(req.SessId),
			})
			if len(msgReq.SegmentProcess) >= jobsPerQuery {
				segResponse, errGet := cGet.GetPidProcStat(ctxTimeout, msgReq, maxSizeOption)
				if errGet != nil {
					return nil, fmt.Errorf("grpc get pid proc stat error: hostname %v port %v error %w", hostname, portn, errGet)
				}
				result = append(result, segResponse.GetPidProcData()...)
				msgReq.SegmentProcess = make([]*pb.SegmentProcess, 0, 10)
			}
		}
	}
	if len(msgReq.SegmentProcess) > 0 {
		segResponse, errGet := cGet.GetPidProcStat(ctxTimeout, msgReq, maxSizeOption)
		if errGet != nil {
			return nil, fmt.Errorf("grpc get pid proc stat error: hostname %v port %v error %w", hostname, portn, errGet)
		}
		result = append(result, segResponse.GetPidProcData()...)
	}
	return result, nil
}

func (ps *ProcfsGatherStorage) GatherProcfsStat(ctx context.Context, nPullers int, portn uint32, gatherTimeout time.Duration, maxMsgSize int) ([]*pbc.GpPidProcInfo, error) {
	if nPullers <= 0 {
		return nil, fmt.Errorf("nPullers must be greater than 0, got %d", nPullers)
	}
	ps.l.Debug("GatherProcfsStat")
	sessions, err := ps.statActivityLister.ListAllSessions(ctx)
	if err != nil {
		return nil, fmt.Errorf("error listing sessions pids: %w", err)
	}
	hostJobs := ps.getJobsMap(sessions)
	ps.setLastQuality(int64(len(hostJobs)), 0)

	if len(hostJobs) == 0 {
		return nil, nil
	}

	ctxT, ctxTC := context.WithTimeout(ctx, gatherTimeout)
	defer ctxTC()

	var mu sync.Mutex
	var collected []*pbc.GpPidProcInfo
	var errs []string
	respondedHosts := int64(0)
	totalJobs := len(hostJobs)

	sem := make(chan struct{}, nPullers)
	var wg sync.WaitGroup

	for hostname, procfsProcesses := range hostJobs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			result, errR := ps.processProcfsRequests(ctxT, hostname, portn, gatherTimeout, maxMsgSize, procfsProcesses)
			mu.Lock()
			defer mu.Unlock()
			if errR != nil {
				ps.l.Warnf("failed to gather procfs from host %s: %v", hostname, errR)
				errs = append(errs, fmt.Sprintf("host %s: %v", hostname, errR))
				return
			}
			respondedHosts++
			collected = append(collected, result...)
		}()
	}

	wg.Wait()
	ps.setLastQuality(int64(totalJobs), respondedHosts)

	if len(errs) == totalJobs {
		return nil, fmt.Errorf("all %d hosts failed: %s", totalJobs, strings.Join(errs, "; "))
	}
	return collected, nil
}
