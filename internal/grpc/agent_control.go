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

package grpc

import (
	"context"
	"runtime"

	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/open-gpdb/yagpcc/api/proto/agent_segment"
	"github.com/open-gpdb/yagpcc/internal/storage"
	"go.uber.org/zap"
)

type AgentControlServer struct {
	pb.UnimplementedAgentControlServer
	Logger    *zap.SugaredLogger
	RQStorage *storage.RunningQueriesStorage
}

func (s *AgentControlServer) ResetStat(ctx context.Context, in *pb.ResetStatReq) (*pb.ControlResponse, error) {
	s.Logger.Debugf("got reset message")
	s.RQStorage.ClearRunningQueries()

	return &pb.ControlResponse{
		ErrorCode: pb.ControlResponseStatusCode_CONTROL_RESPONSE_STATUS_CODE_SUCCESS,
		ErrorText: "Ok"}, nil
}

func (s *AgentControlServer) GetAgentInfo(ctx context.Context, in *pb.GetInfoReq) (*pb.InfoRepsonse, error) {
	s.Logger.Debugf("got reead data message")

	m := runtime.MemStats{}
	runtime.ReadMemStats(&m)

	stat := s.RQStorage.GetStorageStat()
	retMessage := &pb.InfoRepsonse{
		ResetTime:       timestamppb.New(stat.ResetTime),
		PlannodesCount:  stat.PlanNodesCount,
		QueriesWipedOut: stat.QueriesWipedOut,
		PlansWipedOut:   stat.PlansWipedOut,
		NumGc:           stat.NumGC,
		HeapInuse:       m.HeapInuse,
		HeapAlloc:       m.HeapAlloc,
		NumSysGc:        m.NumGC,
	}
	retMessage.QueriesCount = uint64(s.RQStorage.QueriesCount())
	return retMessage, nil
}
