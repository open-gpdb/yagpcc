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
