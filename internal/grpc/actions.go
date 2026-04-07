package grpc

import (
	"context"
	"fmt"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/emptypb"

	pbm "github.com/open-gpdb/yagpcc/api/proto/agent_master"
	pbc "github.com/open-gpdb/yagpcc/api/proto/common"
	"github.com/open-gpdb/yagpcc/internal/gp"
	"github.com/open-gpdb/yagpcc/internal/master"
	"go.uber.org/zap"
)

type ActionsServer struct {
	pbm.UnimplementedActionServiceServer
	ClusterID string
	Logger    *zap.SugaredLogger
	Timeout   time.Duration

	BackgroundStorage *master.BackgroundStorage
}

// terminateSessionForbiddenReason returns a non-empty message if the session must not be terminated.
func terminateSessionForbiddenReason(session *pbc.SessionState) string {
	if session == nil {
		return "session is nil"
	}
	if session.SessionKey == nil {
		return "session has no session key"
	}
	if session.SessionKey.SessId == 0 {
		return "terminating session with sess_id 0 is forbidden"
	}
	if session.SessionInfo != nil && strings.EqualFold(strings.TrimSpace(session.SessionInfo.User), "gpadmin") {
		return "terminating gpadmin sessions is forbidden"
	}
	return ""
}

func (s *ActionsServer) MoveQueryToResourceGroup(ctx context.Context, in *pbm.MoveQueryToResourceGroupRequest) (response *emptypb.Empty, err error) {
	ctx, cancel := context.WithTimeout(ctx, s.Timeout)
	defer cancel()

	s.Logger.Infof("got move query to resource group request %v", in)
	if in.GetQueryKey() == nil {
		return nil, fmt.Errorf("cannot move empty query")
	}
	if in.GetResourceGroupName() == "" {
		return nil, fmt.Errorf("cannot move query to empty resource group")
	}
	start := time.Now()
	err = gp.MoveQueryToResourceGroup(ctx, int(in.GetQueryKey().Ssid), in.GetResourceGroupName())
	if err != nil {
		s.Logger.Infof("fail to move query %v", err)
		return nil, fmt.Errorf("fail to move query to resource group")
	}
	s.Logger.Debugf("move query to resource group took %v\n", time.Since(start))
	return &emptypb.Empty{}, nil
}

func (s *ActionsServer) TerminateQuery(ctx context.Context, in *pbm.TerminateQueryRequest) (response *pbm.TerminateResponse, err error) {
	ctx, cancel := context.WithTimeout(ctx, s.Timeout)
	defer cancel()

	s.Logger.Infof("got terminate query request %v", in)
	if in.GetQueryKey() == nil {
		return nil, fmt.Errorf("cannot cancel empty query")
	}
	start := time.Now()
	response = &pbm.TerminateResponse{
		StatusCode: pbm.TerminateResponseStatusCode_TERMINATE_RESPONSE_STATUS_CODE_SUCCESS,
		StatusText: "",
	}
	err = gp.CancelQuery(ctx, int(in.GetQueryKey().Ssid), false)
	if err != nil {
		s.Logger.Infof("fail to cancel query %v", err)
		response.StatusCode = pbm.TerminateResponseStatusCode_TERMINATE_RESPONSE_STATUS_CODE_ERROR
		response.StatusText = "fail to cancel query"
		return response, nil
	}
	s.Logger.Debugf("terminate query took %v\n", time.Since(start))
	return response, nil
}

func (s *ActionsServer) TerminateSession(ctx context.Context, in *pbm.TerminateSessionRequest) (response *pbm.TerminateResponse, err error) {
	ctx, cancel := context.WithTimeout(ctx, s.Timeout)
	defer cancel()

	s.Logger.Infof("got terminate session request %v", in)
	if in.GetSessionKey() == nil {
		return nil, fmt.Errorf("cannot cancel empty session")
	}
	sessID := in.GetSessionKey().GetSessId()
	if sessID == 0 {
		return nil, fmt.Errorf("cannot terminate session with sess_id 0")
	}
	if s.BackgroundStorage != nil && s.BackgroundStorage.SessionStorage != nil {
		if info, ok := s.BackgroundStorage.SessionStorage.GetSession(gp.SessionKey{SessID: int(sessID)}); ok {
			info.SessionLock.RLock()
			user := ""
			if info.SessionData.GpStatInfo != nil {
				user = info.SessionData.GpStatInfo.Usename
			}
			info.SessionLock.RUnlock()
			if strings.EqualFold(strings.TrimSpace(user), "gpadmin") {
				return &pbm.TerminateResponse{
					StatusCode: pbm.TerminateResponseStatusCode_TERMINATE_RESPONSE_STATUS_CODE_ERROR,
					StatusText: "terminating gpadmin session is forbidden",
				}, nil
			}
		}
	}
	start := time.Now()
	response = &pbm.TerminateResponse{
		StatusCode: pbm.TerminateResponseStatusCode_TERMINATE_RESPONSE_STATUS_CODE_SUCCESS,
		StatusText: "",
	}
	err = gp.CancelQuery(ctx, int(sessID), true)
	if err != nil {
		s.Logger.Infof("fail to terminate session %v", err)
		response.StatusCode = pbm.TerminateResponseStatusCode_TERMINATE_RESPONSE_STATUS_CODE_ERROR
		response.StatusText = "fail to terminate session"
		return response, nil
	}
	response.StatusCode = pbm.TerminateResponseStatusCode_TERMINATE_RESPONSE_STATUS_CODE_SUCCESS
	s.Logger.Debugf("terminate session took %v\n", time.Since(start))
	return response, nil
}

// sessionMatchesTerminateSessionsRequest returns whether session satisfies all non-empty filters in in.
func sessionMatchesTerminateSessionsRequest(session *pbc.SessionState, in *pbm.TerminateSessionsRequest) bool {
	if session == nil || session.SessionInfo == nil {
		return false
	}
	if in.GetDatabase() != "" && session.SessionInfo.Database != in.GetDatabase() {
		return false
	}
	if in.GetUsername() != "" && session.SessionInfo.User != in.GetUsername() {
		return false
	}
	if in.GetQueryId() != 0 {
		if session.RunningQueryInfo == nil || session.RunningQueryInfo.QueryId != in.GetQueryId() {
			return false
		}
	}
	return true
}

func (s *ActionsServer) TerminateSessions(ctx context.Context, in *pbm.TerminateSessionsRequest) (response *pbm.TerminateResponses, err error) {
	ctx, cancel := context.WithTimeout(ctx, s.Timeout)
	defer cancel()

	if in.GetDatabase() == "" && in.GetUsername() == "" && in.GetQueryId() == 0 {
		return nil, fmt.Errorf("should specify database, username or query id")
	}

	if s.BackgroundStorage == nil || s.BackgroundStorage.SessionStorage == nil {
		return nil, fmt.Errorf("session storage is not initialized")
	}

	s.Logger.Infof("got terminate many sessions request %v", in)
	start := time.Now()
	sResp, err := s.BackgroundStorage.SessionStorage.GetAllSessions(false, pbm.RunningQueryType_RQT_LAST)
	if err != nil {
		s.Logger.Infof("fail to list sessions %v", err)
		return nil, fmt.Errorf("fail to list sessions")
	}
	response = &pbm.TerminateResponses{}
	for _, session := range sResp.SessionsState {
		if !sessionMatchesTerminateSessionsRequest(session, in) {
			continue
		}
		if reason := terminateSessionForbiddenReason(session); reason != "" {
			s.Logger.Infof("skip terminate session: %s", reason)
			response.TerminateResponse = append(response.TerminateResponse, &pbm.TerminateResponse{
				StatusCode: pbm.TerminateResponseStatusCode_TERMINATE_RESPONSE_STATUS_CODE_ERROR,
				StatusText: reason,
			})
			continue
		}
		startS := time.Now()
		terminateResponse := pbm.TerminateResponse{
			StatusCode: pbm.TerminateResponseStatusCode_TERMINATE_RESPONSE_STATUS_CODE_SUCCESS,
		}
		err = gp.CancelQuery(ctx, int(session.SessionKey.SessId), true)
		if err != nil {
			s.Logger.Infof("fail to terminate session %v", err)
			terminateResponse.StatusCode = pbm.TerminateResponseStatusCode_TERMINATE_RESPONSE_STATUS_CODE_ERROR
			terminateResponse.StatusText = fmt.Sprintf("fail to terminate session %d", session.SessionKey.SessId)
		} else {
			terminateResponse.StatusText = fmt.Sprintf("terminate session %d took %v", session.SessionKey.SessId, time.Since(startS))
		}
		response.TerminateResponse = append(response.TerminateResponse, &terminateResponse)
		s.Logger.Debugf("terminate session %d took %v\n", session.SessionKey.SessId, time.Since(startS))
	}

	s.Logger.Debugf("terminate all sessions took %v\n", time.Since(start))
	return response, nil
}
