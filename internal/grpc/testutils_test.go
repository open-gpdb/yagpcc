package grpc_test

import (
	"context"
	"log"
	"net"
	"os"
	"sort"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	gogrpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	pbm "github.com/open-gpdb/yagpcc/api/proto/agent_master"
	pb "github.com/open-gpdb/yagpcc/api/proto/agent_segment"
	"github.com/open-gpdb/yagpcc/internal/baseapp"
	"github.com/open-gpdb/yagpcc/internal/config"
	"github.com/open-gpdb/yagpcc/internal/gp"
	"github.com/open-gpdb/yagpcc/internal/grpc"
	"github.com/open-gpdb/yagpcc/internal/master"
	"github.com/open-gpdb/yagpcc/internal/storage"
	"github.com/open-gpdb/yagpcc/internal/utils"
)

func assertMetricResponseIsOk(t *testing.T, response *pb.MetricResponse) bool {
	return utils.AssertProtoMessagesEqual(
		t,
		&pb.MetricResponse{
			ErrorCode: pb.MetricResponseStatusCode_METRIC_RESPONSE_STATUS_CODE_SUCCESS,
			ErrorText: "Ok",
		},
		response,
	)
}

func assertQueriesInfoResponseEqual(t *testing.T, expected *pb.GetQueriesInfoResponse, actual *pb.GetQueriesInfoResponse) bool {
	normalize := func(response *pb.GetQueriesInfoResponse) *pb.GetQueriesInfoResponse {
		sort.Slice(response.QueriesData, func(i, j int) bool {
			if response.QueriesData[i].QueryKey.Ssid != response.QueriesData[j].QueryKey.Ssid {
				return response.QueriesData[i].QueryKey.Ssid < response.QueriesData[j].QueryKey.Ssid
			}
			if response.QueriesData[i].QueryKey.Ccnt != response.QueriesData[j].QueryKey.Ccnt {
				return response.QueriesData[i].QueryKey.Ccnt < response.QueriesData[j].QueryKey.Ccnt
			}
			return response.QueriesData[i].SliceId < response.QueriesData[j].SliceId
		})
		return response
	}

	return utils.AssertProtoMessagesEqual(t, normalize(expected), normalize(actual))
}

func setupGRPCDialer(t *testing.T, sessionMocker *MockStatActivityLister, backgroundStorage *master.BackgroundStorage) func(context.Context, string) (net.Conn, error) {

	cfg, err := config.DefaultConfig()
	require.NoError(t, err, "error getting default config")

	baseApp, err := baseapp.New(
		baseapp.WithConfig(cfg.App),
		baseapp.WithMetrics(),
		baseapp.WithInstrumentation(),
	)
	require.NoError(t, err, "error setting up baseapp")
	defer baseApp.Shutdown()

	listener := bufconn.Listen(1024 * 1024)
	server := gogrpc.NewServer()

	file, err := os.Create("trace.log")
	require.NoError(t, err)
	zLogger := utils.DualLog(true, file)

	pb.RegisterSetQueryInfoServer(server, &grpc.SetQueryInfoServer{Logger: zLogger, UpdateSessionMetrics: true, RQStorage: backgroundStorage.RQStorage, SessionsStorage: backgroundStorage.SessionStorage})
	pb.RegisterGetQueryInfoServer(server, &grpc.GetQueryInfoServer{Logger: zLogger, MaxMessageSize: 100 * 1024 * 1024, RQStorage: backgroundStorage.RQStorage})
	pb.RegisterAgentControlServer(server, &grpc.AgentControlServer{Logger: zLogger, RQStorage: backgroundStorage.RQStorage})
	pbm.RegisterGetGPInfoServer(server, grpc.NewGetMasterInfoServer("test", zLogger, sessionMocker, 100*1024*1024, backgroundStorage))

	go func() {
		if err := server.Serve(listener); err != nil {
			log.Fatal(err)
		}
	}()

	return func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}
}

func (s *grpcClientSet) SetQueryInfoClient() pb.SetQueryInfoClient {
	return s.setQueryInfoClient
}

func (s *grpcClientSet) GetQueryInfoClient() pb.GetQueryInfoClient {
	return s.getQueryInfoClient
}

func (s *grpcClientSet) GetGetGPInfoClient() pbm.GetGPInfoClient {
	return s.getGPInfoClient
}

type grpcClientSet struct {
	setQueryInfoClient pb.SetQueryInfoClient
	getQueryInfoClient pb.GetQueryInfoClient
	getGPInfoClient    pbm.GetGPInfoClient
	getSessionMocker   *MockStatActivityLister
	backgroundStorage  *master.BackgroundStorage
}

func setupGRPCClientSet(t *testing.T, sessionMocker *MockStatActivityLister) (*grpcClientSet, func()) {
	ctx := context.Background()

	if sessionMocker == nil {
		// set pre-defined session mocker
		ctrl := gomock.NewController(t)
		sessionMocker = NewMockStatActivityLister(ctrl)
		sessionMocker.EXPECT().List(gomock.Any()).AnyTimes()
	}

	file, err := os.Create("trace.log")
	require.NoError(t, err)
	zLogger := utils.DualLog(true, file)
	rqStorage := storage.NewRunningQueriesStorage()
	sessStorage := gp.NewSessionsStorage(rqStorage)
	aggStorage := storage.NewAggregatedStorage(zLogger)
	backgroundStorage := master.NewBackgroundStorage(zLogger, sessStorage, rqStorage, aggStorage)

	conn, err := gogrpc.DialContext(
		ctx,
		"",
		gogrpc.WithTransportCredentials(insecure.NewCredentials()),
		gogrpc.WithContextDialer(setupGRPCDialer(t, sessionMocker, backgroundStorage)),
	)
	require.NoError(t, err)

	controlClient := pb.NewAgentControlClient(conn)
	response, err := controlClient.ResetStat(ctx, &pb.ResetStatReq{})
	require.NoError(t, err, "error resetting stats")
	require.Equal(t, pb.ControlResponseStatusCode_CONTROL_RESPONSE_STATUS_CODE_SUCCESS, response.ErrorCode)

	cleanup := func() {
		err := conn.Close()
		require.NoError(t, err, "error cleaning up")
	}

	return &grpcClientSet{
		setQueryInfoClient: pb.NewSetQueryInfoClient(conn),
		getQueryInfoClient: pb.NewGetQueryInfoClient(conn),
		getGPInfoClient:    pbm.NewGetGPInfoClient(conn),
		getSessionMocker:   sessionMocker,
		backgroundStorage:  backgroundStorage,
	}, cleanup
}
