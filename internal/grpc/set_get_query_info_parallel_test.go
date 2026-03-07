package grpc_test

import (
	"context"
	"log"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/open-gpdb/yagpcc/api/proto/agent_segment"
	pbc "github.com/open-gpdb/yagpcc/api/proto/common"
	"github.com/open-gpdb/yagpcc/internal/gp"
	"github.com/open-gpdb/yagpcc/internal/master"
	"github.com/open-gpdb/yagpcc/internal/storage"
	"github.com/open-gpdb/yagpcc/internal/utils"
)

func TestParallelSetGet(t *testing.T) {
	// todo: should be fixed in MDB-34156
	t.Skip("fails because of data race")
	file, err := os.Create("trace.log")
	require.NoError(t, err)
	zLogger := utils.DualLog(true, file)
	require.NoError(t, err, "error setting up logger")
	rqStorage := storage.NewRunningQueriesStorage()
	aggStorage := storage.NewAggregatedStorage(zLogger)
	sessStorage := gp.NewSessionsStorage(rqStorage)
	backgroundStorage := master.NewBackgroundStorage(zLogger, sessStorage, rqStorage, aggStorage)

	tests := []struct {
		name      string
		isSet     bool
		paramName string
		ssid      int
		value     int
		cnt       int
		sleep     float64
	}{
		{name: "test Set Query1", isSet: true, paramName: "CPU", ssid: 1, value: 1, cnt: 10000, sleep: 0},
		{name: "test Set Query1", isSet: true, paramName: "IO", ssid: 1, value: 8, cnt: 80, sleep: 0.01},
		{name: "test Set Query1", isSet: true, paramName: "Memory", ssid: 1, value: 1024, cnt: 30, sleep: 0.02},
		{name: "test Set Query2", isSet: true, paramName: "CPU", ssid: 2, value: 1, cnt: 10000, sleep: 0},
		{name: "test Get Queries", isSet: false, paramName: "ALL", ssid: 1, value: 1, cnt: 80, sleep: 0.01},
		{name: "test Get Query1", isSet: false, paramName: "QUERY", ssid: 1, value: 1, cnt: 10000, sleep: 0},
	}
	dial := setupGRPCDialer(t, nil, backgroundStorage)

	ctx := context.Background()

	conn, err := grpc.DialContext(ctx, "", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(dial))
	require.NoError(t, err)

	controlC := pb.NewAgentControlClient(conn)
	cResp, errResp := controlC.ResetStat(ctx, &pb.ResetStatReq{})
	require.NoError(t, errResp)
	require.Equal(t, pb.ControlResponseStatusCode_CONTROL_RESPONSE_STATUS_CODE_SUCCESS, cResp.ErrorCode)

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()

			conn, err := grpc.DialContext(ctx, "", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(dial))
			if err != nil {
				log.Fatal(err)
			}

			clientSet := pb.NewSetQueryInfoClient(conn)
			clientGet := pb.NewGetQueryInfoClient(conn)
			for i := 0; i < tc.cnt; i++ {
				if tc.isSet {
					systemStat := &pbc.SystemStat{}
					switch tc.paramName {
					case "CPU":
						systemStat.RunningTimeSeconds = float64(tc.value)
					case "IO":
						systemStat.ReadBytes = uint64(tc.value)
					case "Memory":
						systemStat.Rss = uint64(tc.value)
					}

					request := &pb.SetQueryReq{
						QueryStatus:  pbc.QueryStatus_QUERY_STATUS_START,
						QueryKey:     &pbc.QueryKey{Ssid: int32(tc.ssid)},
						QueryMetrics: &pbc.GPMetrics{SystemStat: systemStat},
					}

					_, err := clientSet.SetMetricQuery(ctx, request)

					assert.NoError(t, err)
				} else {
					reqFilterKey := &pb.GetQueriesInfoReq{}
					if tc.paramName == "QUERY" {
						reqFilterKey = &pb.GetQueriesInfoReq{
							FilterQueries: []*pbc.QueryKey{{Ssid: int32(tc.ssid)}},
						}
					}

					response, err := clientGet.GetMetricQueries(ctx, reqFilterKey)

					assert.NoError(t, err)
					assert.False(t, response != nil && response.QueriesData != nil && len(response.QueriesData) == 0, "empty response")
				}
				time.Sleep(time.Duration(tc.sleep) * time.Second)
			}
			err = conn.Close()
			if err != nil {
				log.Fatal(err)
			}
		})
	}
	err = conn.Close()
	require.NoError(t, err)
}
