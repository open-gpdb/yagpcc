package master_test

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pbm "github.com/open-gpdb/yagpcc/api/proto/agent_master"
	pbc "github.com/open-gpdb/yagpcc/api/proto/common"
	"github.com/open-gpdb/yagpcc/internal/gp"
	"github.com/open-gpdb/yagpcc/internal/master"
	"github.com/open-gpdb/yagpcc/internal/storage"
	"github.com/open-gpdb/yagpcc/internal/utils"
	"go.uber.org/zap"
)

type (
	mockWriterT struct {
		lastWrite []byte
		mx        *sync.Mutex
	}

	mockWriterCh struct {
		ChanOut chan int
	}

	testMessage struct {
		Message string
	}
)

func (m testMessage) ToJSON() ([]byte, error) {
	return json.Marshal(m)
}

func (p *mockWriterCh) Write(b []byte) (n int, err error) {
	parts := bytes.Split(b, []byte{'\n'})
	p.ChanOut <- len(parts) - 1
	return len(b), nil
}

func (p *mockWriterT) Write(b []byte) (n int, err error) {
	p.mx.Lock()
	defer p.mx.Unlock()

	p.lastWrite = append(p.lastWrite, b...)
	return len(b), nil
}

func (p *mockWriterT) LastWrite() []byte {
	p.mx.Lock()
	defer p.mx.Unlock()

	lastWriteCopy := make([]byte, len(p.lastWrite))
	copy(lastWriteCopy, p.lastWrite)

	return lastWriteCopy
}

func newMockWriter() *mockWriterT {
	return &mockWriterT{mx: &sync.Mutex{}}
}

func jsonMustUnmarshal[T any](t *testing.T, data []byte) T {
	var result T
	err := json.Unmarshal(data, &result)
	require.NoError(t, err, "error forced unmarshaling json")

	return result
}

func TestSerializableObject(t *testing.T) {
	chanOut := make(chan int, 1)
	mockWriter := mockWriterCh{ChanOut: chanOut}
	message := testMessage{Message: "1"}
	serializableChan := make(master.SerializableChan, 1)
	expectedSlice := []int{master.BatchSize, master.BatchSize, 2}
	resultSlice := make([]int, 0)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < (master.BatchSize + master.BatchSize + 2); i++ {
			serializableChan <- message
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range chanOut {
			resultSlice = append(resultSlice, i)
			if len(resultSlice) == len(expectedSlice) {
				break
			}
		}
	}()
	ctx, cFunc := context.WithCancel(context.Background())
	defer cFunc()
	go master.StoreSerializableData(ctx, &zap.SugaredLogger{}, serializableChan, &mockWriter)
	wg.Wait()
	assert.Equal(t, expectedSlice, resultSlice)
}

func TestWriteSession(t *testing.T) {
	mockWriter := newMockWriter()
	// 1 second should be enough to read data from channel and write them to file
	ctx, cFunc := context.WithTimeout(context.Background(), 1*time.Second)

	defer cFunc()

	file, err := os.Create("trace.log")
	require.NoError(t, err)
	zLogger := utils.DualLog(true, file)

	gp.DiscoveredTmID = 12345
	sessChan := make(chan *gp.SessionDataWrite, 1)
	currTime := time.Now().Truncate(time.Minute)

	sessChan <- &gp.SessionDataWrite{
		CollectTime: gp.UTCTime(currTime),
		ClusterID:   "fxd",
		GpStatInfo: &gp.GpStatActivity{SessID: 123,
			TmID:     12345,
			Usename:  "User1",
			DatID:    1,
			Datname:  "db1",
			Pid:      1,
			UsesysID: 10},
		RunningQuery: &storage.QueryKeyWrite{
			Ssid: 123,
			Tmid: 12345,
			Ccnt: 1,
		},
		RunningQueryInfo: &gp.QueryInfoShort{
			QueryID: 321,
			PlanID:  111,
			QueryText: `Select *
from mytable`,
		},
		TotalGPMetrics: &pbc.GPMetrics{
			SystemStat: &pbc.SystemStat{RunningTimeSeconds: 2},
			Instrumentation: &pbc.MetricInstrumentation{
				Ntuples:      2,
				Interconnect: &pbc.InterconnectStat{Retransmits: 5},
			},
		},
		LongRunningGPMetrics: &pbc.GPMetrics{
			SystemStat: &pbc.SystemStat{RunningTimeSeconds: 1},
			Instrumentation: &pbc.MetricInstrumentation{
				Ntuples:      2,
				Interconnect: &pbc.InterconnectStat{Retransmits: 10},
			},
		},
		QueryGPMetrics: &pbc.GPMetrics{
			SystemStat: &pbc.SystemStat{RunningTimeSeconds: 4},
			Instrumentation: &pbc.MetricInstrumentation{
				Ntuples:      5,
				Interconnect: &pbc.InterconnectStat{Retransmits: 15},
			},
		},
		RunningQuerySlices: 10,
	}

	master.StoreSessions(ctx, zLogger, sessChan, mockWriter)

	expectedLastWrite := map[string]interface{}{
		"ClusterID":   "fxd",
		"CollectTime": currTime.UTC().Format(gp.UTCTimeFormat),
		"GpStatInfo": map[string]any{
			"DatID":    float64(1),
			"Datname":  "db1",
			"Pid":      float64(1),
			"SessID":   float64(123),
			"TmID":     float64(12345),
			"Usename":  "User1",
			"UsesysID": float64(10),
		},
		"Hostname": "",
		"LongRunningGPMetrics": map[string]any{
			"instrumentation": map[string]any{
				"ntuples": float64(2),
				"interconnect": map[string]any{
					"retransmits": float64(10),
				},
			},
			"systemStat": map[string]any{
				"runningTimeSeconds": float64(1),
			},
		},
		"QueryGPMetrics": map[string]any{
			"instrumentation": map[string]any{
				"ntuples": float64(5),
				"interconnect": map[string]any{
					"retransmits": float64(15),
				},
			},
			"systemStat": map[string]any{
				"runningTimeSeconds": float64(4),
			},
		},
		"RunningQuery": map[string]any{
			"Ccnt": float64(1),
			"Ssid": float64(123),
			"Tmid": float64(12345),
		},
		"RunningQueryInfo": map[string]any{
			"PlanID":    float64(111),
			"PlanText":  "",
			"QueryID":   float64(321),
			"QueryText": "Select *\nfrom mytable",
		},
		"RunningQueryLevel":  float64(0),
		"RunningQuerySlices": float64(10),
		"RunningQueryStatus": float64(0),
		"TotalGPMetrics": map[string]any{
			"instrumentation": map[string]any{
				"ntuples": float64(2),
				"interconnect": map[string]any{
					"retransmits": float64(5),
				},
			},
			"systemStat": map[string]any{
				"runningTimeSeconds": float64(2),
			},
		},
	}
	actualLastWrite := jsonMustUnmarshal[map[string]any](t, mockWriter.LastWrite())

	assert.Equal(t, expectedLastWrite, actualLastWrite)
}

func TestWriteQuery(t *testing.T) {
	mockWriter := newMockWriter()
	// 1 second should be enough to read data from channel and write them to file
	ctx, cFunc := context.WithTimeout(context.Background(), 1*time.Second)

	defer cFunc()

	file, err := os.Create("trace.log")
	require.NoError(t, err)
	zLogger := utils.DualLog(true, file)

	currTime := time.Now().Truncate(time.Minute)
	gp.DiscoveredTmID = 123
	qChan := make(chan *pbm.QueryStatWrite, 1)

	qChan <- &pbm.QueryStatWrite{
		QueryKey: &pbc.QueryKey{
			Ssid: 1,
			Tmid: 123,
		},
		QueryInfo: &pbc.QueryInfo{
			QueryId: 12,
			PlanId:  321,
			QueryText: `Select 1
from my_table`,
			TemplateQueryText: "SELECT $1 FROM MY_TABLE",
		},
		CollectTime: utils.GetTimeAsString(currTime),
		StatKind:    pbm.StatKind_SK_PRECISE,
		QueryStatus: pbc.QueryStatus_QUERY_STATUS_DONE,
		TotalQueryMetrics: &pbc.GPMetrics{
			SystemStat: &pbc.SystemStat{RunningTimeSeconds: 4},
			Instrumentation: &pbc.MetricInstrumentation{
				Ntuples:        5,
				InheritedCalls: 10,
				Interconnect: &pbc.InterconnectStat{
					Retransmits: 10,
				},
			},
		},
		Message: "Test",
		Slices:  10,
	}

	master.StoreQuery(ctx, zLogger, qChan, mockWriter)

	expectedLastWrite := map[string]any{
		"aggregatedMetrics": nil,
		"blockedBySessId":   "0",
		"clusterId":         "",
		"collectTime":       currTime.Format("2006-01-02T15:04:05-07:00"),
		"completed":         false,
		"endTime":           "",
		"hostname":          "",
		"lockedItem":        "",
		"lockedMode":        "",
		"message":           "Test",
		"queryInfo": map[string]any{
			"databaseName":      "",
			"generator":         "PLAN_GENERATOR_UNSPECIFIED",
			"planId":            "321",
			"planText":          "",
			"queryId":           "12",
			"queryText":         "Select 1\nfrom my_table",
			"rsgname":           "",
			"templatePlanText":  "",
			"templateQueryText": "SELECT $1 FROM MY_TABLE",
			"userName":          "",
			"analyzeText":       "",
			"startTime":         nil,
			"submitTime":        nil,
			"endTime":           nil,
		},
		"queryKey": map[string]any{
			"ccnt": float64(0),
			"ssid": float64(1),
			"tmid": float64(123),
		},
		"queryStatus":  "QUERY_STATUS_DONE",
		"sessionState": "",
		"slices":       "10",
		"startTime":    "",
		"statKind":     "SK_PRECISE",
		"totalQueryMetrics": map[string]any{
			"instrumentation": map[string]any{
				"blkReadTime":       float64(0),
				"blkWriteTime":      float64(0),
				"firsttuple":        float64(0),
				"inheritedCalls":    "10",
				"inheritedTime":     float64(0),
				"localBlksDirtied":  "0",
				"localBlksHit":      "0",
				"localBlksRead":     "0",
				"localBlksWritten":  "0",
				"nloops":            "0",
				"ntuples":           "5",
				"received":          nil,
				"sent":              nil,
				"sharedBlksDirtied": "0",
				"sharedBlksHit":     "0",
				"sharedBlksRead":    "0",
				"sharedBlksWritten": "0",
				"startup":           float64(0),
				"startupTime":       float64(0),
				"tempBlksRead":      "0",
				"tempBlksWritten":   "0",
				"total":             float64(0),
				"tuplecount":        "0",
				"interconnect": map[string]any{
					"activeConnectionsNum":      "0",
					"bufferCountingTime":        "0",
					"capacityCountingTime":      "0",
					"crcErrors":                 "0",
					"disorderedPktNum":          "0",
					"duplicatedPktNum":          "0",
					"mismatchNum":               "0",
					"recvAckNum":                "0",
					"recvPktNum":                "0",
					"recvQueueSizeCountingTime": "0",
					"retransmits":               "10",
					"sndPktNum":                 "0",
					"startupCachedPktNum":       "0",
					"statusQueryMsgNum":         "0",
					"totalBuffers":              "0",
					"totalCapacity":             "0",
					"totalRecvQueueSize":        "0",
				},
			},
			"spill": nil,
			"systemStat": map[string]any{
				"VmPeakKb":            "0",
				"VmSizeKb":            "0",
				"cancelledWriteBytes": "0",
				"kernelTimeSeconds":   float64(0),
				"rchar":               "0",
				"readBytes":           "0",
				"rss":                 "0",
				"runningTimeSeconds":  float64(4),
				"syscr":               "0",
				"syscw":               "0",
				"userTimeSeconds":     float64(0),
				"vsize":               "0",
				"wchar":               "0",
				"writeBytes":          "0",
			},
		},
		"waitMode": "",
	}

	actualLastWrite := jsonMustUnmarshal[map[string]any](t, mockWriter.LastWrite())
	assert.Equal(t, expectedLastWrite, actualLastWrite)
}

func TestWriteSegstat(t *testing.T) {
	mockWriter := newMockWriter()
	// 1 second should be enough to read data from channel and write them to file
	ctx, cFunc := context.WithTimeout(context.Background(), 1*time.Second)

	defer cFunc()

	file, err := os.Create("trace.log")
	require.NoError(t, err)
	zLogger := utils.DualLog(true, file)

	gp.DiscoveredTmID = 12345
	sChan := make(chan *pbm.SegmentMetricsWrite, 2)

	sChan <- &pbm.SegmentMetricsWrite{SegmentKey: &pbc.SegmentKey{Segindex: 1},
		QueryStatus:    pbc.QueryStatus_QUERY_STATUS_DONE,
		SegmentMetrics: &pbc.GPMetrics{SystemStat: &pbc.SystemStat{RunningTimeSeconds: 3}},
		QueryKey:       &pbc.QueryKey{Ssid: 1, Tmid: 12345},
		QueryInfo:      &pbc.QueryInfo{QueryId: 123, PlanId: 12345},
	}
	sChan <- &pbm.SegmentMetricsWrite{SegmentKey: &pbc.SegmentKey{Segindex: 2},
		QueryStatus:    pbc.QueryStatus_QUERY_STATUS_DONE,
		SegmentMetrics: &pbc.GPMetrics{SystemStat: &pbc.SystemStat{RunningTimeSeconds: 2}},
		QueryKey:       &pbc.QueryKey{Ssid: 1, Tmid: 12345},
		QueryInfo:      &pbc.QueryInfo{QueryId: 123, PlanId: 12345},
	}

	master.StoreSegmensMetrics(ctx, zLogger, sChan, mockWriter)

	expectedMessages := []map[string]any{
		{
			"clusterId":   "",
			"collectTime": "",
			"endTime":     "",
			"hostname":    "",
			"queryInfo": map[string]any{
				"databaseName":      "",
				"generator":         "PLAN_GENERATOR_UNSPECIFIED",
				"planId":            "12345",
				"planText":          "",
				"queryId":           "123",
				"queryText":         "",
				"rsgname":           "",
				"templatePlanText":  "",
				"templateQueryText": "",
				"userName":          "",
				"analyzeText":       "",
				"startTime":         nil,
				"submitTime":        nil,
				"endTime":           nil,
			},
			"queryKey": map[string]any{
				"ccnt": float64(0),
				"ssid": float64(1),
				"tmid": float64(12345),
			},
			"queryStatus": "QUERY_STATUS_DONE",
			"segmentKey": map[string]any{
				"dbid":     float64(0),
				"segindex": float64(1),
			},
			"segmentMetrics": map[string]any{
				"instrumentation": nil,
				"spill":           nil,
				"systemStat": map[string]any{
					"VmPeakKb":            "0",
					"VmSizeKb":            "0",
					"cancelledWriteBytes": "0",
					"kernelTimeSeconds":   float64(0),
					"rchar":               "0",
					"readBytes":           "0",
					"rss":                 "0",
					"runningTimeSeconds":  float64(3),
					"syscr":               "0",
					"syscw":               "0",
					"userTimeSeconds":     float64(0),
					"vsize":               "0",
					"wchar":               "0",
					"writeBytes":          "0",
				},
			},
			"startTime": "",
		},
		{
			"clusterId":   "",
			"collectTime": "",
			"endTime":     "",
			"hostname":    "",
			"queryInfo": map[string]any{
				"databaseName":      "",
				"generator":         "PLAN_GENERATOR_UNSPECIFIED",
				"planId":            "12345",
				"planText":          "",
				"queryId":           "123",
				"queryText":         "",
				"rsgname":           "",
				"templatePlanText":  "",
				"templateQueryText": "",
				"userName":          "",
				"analyzeText":       "",
				"startTime":         nil,
				"submitTime":        nil,
				"endTime":           nil,
			},
			"queryKey": map[string]any{
				"ccnt": float64(0),
				"ssid": float64(1),
				"tmid": float64(12345),
			},
			"queryStatus": "QUERY_STATUS_DONE",
			"segmentKey": map[string]any{
				"dbid":     float64(0),
				"segindex": float64(2),
			},
			"segmentMetrics": map[string]any{
				"instrumentation": nil,
				"spill":           nil,
				"systemStat": map[string]any{
					"VmPeakKb":            "0",
					"VmSizeKb":            "0",
					"cancelledWriteBytes": "0",
					"kernelTimeSeconds":   float64(0),
					"rchar":               "0",
					"readBytes":           "0",
					"rss":                 "0",
					"runningTimeSeconds":  float64(2),
					"syscr":               "0",
					"syscw":               "0",
					"userTimeSeconds":     float64(0),
					"vsize":               "0",
					"wchar":               "0",
					"writeBytes":          "0",
				},
			},
			"startTime": "",
		},
	}
	actualMessages := make([]map[string]any, 0)
	for _, data := range bytes.Split(mockWriter.LastWrite(), []byte{'\n'}) {
		if string(data) == "" {
			continue
		}
		actualMessages = append(actualMessages, jsonMustUnmarshal[map[string]any](t, data))
	}

	assert.Equal(t, expectedMessages, actualMessages)
}
