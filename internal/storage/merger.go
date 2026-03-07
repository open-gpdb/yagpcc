package storage

import (
	"fmt"
	"sort"
	"strconv"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	pbm "github.com/open-gpdb/yagpcc/api/proto/agent_master"
	pb "github.com/open-gpdb/yagpcc/api/proto/agent_segment"
	pbc "github.com/open-gpdb/yagpcc/api/proto/common"
	"github.com/open-gpdb/yagpcc/internal/utils"
)

// aggregation kinds
type AggregateKind int

const (
	_                           = iota
	AggMasterOnly AggregateKind = 1 << iota
	AggSegmentHost
	AggMax
	// AggSlices
	// AggSegmentAndSlices
)

const (
	MainSliceId  = 0
	UnsetSliceId = -1
)

func (s *RunningQueriesStorage) MergeSegmentData(queriesInfo *pb.GetQueriesInfoResponse) error {

	for _, queryData := range queriesInfo.QueriesData {
		if queryData == nil {
			continue
		}
		qKey := NewQueryKey(queryData.GetQueryKey(), queryData.GetSegmentKey(), queryData.SliceId)
		qData := &QueryData{
			CurrentStatus: int32(queryData.GetQueryStatus()),
			QueryInfo:     &pbc.QueryInfo{},
			QueryMetrics:  &pbc.GPMetrics{},
		}
		if queryData.AdditionalStat != nil {
			qData.QueryMessage = queryData.AdditionalStat.ErrorMessage
		}
		if queryData.QueryInfo != nil {
			qData.QueryInfo = proto.Clone(queryData.QueryInfo).(*pbc.QueryInfo)
		}
		if queryData.QueryMetrics != nil {
			qData.QueryMetrics = proto.Clone(queryData.QueryMetrics).(*pbc.GPMetrics)
			if qData.QueryMetrics.SystemStat != nil {
				qData.QueryMetrics.SystemStat.RunningTimeSeconds = qData.QueryMetrics.SystemStat.UserTimeSeconds + qData.QueryMetrics.SystemStat.KernelTimeSeconds
			}
		}
		if queryData.GetQueryStart().IsValid() {
			qData.QueryInfo.StartTime = queryData.GetQueryStart()
		}
		if queryData.GetQueryEnd().IsValid() {
			qData.QueryInfo.EndTime = queryData.GetQueryEnd()
		}
		var err1, err2 error = nil, nil
		s.mx.RLock()
		rQ, okQ := s.runningQueries[qKey.QKey]
		if okQ {
			// we store data only for queries that in a cache
			rQ.QueryLock.Lock()
			// copy already saved values
			oldData, oldOk := rQ.QueriesData[*qKey]
			if oldOk {
				oldData.QueryDataLock.RLock()
				if oldData.QueryInfo != nil {
					err1 = MergeQueryInfo(qData.QueryInfo, oldData.QueryInfo)
				}
				if oldData.QueryMetrics != nil {
					err2 = MergeGPMetrics(qData.QueryMetrics, oldData.QueryMetrics)
				}
				oldData.QueryDataLock.RUnlock()
			}
			// now save data
			rQ.QueriesData[*qKey] = qData
			// mark as processed
			if rQ.Completed {
				rQ.SegmentNodes[uint32(qKey.SKey.Segindex)] = time.Now()
			}
			rQ.QueryLock.Unlock()
		}
		s.mx.RUnlock()
		if err1 != nil {
			return err1
		}
		if err2 != nil {
			return err2
		}
	}

	return nil
}

func AggregateQueryStat(resultQ *pbm.TotalQueryData, qKey QueryKey, rQ *RunningQuery, tmID int64, aggKind AggregateKind) error {
	rQ.QueryLock.RLock()
	qKeyResult := &pbc.QueryKey{
		Ssid: qKey.Ssid,
		Ccnt: qKey.Ccnt,
		Tmid: int32(tmID),
	}
	resultQ.QueryStat.Message = ""
	resultQ.QueryStat.QueryInfo = &pbc.QueryInfo{}
	collectTime := timestamppb.New(time.Now())
	slices := make([]int64, 0)
	segmentQueryMap := make(map[int32]*pbm.SegmentMetrics)
	//intermediateResults the same for the whole query
	intermediateResults := make(map[MapAggregateKey]uint64, 0)
	for keyS, valS := range rQ.QueriesData {
		valS.QueryDataLock.RLock()
		if aggKind == AggSegmentHost {
			segmentKey := &pbc.SegmentKey{
				Dbid:     keyS.SKey.Dbid,
				Segindex: keyS.SKey.Segindex,
			}
			segmentData, ok := segmentQueryMap[segmentKey.Segindex]
			slices = append(slices, keyS.SliceID)
			// here we store data in per-segindex map. For segindex we should sum all the data
			segHost := strconv.Itoa(int(segmentKey.Segindex))
			if !ok {
				segmentData = &pbm.SegmentMetrics{
					SegmentKey:     segmentKey,
					CollectTime:    collectTime,
					QueryKey:       qKeyResult,
					Hostname:       GetHostnameForSegindex(keyS.SKey.Segindex),
					SegmentMetrics: &pbc.GPMetrics{},
					QueryStatus:    pbc.QueryStatus(valS.CurrentStatus),
				}
				if valS.QueryInfo != nil {
					segmentData.QueryInfo = proto.Clone(valS.QueryInfo).(*pbc.QueryInfo)
					segmentData.StartTime = valS.QueryInfo.StartTime
					segmentData.EndTime = valS.QueryInfo.EndTime
					segmentData.StartTime = valS.QueryInfo.StartTime
				}
				err := GroupGPMetrics(segmentData.SegmentMetrics, valS.QueryMetrics, AggSegmentHost, segHost, intermediateResults)
				if err != nil {
					return fmt.Errorf("fail while group metrics %v for %v", err, keyS)
				}

			} else {
				err := GroupGPMetrics(segmentData.SegmentMetrics, valS.QueryMetrics, AggSegmentHost, segHost, intermediateResults)
				if err != nil {
					return fmt.Errorf("fail while group metrics %v for %v", err, keyS)
				}
				err = MergeQueryInfo(segmentData.QueryInfo, valS.QueryInfo)
				if err != nil {
					return fmt.Errorf("fail while group metrics %v for %v", err, keyS)
				}
			}
			segmentQueryMap[segmentKey.Segindex] = segmentData
		}
		// here could be any slice number
		if keyS.SKey.Segindex == -1 {
			err := MergeQueryInfo(resultQ.QueryStat.QueryInfo, valS.QueryInfo)
			if err != nil {
				return fmt.Errorf("fail while group query info %v for %v", err, keyS)
			}
			resultQ.QueryStat.Hostname = GetHostnameForSegindex(keyS.SKey.Segindex)
			resultQ.QueryStat.QueryStatus = pbc.QueryStatus(valS.CurrentStatus)
			if resultQ.QueryStat.QueryInfo != nil {
				resultQ.QueryStat.StartTime = resultQ.QueryStat.QueryInfo.StartTime
				resultQ.QueryStat.EndTime = resultQ.QueryStat.QueryInfo.EndTime
			}
		}
		if resultQ.QueryStat.Message == "" && valS.QueryMessage != "" {
			resultQ.QueryStat.Message = valS.QueryMessage
		}

		valS.QueryDataLock.RUnlock()
	}

	resultQ.QueryStat.Completed = rQ.Completed
	rQ.QueryLock.RUnlock()

	resultQ.QueryStat.CollectTime = collectTime
	resultQ.QueryStat.QueryKey = qKeyResult
	resultQ.QueryStat.StatKind = pbm.StatKind_SK_PRECISE
	resultQ.QueryStat.Slices = utils.NumberOfUniqueSlices(slices)
	// Aggregate values and fill result protobuf
	resultQ.QueryStat.TotalQueryMetrics = &pbc.GPMetrics{}
	// Here we could not use intermediate results but still should create object
	intermediateResults = make(map[MapAggregateKey]uint64, 0)
	for _, valS := range segmentQueryMap {
		resultQ.SegmentQueryMetrics = append(resultQ.SegmentQueryMetrics, valS)
		// copy total info from master slice 0
		err := GroupGPMetrics(resultQ.QueryStat.TotalQueryMetrics, valS.SegmentMetrics, AggMax, GetHostnameForSegindex(valS.SegmentKey.Segindex), intermediateResults)
		if err != nil {
			return fmt.Errorf("fail while group metrics %v", err)
		}
	}
	// sort data for convinience
	sort.Slice(resultQ.SegmentQueryMetrics, func(i, j int) bool {
		return resultQ.SegmentQueryMetrics[i].SegmentKey.Segindex < resultQ.SegmentQueryMetrics[j].SegmentKey.Segindex
	})
	return nil
}

// get query data with statistics
func (s *RunningQueriesStorage) GetQueryData(queryKey QueryKey, tmID int64) (*pbm.TotalQueryData, error) {
	qT := &pbm.TotalQueryData{
		QueryStat:           &pbm.QueryStat{},
		SegmentQueryMetrics: make([]*pbm.SegmentMetrics, 0),
	}
	s.mx.RLock()
	rQ, okQ := s.runningQueries[queryKey]
	s.mx.RUnlock()
	if okQ {
		err := AggregateQueryStat(qT, queryKey, rQ, tmID, AggSegmentHost)
		if err != nil {
			return nil, err
		}
	}
	return qT, nil
}

// get only master info stat
func (s *RunningQueriesStorage) GetQueryInfo(queryKey QueryKey, tmID int64) (*pbm.TotalQueryData, error) {
	qT := &pbm.TotalQueryData{
		QueryStat:           &pbm.QueryStat{},
		SegmentQueryMetrics: make([]*pbm.SegmentMetrics, 0),
	}
	s.mx.RLock()
	rQ, okQ := s.runningQueries[queryKey]
	s.mx.RUnlock()
	if okQ {
		err := AggregateQueryStat(qT, queryKey, rQ, tmID, AggMasterOnly)
		if err != nil {
			return nil, err
		}
	}
	return qT, nil
}
