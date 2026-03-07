package gp

import (
	"encoding/json"
	"fmt"
	"maps"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	pbm "github.com/open-gpdb/yagpcc/api/proto/agent_master"
	pbc "github.com/open-gpdb/yagpcc/api/proto/common"
	"github.com/open-gpdb/yagpcc/internal/metrics"
	"github.com/open-gpdb/yagpcc/internal/storage"
	"github.com/open-gpdb/yagpcc/internal/utils"
	"go.uber.org/zap"
)

const (
	MaxRecurseDepth = 1000
	UTCTimeFormat   = "2006-01-02T15:04:05.000000000-07:00"
)

var (
	StateIdle   = "IDLE"
	StateActive = "ACTIVE"
)

type UTCTime time.Time

func (t UTCTime) MarshalJSON() ([]byte, error) {
	s := time.Time(t).UTC().Format(UTCTimeFormat)
	return []byte(`"` + s + `"`), nil
}

type (
	SessionKeyWrite struct {
		SessID int
		TmID   int
	}

	QueryInfoShort struct {
		QueryID   uint64
		PlanID    uint64
		QueryText string
		PlanText  string
	}

	SessionDataWrite struct {
		CollectTime          UTCTime
		ClusterID            string
		Hostname             string
		RunningQueryStatus   int32
		GpStatInfo           *GpStatActivity
		RunningQuery         *storage.QueryKeyWrite
		RunningQueryInfo     *QueryInfoShort
		TotalGPMetrics       *pbc.GPMetrics
		LongRunningGPMetrics *pbc.GPMetrics
		QueryGPMetrics       *pbc.GPMetrics
		RunningQueryLevel    int64
		RunningQuerySlices   int64
	}

	RunningQueriesInfo struct {
		Ccnts []int32
	}

	SessionData struct {
		CollectTime          time.Time
		ClusterID            string
		Hostname             string
		GpStatInfo           *GpStatActivity
		RunningQueries       RunningQueriesInfo
		LastQuery            int32
		TotalGPMetrics       *pbc.GPMetrics
		LongRunningGPMetrics *pbc.GPMetrics
		QueryGPMetrics       *pbc.GPMetrics
	}

	SessionInfo struct {
		SessionData SessionData
		RefCounter  int
		SessionLock sync.RWMutex
	}

	SessionKey struct {
		SessID int
	}

	SessionMapT map[SessionKey]*SessionInfo

	SessionsStorage struct {
		mx        *sync.RWMutex
		sessMap   SessionMapT
		rqStorage *storage.RunningQueriesStorage
	}
)

var (
	DiscoveredTmID int64 = 0
	SystemUserName       = map[string]interface{}{
		"gpadmin": struct{}{},
		"repl":    struct{}{},
		"monitor": struct{}{},
	}
)

func NewSessionsStorage(rqStorage *storage.RunningQueriesStorage) *SessionsStorage {
	return &SessionsStorage{
		mx:        &sync.RWMutex{},
		sessMap:   make(SessionMapT, 0),
		rqStorage: rqStorage,
	}
}

func (s *SessionsStorage) ClearSessions() {
	s.mx.Lock()
	defer s.mx.Unlock()

	s.sessMap = make(SessionMapT, 0)
}

func NewRunningQueriesInfo() RunningQueriesInfo {
	return RunningQueriesInfo{
		Ccnts: make([]int32, 0),
	}
}

func (rq *RunningQueriesInfo) IsEmpty() bool {
	return len(rq.Ccnts) == 0
}

func (rq *RunningQueriesInfo) SetCurrentQuery(ccnt int32, level int) {
	if level == -1 {
		// calculate new level
		if len(rq.Ccnts) == 0 {
			level = 0
		} else {
			if rq.Ccnts[len(rq.Ccnts)-1] == ccnt {
				level = len(rq.Ccnts) - 1
			} else {
				level = len(rq.Ccnts)
			}
		}
	}
	if level >= MaxRecurseDepth {
		level = MaxRecurseDepth - 1
	}
	oldLen := len(rq.Ccnts)
	if oldLen > level+1 {
		rq.Ccnts = rq.Ccnts[:level+1]
	} else {
		for i := oldLen; i < level+1; i++ {
			rq.Ccnts = append(rq.Ccnts, -1)
		}
	}
	rq.Ccnts[level] = ccnt
}

func (rq *RunningQueriesInfo) EndCurrentQuery(ccnt int32, level int) {
	if len(rq.Ccnts) == 0 {
		return
	}
	if level == -1 {
		// find level based on ccnt
		for level = len(rq.Ccnts) - 1; level > 0 && rq.Ccnts[level] != ccnt; level-- {
		}
	}
	if level == 0 {
		rq.Ccnts = make([]int32, 0)
		return
	}
	if level >= MaxRecurseDepth {
		level = MaxRecurseDepth - 1
	}
	if level >= len(rq.Ccnts) {
		// level wasn't registered, nothing to do
		return
	}
	var newLevel int
	for newLevel = level - 1; newLevel > 0 && rq.Ccnts[newLevel] == -1; newLevel-- {
	}
	rq.Ccnts = rq.Ccnts[:newLevel+1]
}

func (rq *RunningQueriesInfo) GetCurrentQuery() int32 {
	if len(rq.Ccnts) == 0 {
		return -1
	}
	return rq.Ccnts[len(rq.Ccnts)-1]
}

func (sData *SessionData) GetLastQuery() int32 {
	return sData.LastQuery
}

func (p SessionDataWrite) ToJSON() ([]byte, error) {
	return json.Marshal(p)
}

func GetStatus(blockSessID *int, waiting *bool, reason *string) string {
	if blockSessID != nil && *blockSessID > 0 {
		return "blocked"
	}
	if reason != nil {
		if waiting != nil && *waiting && *reason == "resgroup" {
			return "waiting"
		}
		if waiting != nil && *waiting && *reason == "replication" {
			return "waiting"
		}
		if waiting != nil && *waiting && *reason == "lock" {
			return "blocked"
		}
	}
	return ""
}

func emptyStrForNil(s *string) string {
	if s == nil {
		return ""
	} else {
		return *s
	}
}

func emptyIntForNil(i *int) int64 {
	if i == nil {
		return 0
	} else {
		return int64(*i)
	}
}

func emptyTimestampForNil(t *time.Time) *timestamppb.Timestamp {
	if t == nil {
		return &timestamppb.Timestamp{}
	} else {
		return utils.GetTimestampFromTime(*t)
	}
}

func falseBoolForNil(b *bool) bool {
	if b == nil {
		return false
	} else {
		return *b
	}
}

func getSessInfo(gpStatInfo *GpStatActivity) *pbc.SessionInfo {
	if gpStatInfo == nil {
		return &pbc.SessionInfo{}
	}
	return &pbc.SessionInfo{
		Pid:              int64(gpStatInfo.Pid),
		Database:         gpStatInfo.Datname,
		User:             gpStatInfo.Usename,
		ApplicationName:  emptyStrForNil(gpStatInfo.ApplicationName),
		ClientAddr:       emptyStrForNil(gpStatInfo.ClientAddr),
		ClientHostname:   emptyStrForNil(gpStatInfo.ClientHostname),
		ClientPort:       emptyIntForNil(gpStatInfo.ClientPort),
		BackendStart:     emptyTimestampForNil(gpStatInfo.BackendStart),
		XactStart:        emptyTimestampForNil(gpStatInfo.XactStart),
		QueryStart:       emptyTimestampForNil(gpStatInfo.QueryStart),
		StateChange:      emptyTimestampForNil(gpStatInfo.StateChange),
		WaitingReason:    emptyStrForNil(gpStatInfo.WaitingReason),
		Waiting:          falseBoolForNil(gpStatInfo.Waiting),
		State:            strings.ToLower(emptyStrForNil(gpStatInfo.State)),
		BackendXid:       emptyStrForNil(gpStatInfo.BackendXid),
		BackendXmin:      emptyStrForNil(gpStatInfo.BackendXmin),
		Rsgid:            emptyIntForNil(gpStatInfo.Rsgid),
		Rsgname:          emptyStrForNil(gpStatInfo.Rsgname),
		Rsgqueueduration: emptyStrForNil(gpStatInfo.Rsgqueueduration),
		BlockedBySessId:  emptyIntForNil(gpStatInfo.BlockedBySessID),
		WaitMode:         emptyStrForNil(gpStatInfo.WaitMode),
		LockedItem:       emptyStrForNil(gpStatInfo.LockedItem),
		LockedMode:       emptyStrForNil(gpStatInfo.LockedMode),
	}
}

func NotSystemSession(sessInfo *SessionInfo) bool {
	if sessInfo.SessionData.GpStatInfo == nil {
		return false
	}
	_, ok := SystemUserName[sessInfo.SessionData.GpStatInfo.Usename]
	if sessInfo.SessionData.GpStatInfo.Usename == "" || ok {
		return false
	}
	return true
}

func getRunningQKey(key *storage.QueryKey) *pbc.QueryKey {
	if key == nil {
		return &pbc.QueryKey{}
	}
	return &pbc.QueryKey{
		Ssid: key.Ssid,
		Ccnt: key.Ccnt,
		Tmid: int32(DiscoveredTmID),
	}
}

func getRunningQInfo(info *pbc.QueryInfo) *pbc.QueryInfo {
	if info == nil {
		return &pbc.QueryInfo{}
	}
	return info
}

func GetRunningCcnt(sData *SessionData, qType pbm.RunningQueryType) int32 {
	if sData == nil {
		return -1
	}
	ccnt := sData.GetLastQuery()
	if qType == pbm.RunningQueryType_RQT_TOP {
		if len(sData.RunningQueries.Ccnts) > 0 {
			ccnt = sData.RunningQueries.Ccnts[0]
			// interesting, here we could get -1 as ccnt (for example, we missed some nested query). Well, in that case we should return last_query
			if ccnt == 0 || ccnt == -1 {
				ccnt = sData.GetLastQuery()
			}
		}
	}
	// -1 here means no registered query - return -1 if ccnt==0
	if ccnt == 0 {
		return -1
	}
	return ccnt
}

func (s *SessionsStorage) GetRunningSessionQuery(keyS SessionKey, ccnt int32) (*storage.RunningQueryInfo, error) {
	if ccnt == -1 {
		return &storage.RunningQueryInfo{}, nil
	}
	tmID := atomic.LoadInt64(&DiscoveredTmID)
	qKey := storage.QueryKey{Ssid: int32(keyS.SessID), Ccnt: ccnt}
	queryData, err := s.rqStorage.GetQueryInfo(qKey, tmID)
	if err != nil {
		return nil, err
	}
	qStart, qEnd, qSubmit := time.Time{}, time.Time{}, time.Time{}
	if queryData.QueryStat != nil && queryData.QueryStat.QueryInfo != nil {
		if queryData.QueryStat.QueryInfo.StartTime != nil {
			qStart = queryData.QueryStat.QueryInfo.StartTime.AsTime()
		}
		if queryData.QueryStat.QueryInfo.EndTime != nil {
			qEnd = queryData.QueryStat.QueryInfo.EndTime.AsTime()
		}
		if queryData.QueryStat.QueryInfo.SubmitTime != nil {
			qSubmit = queryData.QueryStat.QueryInfo.SubmitTime.AsTime()
		}

	}
	return &storage.RunningQueryInfo{
		TotalQueryMetrics: queryData.QueryStat.TotalQueryMetrics,
		TotalQueryInfo:    queryData.QueryStat.QueryInfo,
		QueryStatus:       int32(queryData.QueryStat.QueryStatus),
		QueryMessage:      queryData.QueryStat.Message,
		QueryKey:          &qKey,
		Slices:            queryData.QueryStat.Slices,
		QueryStart:        qStart,
		QuerySubmit:       qSubmit,
		QueryEnd:          qEnd,
	}, nil

}

func (s *SessionsStorage) getRunningQueryStack(sKey SessionKey, ccnts []int32) ([]*pbc.QueryDesc, error) {
	result := make([]*pbc.QueryDesc, 0)
	for index, stackCcnt := range ccnts {
		if stackCcnt == -1 {
			continue
		}
		runningQ, err := s.GetRunningSessionQuery(sKey, stackCcnt)
		if err != nil {
			return nil, err
		}
		qDesc := pbc.QueryDesc{
			QueryKey:    getRunningQKey(runningQ.QueryKey),
			QueryInfo:   getRunningQInfo(runningQ.TotalQueryInfo),
			QueryStatus: pbc.QueryStatus(runningQ.QueryStatus),
			QueryLevel:  int64(index),
			QueryStart:  utils.GetTimestampFromTime(runningQ.QueryStart),
		}
		result = append(result, &qDesc)
	}
	return result, nil
}

func (s *SessionsStorage) GetQueryDesc(qKey *storage.QueryKey, showStack bool) (*pbc.SessionState, error) {

	if qKey == nil {
		return nil, fmt.Errorf("nil query key given")
	}
	now := time.Now()
	tmID := atomic.LoadInt64(&DiscoveredTmID)
	queryData, err := s.rqStorage.GetQueryData(*qKey, tmID)
	if err != nil {
		return nil, err
	}

	keyS := SessionKey{SessID: int(qKey.Ssid)}
	s.mx.RLock()
	sessionData, okS := s.sessMap[keyS]
	s.mx.RUnlock()

	sessState := pbc.SessionState{
		Time:                utils.GetTimestampFromTime(now),
		SessionKey:          &pbc.SessionKey{SessId: int64(qKey.Ssid), TmId: int64(DiscoveredTmID)},
		SessionInfo:         &pbc.SessionInfo{},
		RunningQuery:        getRunningQKey(qKey),
		RunningQueryInfo:    getRunningQInfo(queryData.QueryStat.QueryInfo),
		RunningQueryStatus:  pbc.QueryStatus(queryData.QueryStat.QueryStatus),
		TotalMetrics:        &pbc.GPMetrics{},
		LastMetrics:         &pbc.GPMetrics{},
		QueryMetrics:        proto.Clone(queryData.QueryStat.TotalQueryMetrics).(*pbc.GPMetrics),
		RunningQueryLevel:   0,
		RunningQueryError:   queryData.QueryStat.Message,
		RunningQuerySlices:  queryData.QueryStat.Slices,
		RunningQueriesStack: make([]*pbc.QueryDesc, 0),
	}

	if okS {
		sessionData.SessionLock.RLock()
		sessState.RunningQueryLevel = int64(len(sessionData.SessionData.RunningQueries.Ccnts))
		sessState.SessionInfo = getSessInfo(sessionData.SessionData.GpStatInfo)
		sessState.LastMetrics = proto.Clone(sessionData.SessionData.LongRunningGPMetrics).(*pbc.GPMetrics)
		ccnt := GetRunningCcnt(&sessionData.SessionData, pbm.RunningQueryType_RQT_TOP)
		ccnts := make([]int32, len(sessionData.SessionData.RunningQueries.Ccnts))
		copy(ccnts, sessionData.SessionData.RunningQueries.Ccnts)
		sessionData.SessionLock.RUnlock()
		if showStack && ccnt != -1 {
			queryStack, err := s.getRunningQueryStack(keyS, ccnts)
			if err == nil {
				sessState.RunningQueriesStack = append(sessState.RunningQueriesStack, queryStack...)
			}
		}
	}

	return &sessState, nil
}

func (s *SessionsStorage) GetSessionDesc(
	keyS SessionKey,
	valS *SessionInfo,
	showStack bool,
	runningQType pbm.RunningQueryType,
	ccnt int32,
) (*pbc.SessionState, error) {

	now := time.Now()
	valS.SessionLock.RLock()
	if ccnt == -1 {
		ccnt = GetRunningCcnt(&valS.SessionData, runningQType)
	}
	valS.SessionLock.RUnlock()
	runningQ, err := s.GetRunningSessionQuery(keyS, ccnt)
	if err != nil {
		return nil, err
	}
	valS.SessionLock.RLock()
	level := len(valS.SessionData.RunningQueries.Ccnts)
	sessState := pbc.SessionState{
		Time:               utils.GetTimestampFromTime(now),
		SessionKey:         &pbc.SessionKey{SessId: int64(keyS.SessID), TmId: int64(DiscoveredTmID)},
		SessionInfo:        getSessInfo(valS.SessionData.GpStatInfo),
		RunningQuery:       getRunningQKey(runningQ.QueryKey),
		RunningQueryInfo:   getRunningQInfo(runningQ.TotalQueryInfo),
		RunningQueryStatus: pbc.QueryStatus(runningQ.QueryStatus),
		TotalMetrics:       proto.Clone(valS.SessionData.TotalGPMetrics).(*pbc.GPMetrics),
		LastMetrics:        proto.Clone(valS.SessionData.LongRunningGPMetrics).(*pbc.GPMetrics),
		QueryMetrics:       proto.Clone(valS.SessionData.QueryGPMetrics).(*pbc.GPMetrics),
		RunningQueryLevel:  int64(level),
		RunningQueryError:  runningQ.QueryMessage,
		RunningQuerySlices: runningQ.Slices,
	}
	ccnts := make([]int32, len(valS.SessionData.RunningQueries.Ccnts))
	copy(ccnts, valS.SessionData.RunningQueries.Ccnts)
	if !runningQ.QueryStart.IsZero() {
		sessState.SessionInfo.QueryStart = utils.GetTimestampFromTime(runningQ.QueryStart)
	}
	valS.SessionLock.RUnlock()
	if showStack && ccnt != -1 {
		queryStack, err := s.getRunningQueryStack(keyS, ccnts)
		if err == nil {
			sessState.RunningQueriesStack = append(sessState.RunningQueriesStack, queryStack...)
		}
	}

	return &sessState, nil
}

func copyGPStatInfo(from *GpStatActivity) *GpStatActivity {
	if from == nil {
		return nil
	}
	copyGpStatActivity := *from
	return &copyGpStatActivity
}

func (s *SessionsStorage) GetSessionDataForWrite(
	clusterID string,
	hostname string,
	keyS SessionKey,
	valS *SessionInfo,
	runningQType pbm.RunningQueryType) (*SessionDataWrite, error) {
	sessState, err := s.GetSessionDesc(keyS, valS, false, runningQType, -1)
	if err != nil {
		return nil, err
	}
	valS.SessionLock.RLock()
	defer valS.SessionLock.RUnlock()
	return &SessionDataWrite{
		CollectTime:        UTCTime(time.Now()),
		ClusterID:          clusterID,
		Hostname:           hostname,
		RunningQueryStatus: int32(sessState.RunningQueryStatus),
		GpStatInfo:         copyGPStatInfo(valS.SessionData.GpStatInfo),
		RunningQuery:       &storage.QueryKeyWrite{Ssid: sessState.RunningQuery.Ssid, Ccnt: sessState.RunningQuery.Ccnt, Tmid: sessState.RunningQuery.Tmid},
		RunningQueryInfo: &QueryInfoShort{
			QueryID:   sessState.RunningQueryInfo.QueryId,
			PlanID:    sessState.RunningQueryInfo.PlanId,
			QueryText: sessState.RunningQueryInfo.QueryText,
			PlanText:  sessState.RunningQueryInfo.PlanText,
		},
		TotalGPMetrics:       sessState.TotalMetrics,
		LongRunningGPMetrics: sessState.LastMetrics,
		QueryGPMetrics:       sessState.QueryMetrics,
		RunningQueryLevel:    sessState.RunningQueryLevel,
		RunningQuerySlices:   sessState.RunningQuerySlices,
	}, nil

}

func (s *SessionsStorage) GetAllSessions(showSystem bool, runningQType pbm.RunningQueryType) (*pbm.GetGPSessionsResponse, error) {
	result := &pbm.GetGPSessionsResponse{
		SessionsState: make([]*pbc.SessionState, 0),
	}
	s.mx.RLock()
	sMap := maps.Clone(s.sessMap)
	s.mx.RUnlock()
	for keyS, valS := range sMap {
		valS.SessionLock.RLock()
		notSystem := NotSystemSession(valS)
		valS.SessionLock.RUnlock()
		if showSystem || notSystem {
			sessState, err := s.GetSessionDesc(keyS, valS, true, runningQType, -1)
			if err == nil {
				result.SessionsState = append(result.SessionsState, sessState)
			}
		}
	}
	return result, nil
}

func (s *SessionsStorage) RefreshSessionList(l *zap.SugaredLogger, newList []*GpStatActivity, clearDeletedSessions bool) error {

	l.Debug("Refreshing session list")
	refreshedSessID := make(map[SessionKey]bool, 0)
	for _, valN := range newList {
		// rewrite state for blocked sessions
		status := GetStatus(valN.BlockedBySessID, valN.Waiting, valN.WaitingReason)
		if status != "" {
			valN.State = &status
		}
		sKey := SessionKey{
			SessID: valN.SessID,
		}
		refreshedSessID[sKey] = true
		s.mx.RLock()
		valS, okS := s.sessMap[sKey]
		s.mx.RUnlock()
		if okS {
			valS.SessionLock.Lock()
			valS.SessionData.GpStatInfo = valN
			valS.SessionLock.Unlock()
		} else {
			newSessI := SessionInfo{
				SessionData: SessionData{
					GpStatInfo:           valN,
					RunningQueries:       NewRunningQueriesInfo(),
					TotalGPMetrics:       &pbc.GPMetrics{},
					LongRunningGPMetrics: &pbc.GPMetrics{},
					QueryGPMetrics:       &pbc.GPMetrics{},
				},
				RefCounter: 0,
			}
			s.mx.Lock()
			s.sessMap[sKey] = &newSessI
			s.mx.Unlock()
			if metrics.YagpccMetrics != nil {
				metrics.YagpccMetrics.NewSessions.Inc()
			}
		}
	}

	if clearDeletedSessions {
		deletedKeys := make([]struct {
			SessionKey
			*SessionInfo
		}, 0)

		// perform minimal amount of work under the lock
		s.mx.Lock()
		for keyS, valS := range s.sessMap {
			okS := refreshedSessID[keyS]
			if !okS {
				delete(s.sessMap, keyS)
				deletedKeys = append(deletedKeys, struct {
					SessionKey
					*SessionInfo
				}{keyS, valS})
			}
		}
		s.mx.Unlock()

		// show what has been deleted
		for _, keyPair := range deletedKeys {
			keyPair.SessionLock.RLock()
			refCounter := keyPair.RefCounter
			keyPair.SessionLock.RUnlock()
			// we expect here 1 refcounter because we store the last query
			if refCounter > 1 {
				l.Infof("Delete session %v with refcounter %v", keyPair.SessionKey, refCounter)
			}
			l.Debugf("Delete session %v", keyPair.SessionKey)
			if metrics.YagpccMetrics != nil {
				metrics.YagpccMetrics.DeletedSessions.Inc()
			}
		}
	}

	if metrics.YagpccMetrics != nil {
		s.mx.RLock()
		defer s.mx.RUnlock()
		metrics.YagpccMetrics.TotalSessions.Set(float64(len(s.sessMap)))
	}
	return nil
}

func (s *SessionsStorage) RegisterNewSessionQuery(valS *SessionInfo, okS bool, qKey *pbc.QueryKey, qI *pbc.QueryInfo, level int) *SessionInfo {
	sTime := time.Now()
	if okS {
		valS.SessionLock.Lock()
		valS.SessionData.RunningQueries.SetCurrentQuery(qKey.Ccnt, level)
		valS.SessionData.GpStatInfo.State = &StateActive
		valS.SessionData.GpStatInfo.StateChange = &sTime
		valS.RefCounter += 1
		valS.SessionLock.Unlock()
	} else {
		valS = &SessionInfo{
			SessionData: SessionData{
				RunningQueries: NewRunningQueriesInfo(),
				GpStatInfo: &GpStatActivity{
					SessID:      int(qKey.Ssid),
					TmID:        int(qKey.Tmid),
					State:       &StateActive,
					StateChange: &sTime,
				},
				TotalGPMetrics:       &pbc.GPMetrics{},
				LongRunningGPMetrics: &pbc.GPMetrics{},
				QueryGPMetrics:       &pbc.GPMetrics{},
			},
			RefCounter: 1,
		}
		valS.SessionData.RunningQueries.SetCurrentQuery(qKey.Ccnt, level)
		s.mx.Lock()
		s.sessMap[SessionKey{SessID: int(qKey.Ssid)}] = valS
		s.mx.Unlock()
		if metrics.YagpccMetrics != nil {
			metrics.YagpccMetrics.NewSessions.Inc()
		}
	}
	return valS
}

func (s *SessionsStorage) UpdateSessionStat(queryStat *pbm.QueryStat) error {
	if queryStat == nil || queryStat.QueryKey == nil {
		return fmt.Errorf("nil as query stat")
	}
	s.mx.RLock()
	valS, okS := s.sessMap[SessionKey{
		SessID: int(queryStat.QueryKey.Ssid),
	}]
	s.mx.RUnlock()
	if !okS {
		return nil
	}
	valS.SessionLock.Lock()
	defer valS.SessionLock.Unlock()
	intermediateResults := make(map[storage.MapAggregateKey]uint64, 0)
	return storage.GroupGPMetrics(valS.SessionData.TotalGPMetrics, queryStat.TotalQueryMetrics, storage.AggMax, "hostname", intermediateResults)
}

func (s *SessionsStorage) UpdateSessionQuery(
	qKey *pbc.QueryKey,
	qI *pbc.QueryInfo,
	qStatus int32,
	addQInfo *pbc.AdditionalQueryInfo,
	newQuery bool,
) error {
	// here we should create new session

	oldID := atomic.LoadInt64(&DiscoveredTmID)
	if oldID < int64(qKey.Tmid) {
		atomic.CompareAndSwapInt64(&DiscoveredTmID, oldID, int64(qKey.Tmid))
	}

	level := -1
	if addQInfo != nil {
		level = int(addQInfo.NestedLevel)
	}

	s.mx.RLock()
	valS, okS := s.sessMap[SessionKey{
		SessID: int(qKey.Ssid),
	}]
	s.mx.RUnlock()

	if newQuery || !okS {
		valS = s.RegisterNewSessionQuery(valS, okS, qKey, qI, level)
	}

	if valS != nil {
		valS.SessionLock.Lock()
		defer valS.SessionLock.Unlock()

		// register last running query
		valS.SessionData.LastQuery = qKey.Ccnt

		// refresh username and database data
		if qI != nil {
			valS.SessionData.GpStatInfo.Usename = max(qI.UserName, valS.SessionData.GpStatInfo.Usename)
			valS.SessionData.GpStatInfo.Datname = max(qI.DatabaseName, valS.SessionData.GpStatInfo.Datname)
			newRsgName := qI.Rsgname
			if valS.SessionData.GpStatInfo.Rsgname != nil {
				newRsgName = max(qI.Rsgname, *valS.SessionData.GpStatInfo.Rsgname)
			}
			valS.SessionData.GpStatInfo.Rsgname = &newRsgName
		}

		// mark query as ende
		if storage.CheckQueryEnded(qStatus) {
			valS.SessionData.RunningQueries.EndCurrentQuery(qKey.Ccnt, level)
			qTime := time.Now()
			valS.SessionData.GpStatInfo.StateChange = &qTime
			if valS.SessionData.RunningQueries.IsEmpty() {
				valS.SessionData.GpStatInfo.State = &StateIdle
			} else {
				valS.SessionData.GpStatInfo.State = &StateActive
			}
		}

	}

	return nil
}

func (s *SessionsStorage) CanLock() bool {
	if s.mx.TryRLock() {
		s.mx.RUnlock()
		return true
	}
	return false
}

func (s *SessionsStorage) SessionsCount() int {
	s.mx.RLock()
	defer s.mx.RUnlock()
	return len(s.sessMap)
}

func (s *SessionsStorage) GetSessions() SessionMapT {
	s.mx.RLock()
	defer s.mx.RUnlock()
	return maps.Clone(s.sessMap)
}

func (s *SessionsStorage) GetSession(key SessionKey) (*SessionInfo, bool) {
	s.mx.RLock()
	defer s.mx.RUnlock()
	val, ok := s.sessMap[key]
	return val, ok
}
