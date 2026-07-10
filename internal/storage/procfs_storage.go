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

package storage

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	pbc "github.com/open-gpdb/yagpcc/api/proto/common"
	"github.com/open-gpdb/yagpcc/internal/utils"
)

type (
	ProcKey struct {
		GpSegmentId int64
		SessId      int64
		Pid         int64
	}

	ProcIndexKey struct {
		SessId int64
	}

	ProcIndexData struct {
		GpSegmentId int64
		Pid         int64
	}

	QueryCellKey struct {
		SliceID  int64
		Segindex int32
	}

	ProcfsCellMetric struct {
		QueryCellKey
		RuntimeMetrics *pbc.RuntimeMetrics
		HasData        bool
	}

	ProcStat struct {
		Cmdline    string
		State      string
		Ccnt       int32
		SliceId    int64
		ProcStat   *pbc.ProcStat
		ProcStatus *pbc.ProcStatus
		ProcIO     *pbc.ProcIO
		ProcSpill  *pbc.ProcSpill
	}
)

// ToGpPidProcInfo converts a ProcStat and its associated ProcKey back into a proto GpPidProcInfo message.
func (ps *ProcStat) ToGpPidProcInfo(key ProcKey) *pbc.GpPidProcInfo {
	return &pbc.GpPidProcInfo{
		GpSegmentId: key.GpSegmentId,
		SessId:      key.SessId,
		Pid:         key.Pid,
		Cmdline:     ps.Cmdline,
		State:       ps.State,
		Ccnt:        ps.Ccnt,
		SliceId:     ps.SliceId,
		ProcStat:    ps.ProcStat,
		ProcStatus:  ps.ProcStatus,
		ProcIo:      ps.ProcIO,
		ProcSpill:   ps.ProcSpill,
	}
}

type (
	ProcMap      map[ProcKey]*ProcStat
	ProcIndexMap map[ProcIndexKey][]*ProcIndexData

	HostLoadAvg struct {
		Avg1  float64
		Avg5  float64
		Avg15 float64
	}

	HostStat struct {
		LoadAvg  *HostLoadAvg
		CPUUsage float64
	}

	HostStatMap map[string]*HostStat

	HostRunningQueriesInfo struct {
		HostName      string
		Segindex      []int32
		ActiveQueries int64
		ActiveSlices  int64
		TotalSessions int64
		CPUUsage      float64
		Avg5          float64
		MemoryUsage   int64
		DiskUsage     int64
		DiskReads     int64
		DiskWrites    int64
		SpillBytes    int64
		Skew          *pbc.Skew
		DataQuality   *pbc.DataQuality
	}

	HostRunningQueryDetailInfo struct {
		QueryKey       *pbc.QueryKey
		UserName       string
		DbName         string
		QueryText      string
		RuntimeMetrics *pbc.RuntimeMetrics
		Skew           *pbc.Skew
		DataQuality    *pbc.DataQuality
		State          string
		IsIdle         bool
	}

	HostRunningQueriesDetailInfo struct {
		HostName      string
		Segindex      []int32
		Queries       []*HostRunningQueryDetailInfo
		DataQuality   *pbc.DataQuality
		NextPageToken string
	}

	ProcfsStatType struct {
		statTime       time.Time
		pidProcData    ProcMap
		pidProcIndex   ProcIndexMap
		hostStat       HostStatMap
		hostsExpected  int64
		hostsResponded int64
	}

	ProcfsStorage struct {
		mx                  *sync.RWMutex
		procfsStat          []ProcfsStatType
		maximumStoredPoints int
	}
)

type ProcfsOption = func(*ProcfsStorage)

const (
	defaultStoredPoints       = 30
	procfsQueryMasterSliceID  = int64(0)
	procfsQuerySegmentSliceID = utils.ProcfsQuerySegmentSliceID
)

func NewProcfsStorage(opts ...ProcfsOption) *ProcfsStorage {
	p := &ProcfsStorage{
		mx:                  &sync.RWMutex{},
		maximumStoredPoints: defaultStoredPoints,
	}
	for _, opt := range opts {
		opt(p)
	}
	p.procfsStat = make([]ProcfsStatType, 0, p.maximumStoredPoints)
	return p
}

func WithMaximumStoredPoints(maximumStoredPoints int) ProcfsOption {
	return func(p *ProcfsStorage) {
		if maximumStoredPoints < 1 {
			maximumStoredPoints = 1
		}
		p.maximumStoredPoints = maximumStoredPoints
	}
}

func (p *ProcfsStorage) TidyUpProcfsStat() {
	p.mx.Lock()
	defer p.mx.Unlock()
	if len(p.procfsStat) > p.maximumStoredPoints {
		firstSurvive := len(p.procfsStat) - p.maximumStoredPoints
		p.procfsStat = p.procfsStat[firstSurvive:]
	}
}

func (p *ProcfsStorage) RegisterProcfsStat(statTime time.Time, procfsStat []*pbc.GpPidProcInfo) {
	respondedHosts := countProcfsHosts(procfsStat)
	p.RegisterProcfsStatWithDataQuality(statTime, procfsStat, respondedHosts, respondedHosts)
}

func countProcfsHosts(procfsStat []*pbc.GpPidProcInfo) int64 {
	hosts := make(map[string]struct{})
	for _, proc := range procfsStat {
		if proc == nil || proc.GpSegmentId < 0 {
			continue
		}
		hosts[GetHostnameForSegindex(int32(proc.GpSegmentId))] = struct{}{}
	}
	return int64(len(hosts))
}

func (p *ProcfsStorage) RegisterProcfsStatWithDataQuality(statTime time.Time, procfsStat []*pbc.GpPidProcInfo, hostsExpected int64, hostsResponded int64) {
	p.RegisterProcfsStatWithHostStat(statTime, procfsStat, nil, hostsExpected, hostsResponded)
}

func (p *ProcfsStorage) RegisterProcfsStatWithHostStat(statTime time.Time, procfsStat []*pbc.GpPidProcInfo, hostStat HostStatMap, hostsExpected int64, hostsResponded int64) {
	stat := ProcfsStatType{
		statTime:       statTime,
		pidProcData:    make(ProcMap, len(procfsStat)),
		pidProcIndex:   make(ProcIndexMap, len(procfsStat)),
		hostStat:       make(HostStatMap, len(hostStat)),
		hostsExpected:  hostsExpected,
		hostsResponded: hostsResponded,
	}
	for hostname, hostStatValue := range hostStat {
		stat.hostStat[hostname] = hostStatValue
	}
	// create map for fast access
	for _, proc := range procfsStat {
		stat.pidProcData[ProcKey{
			GpSegmentId: proc.GpSegmentId,
			SessId:      proc.SessId,
			Pid:         proc.Pid,
		}] = &ProcStat{
			Cmdline:    proc.Cmdline,
			State:      proc.State,
			Ccnt:       proc.Ccnt,
			SliceId:    proc.SliceId,
			ProcStat:   proc.ProcStat,
			ProcStatus: proc.ProcStatus,
			ProcIO:     proc.ProcIo,
			ProcSpill:  proc.ProcSpill,
		}
		key := ProcIndexKey{
			SessId: proc.SessId,
		}
		sessIDData, ok := stat.pidProcIndex[key]
		if !ok {
			sessIDData = make([]*ProcIndexData, 0, 10)
		}
		sessIDData = append(sessIDData, &ProcIndexData{
			GpSegmentId: proc.GpSegmentId,
			Pid:         proc.Pid,
		})
		stat.pidProcIndex[key] = sessIDData
	}

	// store new map
	p.mx.Lock()
	p.procfsStat = append(p.procfsStat, stat)
	p.mx.Unlock()

	// delete old data
	p.TidyUpProcfsStat()
}

func procfsDataQuality(stat *ProcfsStatType) *pbc.DataQuality {
	if stat == nil {
		return CalculateDataQuality(0, 0, time.Time{}, time.Now())
	}
	return CalculateDataQuality(stat.hostsExpected, stat.hostsResponded, stat.statTime, time.Now())
}

func absDuration(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}

func (p *ProcfsStorage) getNearestNTime(d time.Duration) (*ProcfsStatType, error) {
	p.mx.RLock()
	defer p.mx.RUnlock()

	return p.getNearestNTimeUnlocked(d)
}

// getNearestNTimeUnlocked searches for the nearest snapshot without acquiring a lock.
// Callers must hold at least p.mx.RLock before calling this method.
// Returns the ProcfsStatType snapshot closest to duration d in the past, or an error if no data exists.
func (p *ProcfsStorage) getNearestNTimeUnlocked(d time.Duration) (*ProcfsStatType, error) {
	if len(p.procfsStat) == 0 {
		return nil, errors.New("no data in procfsStat")
	}

	lastIdx := len(p.procfsStat) - 1
	currentTime := p.procfsStat[lastIdx].statTime
	minAbsDiff := absDuration(d) // worst case: the last element itself (diff=0, absDiff=|d-0|=d)
	minIndex := lastIdx

	for i := range p.procfsStat {
		idx := lastIdx - i
		currentDiff := currentTime.Sub(p.procfsStat[idx].statTime)
		absDiff := absDuration(d - currentDiff)
		if absDiff <= minAbsDiff {
			minAbsDiff = absDiff
			minIndex = idx
			continue
		}
		// Since times are sorted ascending, currentDiff is monotonically increasing.
		// Once the absolute difference starts growing, it will only get worse.
		break
	}

	return &p.procfsStat[minIndex], nil
}

func (p *ProcfsStorage) getNMin(d time.Duration) (*ProcfsStatType, *ProcfsStatType, error) {
	p.mx.RLock()
	defer p.mx.RUnlock()

	nearest, err := p.getNearestNTimeUnlocked(d)
	if err != nil {
		return nil, nil, fmt.Errorf("fail in get %s interval: %w", d, err)
	}
	return nearest, &p.procfsStat[len(p.procfsStat)-1], nil
}

func (p *ProcfsStorage) get5Min() (*ProcfsStatType, *ProcfsStatType, error) {
	return p.getNMin(5 * time.Minute)
}

func hostDataQuality(hostname string, last *ProcfsStatType) *pbc.DataQuality {
	if last == nil {
		return CalculateDataQuality(1, 0, time.Time{}, time.Now())
	}
	if _, ok := last.hostStat[hostname]; ok {
		return CalculateDataQuality(1, 1, last.statTime, time.Now())
	}
	localHostname, err := os.Hostname()
	if err != nil || localHostname == "" {
		localHostname = "localhost"
	}
	for key := range last.pidProcData {
		if key.GpSegmentId < 0 {
			if hostname == localHostname {
				return CalculateDataQuality(1, 1, last.statTime, time.Now())
			}
			continue
		}
		if GetHostnameForSegindex(int32(key.GpSegmentId)) == hostname {
			return CalculateDataQuality(1, 1, last.statTime, time.Now())
		}
	}
	return CalculateDataQuality(1, 0, last.statTime, time.Now())
}

func configuredHostSegmentsWithFallback(last *ProcfsStatType) map[string][]int32 {
	hostSegments := GetConfiguredHostSegments()
	if len(hostSegments) > 0 || last == nil {
		return hostSegments
	}
	for key := range last.pidProcData {
		if key.GpSegmentId < 0 {
			continue
		}
		hostname := GetHostnameForSegindex(int32(key.GpSegmentId))
		hostSegments[hostname] = append(hostSegments[hostname], int32(key.GpSegmentId))
	}
	for hostname := range hostSegments {
		sort.Slice(hostSegments[hostname], func(i, j int) bool { return hostSegments[hostname][i] < hostSegments[hostname][j] })
	}
	return hostSegments
}

func (p *ProcfsStorage) get15Min() (*ProcfsStatType, *ProcfsStatType, error) {
	return p.getNMin(15 * time.Minute)
}

func (p *ProcfsStorage) get30Min() (*ProcfsStatType, *ProcfsStatType, error) {
	return p.getNMin(30 * time.Minute)
}

func (p *ProcfsStorage) getProcfsSession(first, last *ProcfsStatType, sessId int64) (*pbc.GpPidProcInfo, error) {
	lastId, okLast := last.pidProcIndex[ProcIndexKey{SessId: sessId}]
	if !okLast {
		return nil, fmt.Errorf("session %d not found", sessId)
	}
	var err error
	result := &pbc.GpPidProcInfo{}
	intermediateResults := make(map[MapAggregateKey]int64, 0)
	for _, id := range lastId {
		lastKey := ProcKey{
			SessId:      sessId,
			GpSegmentId: id.GpSegmentId,
			Pid:         id.Pid}
		lastProcData, okLast := last.pidProcData[lastKey]
		if !okLast {
			continue
		}
		diff := lastProcData.ToGpPidProcInfo(lastKey)
		firstKey := ProcKey{
			SessId:      sessId,
			GpSegmentId: id.GpSegmentId,
			Pid:         id.Pid}
		firstProcData, okFirst := first.pidProcData[firstKey]
		if okFirst {
			diff, err = ProcfsDiff(firstProcData.ToGpPidProcInfo(firstKey), diff)
			if err != nil {
				return nil, fmt.Errorf("fail in diff: %w", err)
			}
		}
		err = GroupProcfsMetrics(result, diff, AggSegmentHost, GetHostnameForSegindex(int32(lastKey.GpSegmentId)), intermediateResults)
		if err != nil {
			return nil, fmt.Errorf("fail in group: %w", err)
		}
	}
	return result, nil
}

func (p *ProcfsStorage) GetProcfsSessions(sessIds []int64) (map[int64]*pbc.GpPidProcInfo, error) {
	result := make(map[int64]*pbc.GpPidProcInfo, len(sessIds))
	first, last, err := p.get5Min()
	if err != nil {
		return nil, fmt.Errorf("fail in get 5 min: %w", err)
	}
	for _, sessId := range sessIds {
		sessStat, err := p.getProcfsSession(first, last, sessId)
		if err != nil {
			// session could vanish from the map - it's not a problem, just skip it
			continue
		}
		result[sessId] = sessStat
	}
	return result, nil
}

func CalculateSkew(values map[int32]float64) *pbc.Skew {
	result := &pbc.Skew{}
	if len(values) == 0 {
		return result
	}

	segindexes := make([]int32, 0, len(values))
	for segindex := range values {
		segindexes = append(segindexes, segindex)
	}
	sort.Slice(segindexes, func(i, j int) bool { return segindexes[i] < segindexes[j] })

	var sum float64
	maxValue := values[segindexes[0]]
	hotSegindex := segindexes[0]
	for _, segindex := range segindexes {
		value := values[segindex]
		sum += value
		if value > maxValue {
			maxValue = value
			hotSegindex = segindex
		}
	}

	result.Segindex = hotSegindex
	if maxValue <= 0 {
		return result
	}
	avg := sum / float64(len(values))
	result.Skew = 1 - avg/maxValue
	if result.Skew < 0 {
		result.Skew = 0
	}
	return result
}

func CalculateDataQuality(segmentsExpected, segmentsReceived int64, statTime time.Time, now time.Time) *pbc.DataQuality {
	freshness := int64(0)
	if !statTime.IsZero() {
		freshness = now.Sub(statTime).Milliseconds()
		if freshness < 0 {
			freshness = 0
		}
	}
	return &pbc.DataQuality{
		SegmentsExpected: segmentsExpected,
		SegmentsReceived: segmentsReceived,
		IsPartial:        segmentsReceived < segmentsExpected,
		FreshnessMs:      freshness,
	}
}

func addProcIO(dest *pbc.ProcIO, source *pbc.ProcIO) *pbc.ProcIO {
	if source == nil {
		return dest
	}
	if dest == nil {
		dest = &pbc.ProcIO{}
	}
	dest.Rchar += source.Rchar
	dest.Wchar += source.Wchar
	dest.Syscr += source.Syscr
	dest.Syscw += source.Syscw
	dest.ReadBytes += source.ReadBytes
	dest.WriteBytes += source.WriteBytes
	dest.CancelledWriteBytes += source.CancelledWriteBytes
	return dest
}

func addProcSpill(dest *pbc.ProcSpill, source *pbc.ProcSpill) *pbc.ProcSpill {
	if source == nil {
		return dest
	}
	if dest == nil {
		dest = &pbc.ProcSpill{}
	}
	dest.Size += source.Size
	dest.Files += source.Files
	return dest
}

func isIdleProcState(state string) bool {
	return strings.EqualFold(strings.TrimSpace(state), "idle")
}

func addProcfsRuntimeMetrics(dest *pbc.RuntimeMetrics, source *pbc.GpPidProcInfo) *pbc.RuntimeMetrics {
	if source == nil {
		return dest
	}
	if dest == nil {
		dest = &pbc.RuntimeMetrics{}
	}
	if source.ProcStat != nil {
		dest.Utime += source.ProcStat.Utime
		dest.Stime += source.ProcStat.Stime
	}
	if source.ProcStatus != nil {
		dest.VmPeak += source.ProcStatus.VmPeak
		dest.VmRss += source.ProcStatus.VmRss
	}
	if source.State != "" && (dest.State == "" || isIdleProcState(dest.State) || !isIdleProcState(source.State)) {
		dest.State = source.State
	}
	dest.ProcIo = addProcIO(dest.ProcIo, source.ProcIo)
	dest.ProcSpill = addProcSpill(dest.ProcSpill, source.ProcSpill)
	return dest
}

func procfsMatchesQuery(procData *ProcStat, ccnt int32) bool {
	if ccnt != 0 && procData.Ccnt != ccnt {
		return false
	}
	return procData.SliceId != UnsetSliceId || !isIdleProcState(procData.State)
}

func procfsCellForProc(segindex int32, sliceID int64) QueryCellKey {
	if sliceID != UnsetSliceId {
		return QueryCellKey{SliceID: sliceID, Segindex: segindex}
	}
	return QueryCellKey{SliceID: procfsQuerySegmentSliceID, Segindex: segindex}
}

func (p *ProcfsStorage) getProcfsQueryRuntimeMetrics(first, last *ProcfsStatType, queryKey *pbc.QueryKey) ([]*ProcfsCellMetric, error) {
	if queryKey == nil {
		return nil, fmt.Errorf("query key cannot be nil")
	}
	lastIds, okLast := last.pidProcIndex[ProcIndexKey{SessId: int64(queryKey.Ssid)}]
	if !okLast {
		return nil, nil
	}

	cellMetrics := make(map[QueryCellKey]*ProcfsCellMetric)
	for _, id := range lastIds {
		if id == nil {
			continue
		}
		lastKey := ProcKey{SessId: int64(queryKey.Ssid), GpSegmentId: id.GpSegmentId, Pid: id.Pid}
		lastProcData, okLast := last.pidProcData[lastKey]
		if !okLast || lastProcData == nil || !procfsMatchesQuery(lastProcData, queryKey.Ccnt) {
			continue
		}
		pidStat := lastProcData.ToGpPidProcInfo(lastKey)
		firstProcData, okFirst := first.pidProcData[lastKey]
		if okFirst {
			diff, err := ProcfsDiff(firstProcData.ToGpPidProcInfo(lastKey), pidStat)
			if err != nil {
				return nil, fmt.Errorf("fail in diff: %w", err)
			}
			pidStat = diff
		}

		cell := procfsCellForProc(int32(id.GpSegmentId), pidStat.SliceId)
		cellMetric, ok := cellMetrics[cell]
		if !ok {
			cellMetric = &ProcfsCellMetric{QueryCellKey: cell, HasData: true}
			cellMetrics[cell] = cellMetric
		}
		cellMetric.RuntimeMetrics = addProcfsRuntimeMetrics(cellMetric.RuntimeMetrics, pidStat)
	}

	cells := make([]QueryCellKey, 0, len(cellMetrics))
	for cell := range cellMetrics {
		cells = append(cells, cell)
	}
	sort.Slice(cells, func(i, j int) bool {
		if cells[i].SliceID != cells[j].SliceID {
			return cells[i].SliceID < cells[j].SliceID
		}
		return cells[i].Segindex < cells[j].Segindex
	})

	result := make([]*ProcfsCellMetric, 0, len(cells))
	for _, cell := range cells {
		result = append(result, cellMetrics[cell])
	}
	return result, nil
}

type cpuTimeSkewAccumulator struct {
	count       int64
	sum         float64
	max         float64
	hotSegindex int32
	initialized bool
}

func (a *cpuTimeSkewAccumulator) Add(segindex int32, cpuTime float64) {
	a.count++
	a.sum += cpuTime
	if !a.initialized || cpuTime > a.max {
		a.max = cpuTime
		a.hotSegindex = segindex
		a.initialized = true
	}
}

func (a *cpuTimeSkewAccumulator) Skew() *pbc.Skew {
	result := &pbc.Skew{}
	if a == nil || a.count == 0 || !a.initialized {
		return result
	}
	result.Segindex = a.hotSegindex
	if a.max <= 0 {
		return result
	}
	avg := a.sum / float64(a.count)
	result.Skew = 1 - avg/a.max
	if result.Skew < 0 {
		result.Skew = 0
	}
	return result
}

func CalculateCPUTimeSkew(cells []*ProcfsCellMetric) *pbc.Skew {
	result := &pbc.Skew{}
	bySlice := make(map[int64]*cpuTimeSkewAccumulator)
	for _, cell := range cells {
		if cell == nil || !cell.HasData || cell.RuntimeMetrics == nil {
			continue
		}
		if cell.Segindex < 0 {
			continue
		}
		acc, ok := bySlice[cell.SliceID]
		if !ok {
			acc = &cpuTimeSkewAccumulator{}
			bySlice[cell.SliceID] = acc
		}
		acc.Add(cell.Segindex, float64(cell.RuntimeMetrics.Utime+cell.RuntimeMetrics.Stime))
	}
	for _, acc := range bySlice {
		current := acc.Skew()
		if current.Skew > result.Skew {
			result = current
		}
	}
	return result
}

type hostSliceKey struct {
	Segindex int32
	Ccnt     int32
	SliceID  int64
}

type hostRunningQueryDetailKey struct {
	Ssid int64
	Ccnt int32
}

type hostRunningQueryDetailAccumulator struct {
	info        *HostRunningQueryDetailInfo
	cellMetrics map[QueryCellKey]*ProcfsCellMetric
}

func newHostRunningQueryDetailAccumulator(key hostRunningQueryDetailKey, state string, isIdle bool, dataQuality *pbc.DataQuality) *hostRunningQueryDetailAccumulator {
	ccnt := key.Ccnt
	if isIdle {
		ccnt = UnsetSliceId
	}
	return &hostRunningQueryDetailAccumulator{
		info: &HostRunningQueryDetailInfo{
			QueryKey:    &pbc.QueryKey{Ssid: int32(key.Ssid), Ccnt: ccnt},
			DataQuality: dataQuality,
			State:       state,
			IsIdle:      isIdle,
		},
		cellMetrics: make(map[QueryCellKey]*ProcfsCellMetric),
	}
}

func (a *hostRunningQueryDetailAccumulator) addProc(segindex int32, lastProc *ProcStat, pidStat *pbc.GpPidProcInfo) {
	if a == nil || a.info == nil || lastProc == nil || pidStat == nil {
		return
	}
	a.info.RuntimeMetrics = addProcfsRuntimeMetrics(a.info.RuntimeMetrics, pidStat)
	if a.info.RuntimeMetrics != nil && a.info.RuntimeMetrics.State != "" {
		a.info.State = a.info.RuntimeMetrics.State
	}
	if a.info.IsIdle {
		return
	}
	sliceID := procfsCellForProc(segindex, lastProc.SliceId).SliceID
	cell := QueryCellKey{SliceID: sliceID, Segindex: segindex}
	cellMetric, ok := a.cellMetrics[cell]
	if !ok {
		cellMetric = &ProcfsCellMetric{QueryCellKey: cell, HasData: true}
		a.cellMetrics[cell] = cellMetric
	}
	cellMetric.RuntimeMetrics = addProcfsRuntimeMetrics(cellMetric.RuntimeMetrics, pidStat)
}

func (a *hostRunningQueryDetailAccumulator) result() *HostRunningQueryDetailInfo {
	if a == nil || a.info == nil {
		return nil
	}
	cells := make([]*ProcfsCellMetric, 0, len(a.cellMetrics))
	for _, cell := range a.cellMetrics {
		cells = append(cells, cell)
	}
	a.info.Skew = CalculateCPUTimeSkew(cells)
	return a.info
}

type hostRunningAccumulator struct {
	info          *HostRunningQueriesInfo
	activeQueries map[int32]struct{}
	activeSlices  map[hostSliceKey]struct{}
	totalSessions map[int64]struct{}
	cellMetrics   map[QueryCellKey]*ProcfsCellMetric
}

func newHostRunningAccumulator(hostname string, segindexes []int32, dataQuality *pbc.DataQuality) *hostRunningAccumulator {
	return &hostRunningAccumulator{
		info: &HostRunningQueriesInfo{
			HostName:    hostname,
			Segindex:    append([]int32(nil), segindexes...),
			DataQuality: dataQuality,
		},
		activeQueries: make(map[int32]struct{}),
		activeSlices:  make(map[hostSliceKey]struct{}),
		totalSessions: make(map[int64]struct{}),
		cellMetrics:   make(map[QueryCellKey]*ProcfsCellMetric),
	}
}

func (a *hostRunningAccumulator) addProc(segindex int32, sessId int64, lastProc *ProcStat, pidStat *pbc.GpPidProcInfo, active bool) {
	if lastProc == nil || pidStat == nil {
		return
	}
	// Record total sessions for all processes (active and idle).
	a.totalSessions[sessId] = struct{}{}
	if lastProc.ProcStatus != nil {
		a.info.MemoryUsage += lastProc.ProcStatus.VmRss
	}
	if lastProc.ProcSpill != nil {
		a.info.SpillBytes += lastProc.ProcSpill.Size
	}
	if pidStat.ProcIo != nil {
		a.info.DiskReads += pidStat.ProcIo.ReadBytes
		a.info.DiskWrites += pidStat.ProcIo.WriteBytes
		a.info.DiskUsage += pidStat.ProcIo.ReadBytes + pidStat.ProcIo.WriteBytes
	}
	if !active {
		return
	}
	a.activeQueries[lastProc.Ccnt] = struct{}{}
	sliceID := procfsCellForProc(segindex, lastProc.SliceId).SliceID
	a.activeSlices[hostSliceKey{Segindex: segindex, Ccnt: lastProc.Ccnt, SliceID: sliceID}] = struct{}{}
	cell := QueryCellKey{SliceID: sliceID, Segindex: segindex}
	cellMetric, ok := a.cellMetrics[cell]
	if !ok {
		cellMetric = &ProcfsCellMetric{QueryCellKey: cell, HasData: true}
		a.cellMetrics[cell] = cellMetric
	}
	cellMetric.RuntimeMetrics = addProcfsRuntimeMetrics(cellMetric.RuntimeMetrics, pidStat)
}

func (a *hostRunningAccumulator) result() *HostRunningQueriesInfo {
	cells := make([]*ProcfsCellMetric, 0, len(a.cellMetrics))
	for _, cell := range a.cellMetrics {
		cells = append(cells, cell)
	}
	a.info.ActiveQueries = int64(len(a.activeQueries))
	a.info.ActiveSlices = int64(len(a.activeSlices))
	a.info.TotalSessions = int64(len(a.totalSessions))
	a.info.Skew = CalculateCPUTimeSkew(cells)
	return a.info
}

func (p *ProcfsStorage) GetHostsRunningQueries() ([]*HostRunningQueriesInfo, error) {
	first, last, err := p.get5Min()
	if err != nil {
		return nil, fmt.Errorf("fail in get 5 min: %w", err)
	}
	hostSegments := configuredHostSegmentsWithFallback(last)
	hostnames := make([]string, 0, len(hostSegments))
	for hostname := range hostSegments {
		hostnames = append(hostnames, hostname)
	}
	sort.Strings(hostnames)
	byHost := make(map[string]*hostRunningAccumulator, len(hostnames))
	for _, hostname := range hostnames {
		byHost[hostname] = newHostRunningAccumulator(hostname, hostSegments[hostname], hostDataQuality(hostname, last))
		if hostStat := last.hostStat[hostname]; hostStat != nil {
			byHost[hostname].info.CPUUsage = hostStat.CPUUsage
			if hostStat.LoadAvg != nil {
				byHost[hostname].info.Avg5 = hostStat.LoadAvg.Avg5
			}
		}
	}

	// Get local hostname for master processes (segindex < 0).
	localHostname, err := os.Hostname()
	if err != nil || localHostname == "" {
		localHostname = "localhost"
	}
	for key, lastProc := range last.pidProcData {
		if lastProc == nil {
			continue
		}
		// Determine hostname: use segment mapping for segments, local hostname for master.
		var hostname string
		if key.GpSegmentId < 0 {
			// Master process - use local hostname.
			hostname = localHostname
		} else {
			hostname = GetHostnameForSegindex(int32(key.GpSegmentId))
		}
		acc, ok := byHost[hostname]
		if !ok {
			segindexes := []int32{}
			if key.GpSegmentId >= 0 {
				segindexes = []int32{int32(key.GpSegmentId)}
			}
			acc = newHostRunningAccumulator(hostname, segindexes, hostDataQuality(hostname, last))
			byHost[hostname] = acc
			hostnames = append(hostnames, hostname)
		}
		pidStat := lastProc.ToGpPidProcInfo(key)
		if firstProc, ok := first.pidProcData[key]; ok {
			diff, err := ProcfsDiff(firstProc.ToGpPidProcInfo(key), pidStat)
			if err != nil {
				return nil, fmt.Errorf("fail in diff: %w", err)
			}
			pidStat = diff
		}
		active := lastProc.Ccnt >= 0 && !isIdleProcState(lastProc.State)
		segindex := int32(key.GpSegmentId)
		acc.addProc(segindex, key.SessId, lastProc, pidStat, active)
	}
	sort.Strings(hostnames)
	result := make([]*HostRunningQueriesInfo, 0, len(hostnames))
	seenHosts := make(map[string]struct{}, len(hostnames))
	for _, hostname := range hostnames {
		if _, ok := seenHosts[hostname]; ok {
			continue
		}
		seenHosts[hostname] = struct{}{}
		result = append(result, byHost[hostname].result())
	}
	return result, nil
}

func (p *ProcfsStorage) GetHostRunningQueries(hostname string) (*HostRunningQueriesDetailInfo, error) {
	if strings.TrimSpace(hostname) == "" {
		return nil, fmt.Errorf("hostname cannot be empty")
	}

	first, last, err := p.get5Min()
	if err != nil {
		return nil, fmt.Errorf("fail in get 5 min: %w", err)
	}
	hostSegments := configuredHostSegmentsWithFallback(last)
	segindexes := append([]int32(nil), hostSegments[hostname]...)
	dataQuality := hostDataQuality(hostname, last)
	accs := make(map[hostRunningQueryDetailKey]*hostRunningQueryDetailAccumulator)

	localHostname, err := os.Hostname()
	if err != nil || localHostname == "" {
		localHostname = "localhost"
	}
	for key, lastProc := range last.pidProcData {
		if lastProc == nil {
			continue
		}
		procHostname := localHostname
		if key.GpSegmentId >= 0 {
			procHostname = GetHostnameForSegindex(int32(key.GpSegmentId))
		}
		if procHostname != hostname {
			continue
		}
		if key.GpSegmentId >= 0 && len(hostSegments[hostname]) == 0 {
			segindexes = append(segindexes, int32(key.GpSegmentId))
		}

		pidStat := lastProc.ToGpPidProcInfo(key)
		if firstProc, ok := first.pidProcData[key]; ok {
			diff, err := ProcfsDiff(firstProc.ToGpPidProcInfo(key), pidStat)
			if err != nil {
				return nil, fmt.Errorf("fail in diff: %w", err)
			}
			pidStat = diff
		}

		isIdle := lastProc.Ccnt < 0 || isIdleProcState(lastProc.State)
		ccnt := lastProc.Ccnt
		if isIdle {
			ccnt = UnsetSliceId
		}
		detailKey := hostRunningQueryDetailKey{Ssid: key.SessId, Ccnt: ccnt}
		acc, ok := accs[detailKey]
		if !ok {
			acc = newHostRunningQueryDetailAccumulator(detailKey, lastProc.State, isIdle, dataQuality)
			accs[detailKey] = acc
		}
		acc.addProc(int32(key.GpSegmentId), lastProc, pidStat)
	}

	if len(hostSegments[hostname]) == 0 {
		sort.Slice(segindexes, func(i, j int) bool { return segindexes[i] < segindexes[j] })
		segindexes = uniqueInt32s(segindexes)
	}

	keys := make([]hostRunningQueryDetailKey, 0, len(accs))
	for key := range accs {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		left := accs[keys[i]].info
		right := accs[keys[j]].info
		if left.IsIdle != right.IsIdle {
			return !left.IsIdle
		}
		if keys[i].Ssid != keys[j].Ssid {
			return keys[i].Ssid < keys[j].Ssid
		}
		return keys[i].Ccnt < keys[j].Ccnt
	})

	queries := make([]*HostRunningQueryDetailInfo, 0, len(keys))
	for _, key := range keys {
		if result := accs[key].result(); result != nil {
			queries = append(queries, result)
		}
	}
	return &HostRunningQueriesDetailInfo{
		HostName:    hostname,
		Segindex:    segindexes,
		Queries:     queries,
		DataQuality: dataQuality,
	}, nil
}

func uniqueInt32s(values []int32) []int32 {
	if len(values) == 0 {
		return values
	}
	result := values[:0]
	var last int32
	for i, value := range values {
		if i > 0 && value == last {
			continue
		}
		result = append(result, value)
		last = value
	}
	return result
}

func (p *ProcfsStorage) GetQueryRunningMetrics(queryKey *pbc.QueryKey) ([]*ProcfsCellMetric, *pbc.Skew, *pbc.DataQuality, error) {
	first, last, err := p.get5Min()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("fail in get 5 min: %w", err)
	}

	result, err := p.getProcfsQueryRuntimeMetrics(first, last, queryKey)
	if err != nil {
		return nil, nil, nil, err
	}
	return result, CalculateCPUTimeSkew(result), procfsDataQuality(last), nil
}
