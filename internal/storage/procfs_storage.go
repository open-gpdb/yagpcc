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
	"sync"
	"time"

	pbc "github.com/open-gpdb/yagpcc/api/proto/common"
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

	ProcStat struct {
		Cmdline    string
		State      string
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
		ProcStat:    ps.ProcStat,
		ProcStatus:  ps.ProcStatus,
		ProcIo:      ps.ProcIO,
		ProcSpill:   ps.ProcSpill,
	}
}

type (
	ProcMap      map[ProcKey]*ProcStat
	ProcIndexMap map[ProcIndexKey][]*ProcIndexData

	ProcfsStatType struct {
		statTime     time.Time
		pidProcData  ProcMap
		pidProcIndex ProcIndexMap
	}

	ProcfsStorage struct {
		mx                  *sync.RWMutex
		procfsStat          []ProcfsStatType
		maximumStoredPoints int
	}
)

type ProcfsOption = func(*ProcfsStorage)

const (
	defaultStoredPoints = 30
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
	stat := ProcfsStatType{
		statTime:     statTime,
		pidProcData:  make(ProcMap, len(procfsStat)),
		pidProcIndex: make(ProcIndexMap, len(procfsStat)),
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
