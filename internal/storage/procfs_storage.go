package storage

import (
	"errors"
	"fmt"
	"maps"
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

	ProcStat struct {
		Cmdline    string
		State      string
		ProcStat   *pbc.ProcStat
		ProcStatus *pbc.ProcStatus
		ProcIO     *pbc.ProcIO
	}

	ProcMap map[ProcKey]*ProcStat

	ProcfsStatType struct {
		statTime    time.Time
		pidProcData ProcMap
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
		statTime:    statTime,
		pidProcData: make(ProcMap, len(procfsStat)),
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
		}
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

func (p *ProcfsStorage) GetNearestNTime(d time.Duration) (ProcMap, error) {
	p.mx.RLock()
	defer p.mx.RUnlock()

	return p.getNearestNTimeUnlocked(d)
}

// getNearestNTimeUnlocked searches for the nearest snapshot without acquiring a lock.
// Callers must hold at least p.mx.RLock before calling this method.
// Returns the ProcMap snapshot closest to duration d in the past, or an error if no data exists.
func (p *ProcfsStorage) getNearestNTimeUnlocked(d time.Duration) (ProcMap, error) {
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

	return maps.Clone(p.procfsStat[minIndex].pidProcData), nil
}

func (p *ProcfsStorage) getNMin(d time.Duration) (ProcMap, ProcMap, error) {
	p.mx.RLock()
	defer p.mx.RUnlock()

	nearest, err := p.getNearestNTimeUnlocked(d)
	if err != nil {
		return nil, nil, fmt.Errorf("fail in get %s interval: %w", d, err)
	}
	return nearest, maps.Clone(p.procfsStat[len(p.procfsStat)-1].pidProcData), nil
}

func (p *ProcfsStorage) Get5Min() (ProcMap, ProcMap, error) {
	return p.getNMin(5 * time.Minute)
}

func (p *ProcfsStorage) Get15Min() (ProcMap, ProcMap, error) {
	return p.getNMin(15 * time.Minute)
}

func (p *ProcfsStorage) Get30Min() (ProcMap, ProcMap, error) {
	return p.getNMin(30 * time.Minute)
}
