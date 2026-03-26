package metrics

import (
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// TimeGetter returns start times of queries used to compute executing-query bucket counts.
type TimeGetter func() []time.Time

// TimeGaugeHistogram exposes a set of gauges (one per duration bucket) counting how many
// currently executing queries fall into each bucket by elapsed time since query start.
// It implements prometheus.Collector and recalculates bucket counts on scrape when enough
// time has passed since the last recalculation (see first bucket upper bound).
type TimeGaugeHistogram struct {
	buckets     []float64 // upper bounds in seconds (same semantics as prometheus histogram buckets)
	recalcMin   time.Duration
	getter      TimeGetter
	mu          sync.Mutex
	lastRecalc  time.Time
	valuesCache []float64
	descs       []*prometheus.Desc
	metricName  string
	help        string
}

// NewTimeGaugeHistogram registers a collector on reg. buckets must be non-empty; values are
// upper bounds in seconds (e.g. from the same slice used for HistogramOpts.Buckets).
func NewTimeGaugeHistogram(metricName, help string, reg prometheus.Registerer, buckets []float64) *TimeGaugeHistogram {
	if len(buckets) == 0 {
		panic("gauge_time_histogram: empty buckets")
	}
	recalcMin := time.Duration(buckets[0] * float64(time.Second))
	tgh := &TimeGaugeHistogram{
		buckets:     buckets,
		recalcMin:   recalcMin,
		lastRecalc:  time.Now().Add(-2 * recalcMin),
		valuesCache: make([]float64, len(buckets)+1),
		descs:       make([]*prometheus.Desc, len(buckets)+1),
		metricName:  metricName,
		help:        help,
	}
	for i := 0; i <= len(buckets); i++ {
		le := leLabelForBucket(i, buckets)
		tgh.descs[i] = prometheus.NewDesc(
			metricName,
			help,
			nil,
			prometheus.Labels{"le": le},
		)
	}
	reg.MustRegister(tgh)
	return tgh
}

func leLabelForBucket(idx int, buckets []float64) string {
	if idx < len(buckets) {
		return strconv.FormatFloat(buckets[idx], 'f', -1, 64)
	}
	return "+Inf"
}

func mapElapsedToBucket(elapsed time.Duration, upperBounds []float64) int {
	sec := elapsed.Seconds()
	for i, ub := range upperBounds {
		if sec <= ub {
			return i
		}
	}
	return len(upperBounds)
}

func (tgh *TimeGaugeHistogram) recalc() {
	for i := range tgh.valuesCache {
		tgh.valuesCache[i] = 0
	}
	var startTimes []time.Time
	if tgh.getter != nil {
		startTimes = tgh.getter()
	}
	now := time.Now()
	for _, st := range startTimes {
		idx := mapElapsedToBucket(now.Sub(st), tgh.buckets)
		tgh.valuesCache[idx]++
	}
}

// AssignQueryGetter sets the function that returns start times of in-flight queries.
func (tgh *TimeGaugeHistogram) AssignQueryGetter(getter TimeGetter) {
	tgh.mu.Lock()
	defer tgh.mu.Unlock()
	tgh.getter = getter
}

// Describe implements prometheus.Collector.
func (tgh *TimeGaugeHistogram) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range tgh.descs {
		ch <- d
	}
}

// Collect implements prometheus.Collector.
func (tgh *TimeGaugeHistogram) Collect(ch chan<- prometheus.Metric) {
	tgh.mu.Lock()
	if time.Since(tgh.lastRecalc) > tgh.recalcMin {
		tgh.lastRecalc = time.Now()
		tgh.recalc()
	}
	values := append([]float64(nil), tgh.valuesCache...)
	tgh.mu.Unlock()

	for i, v := range values {
		ch <- prometheus.MustNewConstMetric(tgh.descs[i], prometheus.GaugeValue, v)
	}
}
