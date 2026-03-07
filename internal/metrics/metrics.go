package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

type YagpccMetricsType struct {
	NewSessions          prometheus.Counter
	NewQueries           prometheus.Counter
	DeletedSessions      prometheus.Counter
	DeletedQueries       prometheus.Counter
	NewAggregatedQueries prometheus.Counter
	DroppedQueries       prometheus.Counter

	TotalSessions     prometheus.Gauge
	TotalQueries      prometheus.Gauge
	AggregatedQueries prometheus.Gauge

	HandleLatencies *prometheus.HistogramVec
	QueryLatencies  *prometheus.HistogramVec
	SliceLatencies  *prometheus.HistogramVec
	FailedQueries   prometheus.Counter
	QueryStatuses   *prometheus.CounterVec
	QueriesInFlight prometheus.Gauge
}

var YagpccMetrics *YagpccMetricsType
