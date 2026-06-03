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

	HandleLatencies         *prometheus.HistogramVec
	QueryLatencies          *prometheus.HistogramVec
	SliceLatencies          *prometheus.HistogramVec
	FailedQueries           prometheus.Counter
	QueryStatuses           *prometheus.CounterVec
	QueriesInFlight         prometheus.Gauge
	ExecutingQueryLatencies *TimeGaugeHistogram

	// GC metrics
	GCRuns            prometheus.Counter
	GCDeletedQueries  prometheus.Counter
	GCArchivedQueries prometheus.Counter
}

var YagpccMetrics *YagpccMetricsType
