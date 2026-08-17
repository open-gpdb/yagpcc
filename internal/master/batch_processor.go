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

package master

import (
	"context"
	"time"

	pbm "github.com/open-gpdb/yagpcc/api/proto/agent_master"
	"github.com/open-gpdb/yagpcc/internal/config"
	"github.com/open-gpdb/yagpcc/internal/gp"
	"github.com/open-gpdb/yagpcc/internal/metrics"
	"go.uber.org/zap"
)

// BatchProcessorConfig holds configuration for the batch processor.
type BatchProcessorConfig = config.BatchProcessorConfig

// DefaultBatchProcessorConfig returns the default configuration for batch processors.
func DefaultBatchProcessorConfig() BatchProcessorConfig {
	return config.DefaultBatchProcessorConfig()
}

// SessionStoreFunc is the function signature for storing a batch of sessions.
type SessionStoreFunc func(ctx context.Context, sessions []*gp.SessionDataWrite) error

// QueryStoreFunc is the function signature for storing a batch of queries.
type QueryStoreFunc func(ctx context.Context, queries []*pbm.QueryStatWrite) error

// SegmentStoreFunc is the function signature for storing a batch of segment metrics.
type SegmentStoreFunc func(ctx context.Context, metrics []*pbm.SegmentMetricsWrite) error

// RunSessionBatchProcessor starts the batch processor for sessions. target names
// the writer destination and is used as a Prometheus label so multiple targets
// can be distinguished.
func RunSessionBatchProcessor(
	ctx context.Context,
	logger *zap.SugaredLogger,
	config BatchProcessorConfig,
	target string,
	sourceCh <-chan *gp.SessionDataWrite,
	store SessionStoreFunc,
) {
	pipeCh := make(chan []*gp.SessionDataWrite, config.BatchQueueSize)
	go collectSessionBatches(ctx, logger, config, target, sourceCh, pipeCh)
	go processSessionBatches(ctx, logger, config, target, pipeCh, store)
	<-ctx.Done()
	logger.Info("done session batch processor")
}

// RunQueryBatchProcessor starts the batch processor for queries.
func RunQueryBatchProcessor(
	ctx context.Context,
	logger *zap.SugaredLogger,
	config BatchProcessorConfig,
	target string,
	sourceCh <-chan *pbm.QueryStatWrite,
	store QueryStoreFunc,
) {
	pipeCh := make(chan []*pbm.QueryStatWrite, config.BatchQueueSize)
	go collectQueryBatches(ctx, logger, config, target, sourceCh, pipeCh)
	go processQueryBatches(ctx, logger, config, target, pipeCh, store)
	<-ctx.Done()
	logger.Info("done query batch processor")
}

// RunSegmentBatchProcessor starts the batch processor for segment metrics.
func RunSegmentBatchProcessor(
	ctx context.Context,
	logger *zap.SugaredLogger,
	config BatchProcessorConfig,
	target string,
	sourceCh <-chan *pbm.SegmentMetricsWrite,
	store SegmentStoreFunc,
) {
	pipeCh := make(chan []*pbm.SegmentMetricsWrite, config.BatchQueueSize)
	go collectSegmentBatches(ctx, logger, config, target, sourceCh, pipeCh)
	go processSegmentBatches(ctx, logger, config, target, pipeCh, store)
	<-ctx.Done()
	logger.Info("done segment batch processor")
}

// collectSessionBatches collects sessions from source channel into batches.
func collectSessionBatches(
	ctx context.Context,
	logger *zap.SugaredLogger,
	config BatchProcessorConfig,
	target string,
	sourceCh <-chan *gp.SessionDataWrite,
	pipeCh chan<- []*gp.SessionDataWrite,
) {
	ticker := time.NewTicker(config.BatchInterval)
	defer ticker.Stop()
	stream := "sessions"

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		batch := collectSessionBatch(ctx, ticker, sourceCh)
		if len(batch) > 0 {
			select {
			case pipeCh <- batch:
				if metrics.YagpccMetrics != nil {
					metrics.YagpccMetrics.WriterBatchSize.WithLabelValues(stream, target).Observe(float64(len(batch)))
				}
			case <-ctx.Done():
				if metrics.YagpccMetrics != nil {
					metrics.YagpccMetrics.WriterDroppedMessages.WithLabelValues(stream, target).Add(float64(len(batch)))
				}
				return
			default:
				if metrics.YagpccMetrics != nil {
					metrics.YagpccMetrics.WriterDroppedMessages.WithLabelValues(stream, target).Add(float64(len(batch)))
				}
				logger.Warnf("pipe full for stream %s target %s, dropping batch of %d messages", stream, target, len(batch))
			}
		}

		// BatchInterval is enforced inside collectSessionBatch (it consumes ticker.C).
	}
}

// collectSessionBatch collects sessions until the ticker fires.
func collectSessionBatch(
	ctx context.Context,
	ticker *time.Ticker,
	sourceCh <-chan *gp.SessionDataWrite,
) []*gp.SessionDataWrite {
	batch := make([]*gp.SessionDataWrite, 0)

	for {
		select {
		case <-ctx.Done():
			return batch
		case <-ticker.C:
			return batch
		case msg, ok := <-sourceCh:
			if !ok {
				return batch
			}
			batch = append(batch, msg)
		}
	}
}

// processSessionBatches processes batches from the pipe.
func processSessionBatches(
	ctx context.Context,
	logger *zap.SugaredLogger,
	config BatchProcessorConfig,
	target string,
	pipeCh <-chan []*gp.SessionDataWrite,
	store SessionStoreFunc,
) {
	stream := "sessions"

	for {
		select {
		case <-ctx.Done():
			return
		case batch := <-pipeCh:
			processBatch(ctx, logger, config, stream, target, batch, func(ctx context.Context, b []*gp.SessionDataWrite) error {
				return store(ctx, b)
			})
		}
	}
}

// collectQueryBatches collects queries from source channel into batches.
func collectQueryBatches(
	ctx context.Context,
	logger *zap.SugaredLogger,
	config BatchProcessorConfig,
	target string,
	sourceCh <-chan *pbm.QueryStatWrite,
	pipeCh chan<- []*pbm.QueryStatWrite,
) {
	ticker := time.NewTicker(config.BatchInterval)
	defer ticker.Stop()
	stream := "queries"

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		batch := collectQueryBatch(ctx, ticker, sourceCh)
		if len(batch) > 0 {
			select {
			case pipeCh <- batch:
				if metrics.YagpccMetrics != nil {
					metrics.YagpccMetrics.WriterBatchSize.WithLabelValues(stream, target).Observe(float64(len(batch)))
				}
			case <-ctx.Done():
				if metrics.YagpccMetrics != nil {
					metrics.YagpccMetrics.WriterDroppedMessages.WithLabelValues(stream, target).Add(float64(len(batch)))
				}
				return
			default:
				if metrics.YagpccMetrics != nil {
					metrics.YagpccMetrics.WriterDroppedMessages.WithLabelValues(stream, target).Add(float64(len(batch)))
				}
				logger.Warnf("pipe full for stream %s target %s, dropping batch of %d messages", stream, target, len(batch))
			}
		}

		// BatchInterval is enforced inside collectQueryBatch (it consumes ticker.C).
	}
}

// collectQueryBatch collects queries until the ticker fires.
func collectQueryBatch(
	ctx context.Context,
	ticker *time.Ticker,
	sourceCh <-chan *pbm.QueryStatWrite,
) []*pbm.QueryStatWrite {
	batch := make([]*pbm.QueryStatWrite, 0)

	for {
		select {
		case <-ctx.Done():
			return batch
		case <-ticker.C:
			return batch
		case msg, ok := <-sourceCh:
			if !ok {
				return batch
			}
			batch = append(batch, msg)
		}
	}
}

// processQueryBatches processes batches from the pipe.
func processQueryBatches(
	ctx context.Context,
	logger *zap.SugaredLogger,
	config BatchProcessorConfig,
	target string,
	pipeCh <-chan []*pbm.QueryStatWrite,
	store QueryStoreFunc,
) {
	stream := "queries"

	for {
		select {
		case <-ctx.Done():
			return
		case batch := <-pipeCh:
			processBatch(ctx, logger, config, stream, target, batch, func(ctx context.Context, b []*pbm.QueryStatWrite) error {
				return store(ctx, b)
			})
		}
	}
}

// collectSegmentBatches collects segment metrics from source channel into batches.
func collectSegmentBatches(
	ctx context.Context,
	logger *zap.SugaredLogger,
	config BatchProcessorConfig,
	target string,
	sourceCh <-chan *pbm.SegmentMetricsWrite,
	pipeCh chan<- []*pbm.SegmentMetricsWrite,
) {
	ticker := time.NewTicker(config.BatchInterval)
	defer ticker.Stop()
	stream := "segments"

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		batch := collectSegmentBatch(ctx, ticker, sourceCh)
		if len(batch) > 0 {
			select {
			case pipeCh <- batch:
				if metrics.YagpccMetrics != nil {
					metrics.YagpccMetrics.WriterBatchSize.WithLabelValues(stream, target).Observe(float64(len(batch)))
				}
			case <-ctx.Done():
				if metrics.YagpccMetrics != nil {
					metrics.YagpccMetrics.WriterDroppedMessages.WithLabelValues(stream, target).Add(float64(len(batch)))
				}
				return
			default:
				if metrics.YagpccMetrics != nil {
					metrics.YagpccMetrics.WriterDroppedMessages.WithLabelValues(stream, target).Add(float64(len(batch)))
				}
				logger.Warnf("pipe full for stream %s target %s, dropping batch of %d messages", stream, target, len(batch))
			}
		}

		// BatchInterval is enforced inside collectSegmentBatch (it consumes ticker.C).
	}
}

// collectSegmentBatch collects segment metrics until the ticker fires.
func collectSegmentBatch(
	ctx context.Context,
	ticker *time.Ticker,
	sourceCh <-chan *pbm.SegmentMetricsWrite,
) []*pbm.SegmentMetricsWrite {
	batch := make([]*pbm.SegmentMetricsWrite, 0)

	for {
		select {
		case <-ctx.Done():
			return batch
		case <-ticker.C:
			return batch
		case msg, ok := <-sourceCh:
			if !ok {
				return batch
			}
			batch = append(batch, msg)
		}
	}
}

// processSegmentBatches processes batches from the pipe.
func processSegmentBatches(
	ctx context.Context,
	logger *zap.SugaredLogger,
	config BatchProcessorConfig,
	target string,
	pipeCh <-chan []*pbm.SegmentMetricsWrite,
	store SegmentStoreFunc,
) {
	stream := "segments"

	for {
		select {
		case <-ctx.Done():
			return
		case batch := <-pipeCh:
			processBatch(ctx, logger, config, stream, target, batch, func(ctx context.Context, b []*pbm.SegmentMetricsWrite) error {
				return store(ctx, b)
			})
		}
	}
}

// processBatch is a helper that processes a single batch with timing and metrics.
func processBatch[T any](
	ctx context.Context,
	logger *zap.SugaredLogger,
	config BatchProcessorConfig,
	stream string,
	target string,
	batch []T,
	store func(ctx context.Context, batch []T) error,
) {
	startTime := time.Now()

	writeCtx, cancel := context.WithTimeout(ctx, config.WriteTimeout)
	defer cancel()

	err := store(writeCtx, batch)

	duration := time.Since(startTime).Seconds()

	if err != nil {
		if metrics.YagpccMetrics != nil {
			metrics.YagpccMetrics.WriterDroppedMessages.WithLabelValues(stream, target).Add(float64(len(batch)))
		}
		logger.Warnf("write failed for stream %s target %s, dropping batch of %d messages: %v", stream, target, len(batch), err)
	} else if metrics.YagpccMetrics != nil {
		metrics.YagpccMetrics.WriterProcessedMessages.WithLabelValues(stream, target).Add(float64(len(batch)))
	}

	if metrics.YagpccMetrics != nil {
		metrics.YagpccMetrics.WriterDuration.WithLabelValues(stream, target).Observe(duration)
	}
}
