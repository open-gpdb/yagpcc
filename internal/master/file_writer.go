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
	"io"
	"os"
	"sync"

	"google.golang.org/protobuf/encoding/protojson"

	pbm "github.com/open-gpdb/yagpcc/api/proto/agent_master"
	"github.com/open-gpdb/yagpcc/internal/gp"
	"github.com/open-gpdb/yagpcc/internal/interfaces"
	"go.uber.org/zap"
)

// FileWriter implements ArchiveWriter interface using rotating JSONL files.
type FileWriters struct {
	logger        *zap.SugaredLogger
	sessionWriter io.Writer
	queryWriter   io.Writer
	segmentWriter io.Writer
}

// NewFileWriters creates a new FileWriters instance with rotating file writers.
func NewFileWriters(logger *zap.SugaredLogger, sessionsFile, queriesFile, segmentsFile string, maxFileSize int64) (*FileWriters, error) {
	sessionWriter, err := NewRotateWriter(sessionsFile, maxFileSize)
	if err != nil {
		return nil, err
	}
	queryWriter, err := NewRotateWriter(queriesFile, maxFileSize)
	if err != nil {
		return nil, err
	}
	segmentWriter, err := NewRotateWriter(segmentsFile, maxFileSize)
	if err != nil {
		return nil, err
	}

	return &FileWriters{
		logger:        logger,
		sessionWriter: sessionWriter,
		queryWriter:   queryWriter,
		segmentWriter: segmentWriter,
	}, nil
}

// StoreSessions writes a batch of session data to the archive file.
func (fw *FileWriters) StoreSessions(ctx context.Context, sessions []*gp.SessionDataWrite) error {
	if len(sessions) == 0 {
		return nil
	}

	writeJS := make([]byte, 0)
	for _, val := range sessions {
		val.GpStatInfo.TmID = int(gp.DiscoveredTmID)
		val.RunningQuery.Tmid = int32(gp.DiscoveredTmID)

		myJS, err := val.ToJSON()
		if err != nil {
			fw.logger.Errorf("fail to convert sessions data %v with error %v", val, err)
			continue
		}
		writeJS = append(writeJS, myJS...)
		writeJS = append(writeJS, '\n')
	}

	if len(writeJS) > 0 {
		_, err := fw.sessionWriter.Write(writeJS)
		return err
	}
	return nil
}

// StoreQuery writes a batch of query statistics to the archive file.
func (fw *FileWriters) StoreQuery(ctx context.Context, queries []*pbm.QueryStatWrite) error {
	if len(queries) == 0 {
		return nil
	}

	writeJS := make([]byte, 0)
	for _, val := range queries {
		val.QueryKey.Tmid = int32(gp.DiscoveredTmID)

		serializable := &QueryStatWriteSerializable{v: val}
		myJS, err := serializable.ToJSON()
		if err != nil {
			fw.logger.Errorf("fail to convert query data %v with error %v", val, err)
			continue
		}
		writeJS = append(writeJS, myJS...)
		writeJS = append(writeJS, '\n')
	}

	if len(writeJS) > 0 {
		_, err := fw.queryWriter.Write(writeJS)
		return err
	}
	return nil
}

// StoreSegmensMetrics writes a batch of segment metrics to the archive file.
// Note: the misspelled name is preserved for backward compatibility.
func (fw *FileWriters) StoreSegmensMetrics(ctx context.Context, metrics []*pbm.SegmentMetricsWrite) error {
	if len(metrics) == 0 {
		return nil
	}

	writeJS := make([]byte, 0)
	for _, val := range metrics {
		val.QueryKey.Tmid = int32(gp.DiscoveredTmID)

		serializable := &SegmentMetricsWriteSerializable{v: val}
		myJS, err := serializable.ToJSON()
		if err != nil {
			fw.logger.Errorf("fail to convert segment metrics data %v with error %v", val, err)
			continue
		}
		writeJS = append(writeJS, myJS...)
		writeJS = append(writeJS, '\n')
	}

	if len(writeJS) > 0 {
		_, err := fw.segmentWriter.Write(writeJS)
		return err
	}
	return nil
}

// SerializableChan is a channel for serializable data items.
type SerializableChan chan interfaces.DataSerialization

// RotateWriter handles rotating log files when they exceed a maximum size.
type RotateWriter struct {
	lock     sync.Mutex
	filename string
	maxSize  int64
	fp       *os.File
	counter  int
}

// QueryStatWriteSerializable implements DataSerialization for QueryStatWrite.
type QueryStatWriteSerializable struct {
	v *pbm.QueryStatWrite
}

// SegmentMetricsWriteSerializable implements DataSerialization for SegmentMetricsWrite.
type SegmentMetricsWriteSerializable struct {
	v *pbm.SegmentMetricsWrite
}

// ToJSON marshals the QueryStatWrite to JSON bytes.
func (p *QueryStatWriteSerializable) ToJSON() ([]byte, error) {
	marshaler := protojson.MarshalOptions{
		EmitUnpopulated: true,
	}
	return marshaler.Marshal(p.v)
}

// ToJSON marshals the SegmentMetricsWrite to JSON bytes.
func (p *SegmentMetricsWriteSerializable) ToJSON() ([]byte, error) {
	marshaler := protojson.MarshalOptions{
		EmitUnpopulated: true,
	}
	return marshaler.Marshal(p.v)
}

// NewRotateWriter creates a new RotateWriter. Returns nil if error occurs during setup.
func NewRotateWriter(filename string, maxSize int64) (*RotateWriter, error) {
	var err error
	w := &RotateWriter{filename: filename, maxSize: maxSize, counter: 0}

	w.fp, err = os.OpenFile(w.filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}
	return w, nil
}

// Write implements the io.Writer interface.
func (w *RotateWriter) Write(output []byte) (int, error) {
	w.lock.Lock()
	defer w.lock.Unlock()
	if w.counter%10 == 0 || w.fp == nil {
		err := w.Rotate()
		if err != nil {
			return 0, err
		}
	}
	w.counter += 1
	cnt, err := w.fp.Write(output)
	if err == nil {
		// fsync data each time
		err = w.fp.Sync()
	}
	return cnt, err
}

// Rotate performs the actual act of rotating and reopening file.
func (w *RotateWriter) Rotate() (err error) {
	// Rename dest file if exceeded file size
	fi, err := os.Stat(w.filename)
	if err == nil {
		if fi.Size() > w.maxSize {
			err = os.Rename(w.filename, w.filename+".1")
			if err != nil {
				return err
			}
			// Close existing file if open
			if w.fp != nil {
				err = w.fp.Close()
				w.fp = nil
				if err != nil {
					return err
				}
			}
			// Create a file.
			w.fp, err = os.OpenFile(w.filename, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
			if err != nil {
				return err
			}
		}
	}

	return nil
}
