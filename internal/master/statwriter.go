package master

import (
	"context"
	"io"
	"os"
	"sync"
	"time"

	"google.golang.org/protobuf/encoding/protojson"

	pbm "github.com/open-gpdb/yagpcc/api/proto/agent_master"
	"github.com/open-gpdb/yagpcc/internal/gp"
	"github.com/open-gpdb/yagpcc/internal/interfaces"
	"go.uber.org/zap"
)

type (
	SerializableChan chan interfaces.DataSerialization

	SessionChanRead     <-chan *gp.SessionDataWrite
	QueryStatChanRead   <-chan *pbm.QueryStatWrite
	SegmentStatChanRead <-chan *pbm.SegmentMetricsWrite

	SessionChanWrite     chan<- *gp.SessionDataWrite
	QueryStatChanWrite   chan<- *pbm.QueryStatWrite
	SegmentStatChanWrite chan<- *pbm.SegmentMetricsWrite
)

const (
	BatchSize    = 1000
	batchTimeout = time.Second * 1 / 2
)

type RotateWriter struct {
	lock     sync.Mutex
	filename string
	maxSize  int64
	fp       *os.File
	counter  int
}

type QueryStatWriteSerializable struct {
	v *pbm.QueryStatWrite
}

type SegmentMetricsWriteSerializable struct {
	v *pbm.SegmentMetricsWrite
}

func (p *QueryStatWriteSerializable) ToJSON() ([]byte, error) {
	marshaler := protojson.MarshalOptions{
		EmitUnpopulated: true,
	}
	return marshaler.Marshal(p.v)
}

func (p *SegmentMetricsWriteSerializable) ToJSON() ([]byte, error) {
	marshaler := protojson.MarshalOptions{
		EmitUnpopulated: true,
	}
	return marshaler.Marshal(p.v)
}

// Make a new RotateWriter. Return nil if error occurs during setup.
func NewRotateWriter(filename string, maxSize int64) (*RotateWriter, error) {
	var err error
	w := &RotateWriter{filename: filename, maxSize: maxSize, counter: 0}

	w.fp, err = os.OpenFile(w.filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}
	return w, nil
}

// Write satisfies the io.Writer interface.
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

// Perform the actual act of rotating and reopening file.
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

func StoreSerializableData(ctx context.Context, l *zap.SugaredLogger, sChan SerializableChan, writer io.Writer) {
	cachedItems := 0
	tick := time.NewTicker(batchTimeout)
	writeJS := make([]byte, 0)
	for {
		select {
		case val := <-sChan:
			myJS, err := val.ToJSON()
			if err != nil {
				l.Errorf("fail to convert sessions data %v with error %v", val, err)
				break
			}
			writeJS = append(writeJS, myJS...)
			writeJS = append(writeJS, '\n')
			cachedItems++
		case <-tick.C:
			cachedItems = BatchSize
		case <-ctx.Done():
			// here, we lost all the data, but that's expected behavior. We don't wait and exit immediately.
			return
		}
		if cachedItems >= BatchSize {
			tick.Stop()
			cnt, err := writer.Write(writeJS)
			if err != nil {
				l.Errorf("fail to write sessions data, error %v, %v bytes written", err, cnt)
			}
			cachedItems = 0
			writeJS = make([]byte, 0)
			tick.Reset(batchTimeout)
		}
	}
}

func StoreSessions(ctx context.Context, l *zap.SugaredLogger, sessionChan SessionChanRead, writer io.Writer) {
	SerializableChan := make(SerializableChan, cap(sessionChan))
	defer close(SerializableChan)
	go StoreSerializableData(ctx, l, SerializableChan, writer)
	for {
		select {
		case val := <-sessionChan:
			val.GpStatInfo.TmID = int(gp.DiscoveredTmID)
			val.RunningQuery.Tmid = int32(gp.DiscoveredTmID)
			SerializableChan <- val
		case <-ctx.Done():
			l.Warn("done StoreSessions")
			return
		}
	}
}

func StoreQuery(ctx context.Context, l *zap.SugaredLogger, queryChan QueryStatChanRead, writer io.Writer) {
	SerializableChan := make(SerializableChan, cap(queryChan))
	defer close(SerializableChan)
	go StoreSerializableData(ctx, l, SerializableChan, writer)
	for {
		select {
		case val := <-queryChan:
			val.QueryKey.Tmid = int32(gp.DiscoveredTmID)
			SerializableChan <- &QueryStatWriteSerializable{v: val}
		case <-ctx.Done():
			l.Warn("done StoreQuery")
			return
		}
	}
}

func StoreSegmensMetrics(ctx context.Context, l *zap.SugaredLogger, segChan SegmentStatChanRead, writer io.Writer) {
	SerializableChan := make(SerializableChan, cap(segChan))
	defer close(SerializableChan)
	go StoreSerializableData(ctx, l, SerializableChan, writer)
	for {
		select {
		case val := <-segChan:
			val.QueryKey.Tmid = int32(gp.DiscoveredTmID)
			SerializableChan <- &SegmentMetricsWriteSerializable{v: val}
		case <-ctx.Done():
			l.Warn("done StoreSegmensMetrics")
			return
		}
	}
}
