package uds

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	pb "github.com/open-gpdb/yagpcc/api/proto/agent_segment"
	"go.uber.org/zap"
)

func (p *Processor) Process(ctx context.Context, conn connection) error {
	defer func() {
		_ = conn.Close()
	}()

	if err := conn.SetReadDeadline(p.clock().Add(time.Second * 60)); err != nil {
		return fmt.Errorf("error setting connection read deadline: %w", err)
	}

	var buffer []byte
	var msgSize int

	firstRead := true
	totalBytes := 0
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("error handling connection: %w", ctx.Err())
		default:
			tmpBuffer := make([]byte, p.bufferSize)
			bytesRead, err := conn.Read(tmpBuffer)

			if err == io.EOF {
				// cannot continue if got an EOF
				if bytesRead > 0 || totalBytes > 0 {
					return fmt.Errorf("unexpected EOF: read %d bytes and got EOF", bytesRead+totalBytes)
				}

				return nil
			}

			if err != nil {
				return fmt.Errorf("error reading from connection: %w", err)
			}

			if firstRead {
				if bytesRead <= 4 {
					return fmt.Errorf("got empty message of len %d", bytesRead)
				}
				buffer = tmpBuffer
				msgSize = int(binary.LittleEndian.Uint32(buffer))
			} else {
				buffer = append(buffer, tmpBuffer[:bytesRead]...)
			}

			totalBytes += bytesRead
			if msgSize+4 > totalBytes {
				// continue reading
				firstRead = false
				continue
			}

			if msgSize+4 < totalBytes {
				return fmt.Errorf("got incomplete message expected size %d got %d", msgSize+8, totalBytes)
			}

			msgB := buffer[4 : 4+msgSize]
			req := &pb.SetQueryReq{}

			if err = proto.Unmarshal(msgB, req); err != nil {
				p.log.Warn(
					"error unmarshaling message",
					zap.Error(err),
					zap.Binary("buffer", buffer),
					zap.Int("message_size", msgSize),
					zap.Binary("message_binary", msgB),
				)
				return fmt.Errorf("error unmarshalling message: %w", err)
			}

			if _, err = p.setQIServer.SetMetricQuery(ctx, req); err != nil {
				p.log.Warn("error processing message", zap.Error(err), p.messageLog(req))
				return fmt.Errorf("error processing message: %w", err)
			}

			return nil
		}
	}
}

func NewProcessor(log *zap.SugaredLogger, setQIServer setQIServer, opts ...Option) *Processor {
	const defaultBufferSize = 4 * 1024

	p := &Processor{
		log:         log,
		bufferSize:  defaultBufferSize,
		setQIServer: setQIServer,
		clock:       time.Now,
	}

	for _, o := range opts {
		o(p)
	}

	return p
}

type Processor struct {
	log         *zap.SugaredLogger
	setQIServer setQIServer
	bufferSize  uint32
	clock       func() time.Time
}

type Option func(*Processor)

func WithBufferSize(size uint32) Option {
	return func(p *Processor) {
		p.bufferSize = size
	}
}

func WithClock(clock func() time.Time) Option {
	return func(p *Processor) {
		p.clock = clock
	}
}

func (p *Processor) messageLog(message proto.Message) zap.Field {
	asJSONString := "<error representing message as json string>"

	if asJSONBytes, err := protojson.Marshal(message); err == nil {
		asJSONString = string(asJSONBytes)
	}

	return zap.String("message", asJSONString)
}
