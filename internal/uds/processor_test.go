package uds_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	pb "github.com/open-gpdb/yagpcc/api/proto/agent_segment"
	greenplum "github.com/open-gpdb/yagpcc/api/proto/common"
	"github.com/open-gpdb/yagpcc/internal/uds"

	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func setupLogsCapture() (*zap.SugaredLogger, *observer.ObservedLogs) {
	core, logs := observer.New(zap.DebugLevel)
	return zap.New(core).Sugar(), logs
}

func TestClientProcessor_Process_Positive(t *testing.T) {

	logger, _ := setupLogsCapture()
	t.Run("empty buffer", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		now := time.Now()
		expectedConnReadDeadline := now.Add(1 * time.Minute)

		connMock := NewMockConnection(ctrl)
		connMock.EXPECT().SetReadDeadline(expectedConnReadDeadline).Return(nil)
		connMock.
			EXPECT().
			Read(gomock.AssignableToTypeOf([]byte{})).
			DoAndReturn(connRead([]byte{})).
			AnyTimes()
		connMock.EXPECT().Close().Return(nil)

		sut := uds.NewProcessor(logger, NewMockSetQIServer(ctrl), uds.WithClock(func() time.Time { return now }))

		err := sut.Process(context.Background(), connMock)

		assert.NoError(t, err)
	})

	t.Run("normal message", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		ctx := context.Background()
		message := &pb.SetQueryReq{QueryKey: &greenplum.QueryKey{Ccnt: 1}}

		connMock := NewMockConnection(ctrl)
		connMock.EXPECT().SetReadDeadline(gomock.AssignableToTypeOf(time.Now())).Return(nil)
		connMock.
			EXPECT().
			Read(gomock.AssignableToTypeOf([]byte{})).
			DoAndReturn(connRead(messageBytesWithHeader(t, message))).
			AnyTimes()
		connMock.EXPECT().Close().Return(nil)

		setQIServerMock := NewMockSetQIServer(ctrl)
		setQIServerMock.
			EXPECT().
			SetMetricQuery(ctx, newProtoMessageMatcher(message)).
			Return(nil, nil)

		sut := uds.NewProcessor(logger, setQIServerMock)

		err := sut.Process(context.Background(), connMock)
		assert.NoError(t, err)
	})

	t.Run("large message with small buffer", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		ctx := context.Background()
		queryTextSize := 16 * 1024
		bufferSize := uint32(1024)
		message := &pb.SetQueryReq{
			QueryKey:  &greenplum.QueryKey{Ccnt: 1},
			QueryInfo: &greenplum.QueryInfo{QueryText: strings.Repeat(string(byte(0)), queryTextSize)},
		}

		connMock := NewMockConnection(ctrl)
		connMock.EXPECT().SetReadDeadline(gomock.AssignableToTypeOf(time.Now())).Return(nil)
		connMock.
			EXPECT().
			Read(gomock.AssignableToTypeOf([]byte{})).
			DoAndReturn(connRead(messageBytesWithHeader(t, message))).
			AnyTimes()
		connMock.EXPECT().Close().Return(nil)

		setQIServerMock := NewMockSetQIServer(ctrl)
		setQIServerMock.
			EXPECT().
			SetMetricQuery(ctx, newProtoMessageMatcher(message)).
			Return(nil, nil)

		sut := uds.NewProcessor(logger, setQIServerMock, uds.WithBufferSize(bufferSize))

		err := sut.Process(ctx, connMock)

		assert.NoError(t, err)
	})

	t.Run("large message with large buffer", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		ctx := context.Background()
		queryTextSize := 16 * 1024
		bufferSize := uint32(17 * 1024)
		message := &pb.SetQueryReq{
			QueryKey: &greenplum.QueryKey{Ccnt: 1},
			QueryInfo: &greenplum.QueryInfo{
				QueryText: strings.Repeat(string(byte(0)), queryTextSize),
			},
		}

		connMock := NewMockConnection(ctrl)
		connMock.EXPECT().SetReadDeadline(gomock.AssignableToTypeOf(time.Now())).Return(nil)
		connMock.
			EXPECT().
			Read(gomock.AssignableToTypeOf([]byte{})).
			DoAndReturn(connRead(messageBytesWithHeader(t, message)))
		connMock.EXPECT().Close().Return(nil)

		setQIServerMock := NewMockSetQIServer(ctrl)
		setQIServerMock.
			EXPECT().
			SetMetricQuery(ctx, newProtoMessageMatcher(message)).
			Return(nil, nil)

		sut := uds.NewProcessor(logger, setQIServerMock, uds.WithBufferSize(bufferSize))

		err := sut.Process(ctx, connMock)

		assert.NoError(t, err)
	})
}

func TestClientProcessor_Process_Negative(t *testing.T) {

	logger, _ := setupLogsCapture()
	t.Run("error setting connection read deadline", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		connMock := NewMockConnection(ctrl)
		connMock.EXPECT().SetReadDeadline(gomock.AssignableToTypeOf(time.Now())).Return(fmt.Errorf("test error"))
		connMock.EXPECT().Close().Return(nil)

		sut := uds.NewProcessor(logger, NewMockSetQIServer(ctrl))

		err := sut.Process(context.Background(), connMock)

		assert.EqualError(t, err, "error setting connection read deadline: test error")
	})

	t.Run("context cancellation on read attempt", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		connMock := NewMockConnection(ctrl)
		connMock.EXPECT().SetReadDeadline(gomock.AssignableToTypeOf(time.Now())).Return(nil)
		connMock.EXPECT().Close().Return(nil)

		sut := uds.NewProcessor(logger, NewMockSetQIServer(ctrl))

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err := sut.Process(ctx, connMock)

		assert.EqualError(t, err, "error handling connection: context canceled")
	})

	t.Run("unexpected EOF", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		connMock := NewMockConnection(ctrl)
		connMock.EXPECT().SetReadDeadline(gomock.AssignableToTypeOf(time.Now())).Return(nil)
		connMock.
			EXPECT().
			Read(gomock.AssignableToTypeOf([]byte{})).
			DoAndReturn(connReadError(10, io.EOF))
		connMock.EXPECT().Close().Return(nil)

		sut := uds.NewProcessor(logger, NewMockSetQIServer(ctrl))

		err := sut.Process(context.Background(), connMock)

		assert.EqualError(t, err, "unexpected EOF: read 10 bytes and got EOF")
	})

	t.Run("error reading from connection", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		connMock := NewMockConnection(ctrl)
		connMock.EXPECT().SetReadDeadline(gomock.AssignableToTypeOf(time.Now())).Return(nil)
		connMock.
			EXPECT().
			Read(gomock.AssignableToTypeOf([]byte{})).
			DoAndReturn(connReadError(0, fmt.Errorf("test error")))
		connMock.EXPECT().Close().Return(nil)

		sut := uds.NewProcessor(logger, NewMockSetQIServer(ctrl))

		err := sut.Process(context.Background(), connMock)

		assert.EqualError(t, err, "error reading from connection: test error")
	})

	t.Run("short message", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		connMock := NewMockConnection(ctrl)
		connMock.EXPECT().SetReadDeadline(gomock.AssignableToTypeOf(time.Now())).Return(nil)
		connMock.
			EXPECT().
			Read(gomock.AssignableToTypeOf([]byte{})).
			DoAndReturn(connRead([]byte("Hel")))
		connMock.EXPECT().Close().Return(nil)

		sut := uds.NewProcessor(logger, NewMockSetQIServer(ctrl))

		err := sut.Process(context.Background(), connMock)

		assert.EqualError(t, err, "got empty message of len 3")
	})

	t.Run("incomplete message", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		connMock := NewMockConnection(ctrl)
		connMock.EXPECT().SetReadDeadline(gomock.AssignableToTypeOf(time.Now())).Return(nil)
		connMock.
			EXPECT().
			Read(gomock.AssignableToTypeOf([]byte{})).
			DoAndReturn(connRead([]byte{2, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1}))
		connMock.EXPECT().Close().Return(nil)

		sut := uds.NewProcessor(logger, NewMockSetQIServer(ctrl))

		err := sut.Process(context.Background(), connMock)

		assert.EqualError(t, err, "got incomplete message expected size 10 got 11")
	})

	t.Run("error unmarshalling message", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		sut := uds.NewProcessor(logger, NewMockSetQIServer(ctrl))

		connMock := NewMockConnection(ctrl)
		connMock.EXPECT().SetReadDeadline(gomock.AssignableToTypeOf(time.Now())).Return(nil)
		connMock.EXPECT().Read(gomock.AssignableToTypeOf([]byte{})).DoAndReturn(connRead([]byte{1, 0, 0, 0, '{'}))
		connMock.EXPECT().Close().Return(nil)

		err := sut.Process(context.Background(), connMock)

		assert.Error(t, err)
		assert.Regexp(t, `error unmarshalling message:.*`, err.Error())
	})

	t.Run("error processing message", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		ctx := context.Background()
		message := &pb.SetQueryReq{QueryKey: &greenplum.QueryKey{Ccnt: 1}}

		connMock := NewMockConnection(ctrl)
		connMock.EXPECT().SetReadDeadline(gomock.AssignableToTypeOf(time.Now())).Return(nil)
		connMock.
			EXPECT().
			Read(gomock.AssignableToTypeOf([]byte{})).
			DoAndReturn(connRead(messageBytesWithHeader(t, message))).
			AnyTimes()
		connMock.EXPECT().Close().Return(nil)

		setQIServerMock := NewMockSetQIServer(ctrl)
		setQIServerMock.
			EXPECT().
			SetMetricQuery(ctx, newProtoMessageMatcher(message)).
			Return(nil, fmt.Errorf("test error"))

		sut := uds.NewProcessor(logger, setQIServerMock)

		err := sut.Process(context.Background(), connMock)
		assert.EqualError(t, err, "error processing message: test error")
	})
}

func connRead(data []byte) func([]byte) (int, error) {
	b := bytes.NewBuffer(data)
	return func(in []byte) (int, error) {
		return b.Read(in)
	}
}

func connReadError(readN int, err error) func([]byte) (int, error) {
	return func([]byte) (int, error) {
		return readN, err
	}
}

func messageBytesWithHeader(t *testing.T, message proto.Message) []byte {
	messageBytes, err := proto.Marshal(message)
	require.NoError(t, err)
	msgWithHeader := make([]byte, 4)
	binary.LittleEndian.PutUint32(msgWithHeader, uint32(len(messageBytes)))
	return append(msgWithHeader, messageBytes...)
}

func (p *protoMessageMatcher) Matches(x interface{}) bool {
	givenMessage, ok := x.(proto.Message)
	if !ok {
		p.errorMessage = "given value doesn't implement proto.Message interface"
		return false
	}

	if !proto.Equal(p.expected, givenMessage) {
		p.errorMessage = fmt.Sprintf(
			"given is not equal to expected:\n\t"+
				"expected: %#v\n\t"+
				"given:    %#v",
			p.expected,
			givenMessage,
		)
		return false
	}

	return true
}

func (p *protoMessageMatcher) String() string {
	return p.errorMessage
}

func newProtoMessageMatcher(expected proto.Message) *protoMessageMatcher {
	return &protoMessageMatcher{expected: expected}
}

type protoMessageMatcher struct {
	expected     proto.Message
	errorMessage string
}
