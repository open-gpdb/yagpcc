package master

import (
	"context"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	segConnections    map[string]*grpc.ClientConn = make(map[string]*grpc.ClientConn)
	segConnectionLock sync.Mutex
)

func getGrpcClientConnection(ctx context.Context, hostname string, portn uint32, segConnectTimeoutSec float64) (*grpc.ClientConn, error) {
	var err error
	segConnectionLock.Lock()
	defer segConnectionLock.Unlock()
	conn, ok := segConnections[hostname]
	if ok {
		if conn.GetState() != connectivity.Shutdown {
			return conn, nil
		}
	}
	connectTimeout := time.Second * time.Duration(segConnectTimeoutSec)
	if portn > 0 {
		conn, err = grpc.NewClient(
			getSegAddr(hostname, portn),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithConnectParams(grpc.ConnectParams{
				MinConnectTimeout: connectTimeout,
			}),
		)
		if err != nil {
			return nil, err
		}
	} else {
		conn, err = grpc.NewClient(
			hostname,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, "unix", addr)
			}),
			grpc.WithConnectParams(grpc.ConnectParams{
				MinConnectTimeout: connectTimeout,
			}),
		)
		if err != nil {
			return nil, err
		}
	}
	segConnections[hostname] = conn
	return conn, nil
}
