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
	"net"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	segConnections    = make(map[string]*grpc.ClientConn)
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
			net.JoinHostPort(hostname, strconv.FormatUint(uint64(portn), 10)),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithConnectParams(grpc.ConnectParams{
				MinConnectTimeout: connectTimeout,
			}),
		)
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
	}
	if err != nil {
		return nil, err
	}
	segConnections[hostname] = conn
	return conn, nil
}
