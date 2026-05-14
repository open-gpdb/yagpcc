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

//go:generate mockgen -source=grpc_test.go -package=grpc_test -mock_names statActivityLister=MockStatActivityLister -destination mocks_test.go

package grpc_test

import (
	"context"

	"github.com/open-gpdb/yagpcc/internal/gp"
	"github.com/open-gpdb/yagpcc/internal/gp/stat_activity"
)

type statActivityLister interface { //nolint:unused // used by go:generate mockgen
	Start(ctx context.Context) error
	Stop()
	List(ctx context.Context) ([]*gp.GpStatActivity, error)
	ListAllSessions(context.Context) ([]stat_activity.SessionPid, error)
}
