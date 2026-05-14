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

//go:generate mockgen -source=deps.go -package=master_sentinel_test -mock_names log=MockLog,db=MockDB -destination mocks_test.go
package master_sentinel

import "context"

type log interface {
	Warnf(format string, args ...any)
}

type db interface {
	ExecQueryNoRetry(ctx context.Context, query string, dest any) error
}
