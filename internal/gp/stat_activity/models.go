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

package stat_activity

import "time"

type Session struct {
	DatID            int
	Datname          string
	Pid              int
	SessID           int
	TmID             int
	UsesysID         int
	Usename          string
	ApplicationName  *string
	ClientAddr       *string
	ClientHostname   *string
	ClientPort       *int
	BackendStart     *time.Time
	XactStart        *time.Time
	QueryStart       *time.Time
	StateChange      *time.Time
	Waiting          *bool
	State            *string
	BackendXid       *string
	BackendXmin      *string
	Query            *string
	WaitingReason    *string
	Rsgid            *int
	Rsgname          *string
	Rsgqueueduration *string
	WaitEvent        *string
	WaitEventType    *string
}

type SessionPid struct {
	GpSegmentId int
	Pid         int
	SessId      int
	BackendType string
}

type SessionLock struct {
	BlockSessID     int
	BlockedBySessID int
	WaitMode        string
	LockedItem      string
	LockedMode      string
}
