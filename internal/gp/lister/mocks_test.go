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

package lister_test

import (
	"context"
	"reflect"

	gomock "github.com/golang/mock/gomock"
)

// MockLog is a hand-written mock of the lister.Log interface.
type MockLog struct {
	ctrl     *gomock.Controller
	recorder *MockLogMockRecorder
}

type MockLogMockRecorder struct{ mock *MockLog }

func NewMockLog(ctrl *gomock.Controller) *MockLog {
	m := &MockLog{ctrl: ctrl}
	m.recorder = &MockLogMockRecorder{m}
	return m
}

func (m *MockLog) EXPECT() *MockLogMockRecorder { return m.recorder }

func (m *MockLog) Infof(format string, args ...any) {
	m.ctrl.T.Helper()
	varargs := []interface{}{format}
	for _, a := range args {
		varargs = append(varargs, a)
	}
	m.ctrl.Call(m, "Infof", varargs...)
}

func (mr *MockLogMockRecorder) Infof(format interface{}, args ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{format}, args...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Infof", reflect.TypeOf((*MockLog)(nil).Infof), varargs...)
}

func (m *MockLog) Warnf(format string, args ...any) {
	m.ctrl.T.Helper()
	varargs := []interface{}{format}
	for _, a := range args {
		varargs = append(varargs, a)
	}
	m.ctrl.Call(m, "Warnf", varargs...)
}

func (mr *MockLogMockRecorder) Warnf(format interface{}, args ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{format}, args...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Warnf", reflect.TypeOf((*MockLog)(nil).Warnf), varargs...)
}

// MockDB is a hand-written mock of the lister.DB interface.
type MockDB struct {
	ctrl     *gomock.Controller
	recorder *MockDBMockRecorder
}

type MockDBMockRecorder struct{ mock *MockDB }

func NewMockDB(ctrl *gomock.Controller) *MockDB {
	m := &MockDB{ctrl: ctrl}
	m.recorder = &MockDBMockRecorder{m}
	return m
}

func (m *MockDB) EXPECT() *MockDBMockRecorder { return m.recorder }

func (m *MockDB) ExecQuery(ctx context.Context, query string, dest any) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ExecQuery", ctx, query, dest)
	ret0, _ := ret[0].(error)
	return ret0
}

func (mr *MockDBMockRecorder) ExecQuery(ctx, query, dest interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ExecQuery", reflect.TypeOf((*MockDB)(nil).ExecQuery), ctx, query, dest)
}
