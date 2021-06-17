// Code generated by MockGen. DO NOT EDIT.
// Source: message_adapter.go

// Package persistent is a generated GoMock package.
package persistent

import (
	reflect "reflect"

	messages "github.com/EventStore/EventStore-Client-Go/messages"
	persistent "github.com/EventStore/EventStore-Client-Go/protos/persistent"
	gomock "github.com/golang/mock/gomock"
)

// MockmessageAdapter is a mock of messageAdapter interface.
type MockmessageAdapter struct {
	ctrl     *gomock.Controller
	recorder *MockmessageAdapterMockRecorder
}

// MockmessageAdapterMockRecorder is the mock recorder for MockmessageAdapter.
type MockmessageAdapterMockRecorder struct {
	mock *MockmessageAdapter
}

// NewMockmessageAdapter creates a new mock instance.
func NewMockmessageAdapter(ctrl *gomock.Controller) *MockmessageAdapter {
	mock := &MockmessageAdapter{ctrl: ctrl}
	mock.recorder = &MockmessageAdapterMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockmessageAdapter) EXPECT() *MockmessageAdapterMockRecorder {
	return m.recorder
}

// FromProtoResponse mocks base method.
func (m *MockmessageAdapter) FromProtoResponse(resp *persistent.ReadResp) *messages.RecordedEvent {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "FromProtoResponse", resp)
	ret0, _ := ret[0].(*messages.RecordedEvent)
	return ret0
}

// FromProtoResponse indicates an expected call of FromProtoResponse.
func (mr *MockmessageAdapterMockRecorder) FromProtoResponse(resp interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "FromProtoResponse", reflect.TypeOf((*MockmessageAdapter)(nil).FromProtoResponse), resp)
}
