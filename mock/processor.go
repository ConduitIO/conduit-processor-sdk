// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/conduitio/conduit-processor-sdk (interfaces: Processor)
//
// Generated by this command:
//
//	mockgen -destination=mock/processor.go -package=mock -mock_names=Processor=Processor . Processor
//
// Package mock is a generated GoMock package.
package mock

import (
	context "context"
	reflect "reflect"

	opencdc "github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-processor-sdk"
	gomock "go.uber.org/mock/gomock"
)

// Processor is a mock of Processor interface.
type Processor struct {
	ctrl     *gomock.Controller
	recorder *ProcessorMockRecorder
}

// ProcessorMockRecorder is the mock recorder for Processor.
type ProcessorMockRecorder struct {
	mock *Processor
}

// NewProcessor creates a new mock instance.
func NewProcessor(ctrl *gomock.Controller) *Processor {
	mock := &Processor{ctrl: ctrl}
	mock.recorder = &ProcessorMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *Processor) EXPECT() *ProcessorMockRecorder {
	return m.recorder
}

// Configure mocks base method.
func (m *Processor) Configure(arg0 context.Context, arg1 map[string]string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Configure", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// Configure indicates an expected call of Configure.
func (mr *ProcessorMockRecorder) Configure(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Configure", reflect.TypeOf((*Processor)(nil).Configure), arg0, arg1)
}

// Open mocks base method.
func (m *Processor) Open(arg0 context.Context) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Open", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// Open indicates an expected call of Open.
func (mr *ProcessorMockRecorder) Open(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Open", reflect.TypeOf((*Processor)(nil).Open), arg0)
}

// Process mocks base method.
func (m *Processor) Process(arg0 context.Context, arg1 []opencdc.Record) []sdk.ProcessedRecord {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Process", arg0, arg1)
	ret0, _ := ret[0].([]sdk.ProcessedRecord)
	return ret0
}

// Process indicates an expected call of Process.
func (mr *ProcessorMockRecorder) Process(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Process", reflect.TypeOf((*Processor)(nil).Process), arg0, arg1)
}

// Specification mocks base method.
func (m *Processor) Specification() sdk.Specification {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Specification")
	ret0, _ := ret[0].(sdk.Specification)
	return ret0
}

// Specification indicates an expected call of Specification.
func (mr *ProcessorMockRecorder) Specification() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Specification", reflect.TypeOf((*Processor)(nil).Specification))
}

// Teardown mocks base method.
func (m *Processor) Teardown(arg0 context.Context) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Teardown", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// Teardown indicates an expected call of Teardown.
func (mr *ProcessorMockRecorder) Teardown(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Teardown", reflect.TypeOf((*Processor)(nil).Teardown), arg0)
}
