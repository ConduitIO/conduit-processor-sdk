// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/conduitio/conduit-processor-sdk (interfaces: Processor)
//
// Generated by this command:
//
//	mockgen -typed -destination=mock_destination_test.go -self_package=github.com/conduitio/conduit-processor-sdk -package=sdk -write_package_comment=false . Processor
package sdk

import (
	context "context"
	reflect "reflect"

	config "github.com/conduitio/conduit-commons/config"
	opencdc "github.com/conduitio/conduit-commons/opencdc"
	gomock "go.uber.org/mock/gomock"
)

// MockProcessor is a mock of Processor interface.
type MockProcessor struct {
	ctrl     *gomock.Controller
	recorder *MockProcessorMockRecorder
}

// MockProcessorMockRecorder is the mock recorder for MockProcessor.
type MockProcessorMockRecorder struct {
	mock *MockProcessor
}

// NewMockProcessor creates a new mock instance.
func NewMockProcessor(ctrl *gomock.Controller) *MockProcessor {
	mock := &MockProcessor{ctrl: ctrl}
	mock.recorder = &MockProcessorMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockProcessor) EXPECT() *MockProcessorMockRecorder {
	return m.recorder
}

// Configure mocks base method.
func (m *MockProcessor) Configure(arg0 context.Context, arg1 config.Config) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Configure", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// Configure indicates an expected call of Configure.
func (mr *MockProcessorMockRecorder) Configure(arg0, arg1 any) *MockProcessorConfigureCall {
	mr.mock.ctrl.T.Helper()
	call := mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Configure", reflect.TypeOf((*MockProcessor)(nil).Configure), arg0, arg1)
	return &MockProcessorConfigureCall{Call: call}
}

// MockProcessorConfigureCall wrap *gomock.Call
type MockProcessorConfigureCall struct {
	*gomock.Call
}

// Return rewrite *gomock.Call.Return
func (c *MockProcessorConfigureCall) Return(arg0 error) *MockProcessorConfigureCall {
	c.Call = c.Call.Return(arg0)
	return c
}

// Do rewrite *gomock.Call.Do
func (c *MockProcessorConfigureCall) Do(f func(context.Context, config.Config) error) *MockProcessorConfigureCall {
	c.Call = c.Call.Do(f)
	return c
}

// DoAndReturn rewrite *gomock.Call.DoAndReturn
func (c *MockProcessorConfigureCall) DoAndReturn(f func(context.Context, config.Config) error) *MockProcessorConfigureCall {
	c.Call = c.Call.DoAndReturn(f)
	return c
}

// MiddlewareOptions mocks base method.
func (m *MockProcessor) MiddlewareOptions() []ProcessorMiddlewareOption {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "MiddlewareOptions")
	ret0, _ := ret[0].([]ProcessorMiddlewareOption)
	return ret0
}

// MiddlewareOptions indicates an expected call of MiddlewareOptions.
func (mr *MockProcessorMockRecorder) MiddlewareOptions() *MockProcessorMiddlewareOptionsCall {
	mr.mock.ctrl.T.Helper()
	call := mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "MiddlewareOptions", reflect.TypeOf((*MockProcessor)(nil).MiddlewareOptions))
	return &MockProcessorMiddlewareOptionsCall{Call: call}
}

// MockProcessorMiddlewareOptionsCall wrap *gomock.Call
type MockProcessorMiddlewareOptionsCall struct {
	*gomock.Call
}

// Return rewrite *gomock.Call.Return
func (c *MockProcessorMiddlewareOptionsCall) Return(arg0 []ProcessorMiddlewareOption) *MockProcessorMiddlewareOptionsCall {
	c.Call = c.Call.Return(arg0)
	return c
}

// Do rewrite *gomock.Call.Do
func (c *MockProcessorMiddlewareOptionsCall) Do(f func() []ProcessorMiddlewareOption) *MockProcessorMiddlewareOptionsCall {
	c.Call = c.Call.Do(f)
	return c
}

// DoAndReturn rewrite *gomock.Call.DoAndReturn
func (c *MockProcessorMiddlewareOptionsCall) DoAndReturn(f func() []ProcessorMiddlewareOption) *MockProcessorMiddlewareOptionsCall {
	c.Call = c.Call.DoAndReturn(f)
	return c
}

// Open mocks base method.
func (m *MockProcessor) Open(arg0 context.Context) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Open", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// Open indicates an expected call of Open.
func (mr *MockProcessorMockRecorder) Open(arg0 any) *MockProcessorOpenCall {
	mr.mock.ctrl.T.Helper()
	call := mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Open", reflect.TypeOf((*MockProcessor)(nil).Open), arg0)
	return &MockProcessorOpenCall{Call: call}
}

// MockProcessorOpenCall wrap *gomock.Call
type MockProcessorOpenCall struct {
	*gomock.Call
}

// Return rewrite *gomock.Call.Return
func (c *MockProcessorOpenCall) Return(arg0 error) *MockProcessorOpenCall {
	c.Call = c.Call.Return(arg0)
	return c
}

// Do rewrite *gomock.Call.Do
func (c *MockProcessorOpenCall) Do(f func(context.Context) error) *MockProcessorOpenCall {
	c.Call = c.Call.Do(f)
	return c
}

// DoAndReturn rewrite *gomock.Call.DoAndReturn
func (c *MockProcessorOpenCall) DoAndReturn(f func(context.Context) error) *MockProcessorOpenCall {
	c.Call = c.Call.DoAndReturn(f)
	return c
}

// Process mocks base method.
func (m *MockProcessor) Process(arg0 context.Context, arg1 []opencdc.Record) []ProcessedRecord {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Process", arg0, arg1)
	ret0, _ := ret[0].([]ProcessedRecord)
	return ret0
}

// Process indicates an expected call of Process.
func (mr *MockProcessorMockRecorder) Process(arg0, arg1 any) *MockProcessorProcessCall {
	mr.mock.ctrl.T.Helper()
	call := mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Process", reflect.TypeOf((*MockProcessor)(nil).Process), arg0, arg1)
	return &MockProcessorProcessCall{Call: call}
}

// MockProcessorProcessCall wrap *gomock.Call
type MockProcessorProcessCall struct {
	*gomock.Call
}

// Return rewrite *gomock.Call.Return
func (c *MockProcessorProcessCall) Return(arg0 []ProcessedRecord) *MockProcessorProcessCall {
	c.Call = c.Call.Return(arg0)
	return c
}

// Do rewrite *gomock.Call.Do
func (c *MockProcessorProcessCall) Do(f func(context.Context, []opencdc.Record) []ProcessedRecord) *MockProcessorProcessCall {
	c.Call = c.Call.Do(f)
	return c
}

// DoAndReturn rewrite *gomock.Call.DoAndReturn
func (c *MockProcessorProcessCall) DoAndReturn(f func(context.Context, []opencdc.Record) []ProcessedRecord) *MockProcessorProcessCall {
	c.Call = c.Call.DoAndReturn(f)
	return c
}

// Specification mocks base method.
func (m *MockProcessor) Specification() (Specification, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Specification")
	ret0, _ := ret[0].(Specification)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Specification indicates an expected call of Specification.
func (mr *MockProcessorMockRecorder) Specification() *MockProcessorSpecificationCall {
	mr.mock.ctrl.T.Helper()
	call := mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Specification", reflect.TypeOf((*MockProcessor)(nil).Specification))
	return &MockProcessorSpecificationCall{Call: call}
}

// MockProcessorSpecificationCall wrap *gomock.Call
type MockProcessorSpecificationCall struct {
	*gomock.Call
}

// Return rewrite *gomock.Call.Return
func (c *MockProcessorSpecificationCall) Return(arg0 Specification, arg1 error) *MockProcessorSpecificationCall {
	c.Call = c.Call.Return(arg0, arg1)
	return c
}

// Do rewrite *gomock.Call.Do
func (c *MockProcessorSpecificationCall) Do(f func() (Specification, error)) *MockProcessorSpecificationCall {
	c.Call = c.Call.Do(f)
	return c
}

// DoAndReturn rewrite *gomock.Call.DoAndReturn
func (c *MockProcessorSpecificationCall) DoAndReturn(f func() (Specification, error)) *MockProcessorSpecificationCall {
	c.Call = c.Call.DoAndReturn(f)
	return c
}

// Teardown mocks base method.
func (m *MockProcessor) Teardown(arg0 context.Context) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Teardown", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// Teardown indicates an expected call of Teardown.
func (mr *MockProcessorMockRecorder) Teardown(arg0 any) *MockProcessorTeardownCall {
	mr.mock.ctrl.T.Helper()
	call := mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Teardown", reflect.TypeOf((*MockProcessor)(nil).Teardown), arg0)
	return &MockProcessorTeardownCall{Call: call}
}

// MockProcessorTeardownCall wrap *gomock.Call
type MockProcessorTeardownCall struct {
	*gomock.Call
}

// Return rewrite *gomock.Call.Return
func (c *MockProcessorTeardownCall) Return(arg0 error) *MockProcessorTeardownCall {
	c.Call = c.Call.Return(arg0)
	return c
}

// Do rewrite *gomock.Call.Do
func (c *MockProcessorTeardownCall) Do(f func(context.Context) error) *MockProcessorTeardownCall {
	c.Call = c.Call.Do(f)
	return c
}

// DoAndReturn rewrite *gomock.Call.DoAndReturn
func (c *MockProcessorTeardownCall) DoAndReturn(f func(context.Context) error) *MockProcessorTeardownCall {
	c.Call = c.Call.DoAndReturn(f)
	return c
}

// mustEmbedUnimplementedProcessor mocks base method.
func (m *MockProcessor) mustEmbedUnimplementedProcessor() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "mustEmbedUnimplementedProcessor")
}

// mustEmbedUnimplementedProcessor indicates an expected call of mustEmbedUnimplementedProcessor.
func (mr *MockProcessorMockRecorder) mustEmbedUnimplementedProcessor() *MockProcessormustEmbedUnimplementedProcessorCall {
	mr.mock.ctrl.T.Helper()
	call := mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "mustEmbedUnimplementedProcessor", reflect.TypeOf((*MockProcessor)(nil).mustEmbedUnimplementedProcessor))
	return &MockProcessormustEmbedUnimplementedProcessorCall{Call: call}
}

// MockProcessormustEmbedUnimplementedProcessorCall wrap *gomock.Call
type MockProcessormustEmbedUnimplementedProcessorCall struct {
	*gomock.Call
}

// Return rewrite *gomock.Call.Return
func (c *MockProcessormustEmbedUnimplementedProcessorCall) Return() *MockProcessormustEmbedUnimplementedProcessorCall {
	c.Call = c.Call.Return()
	return c
}

// Do rewrite *gomock.Call.Do
func (c *MockProcessormustEmbedUnimplementedProcessorCall) Do(f func()) *MockProcessormustEmbedUnimplementedProcessorCall {
	c.Call = c.Call.Do(f)
	return c
}

// DoAndReturn rewrite *gomock.Call.DoAndReturn
func (c *MockProcessormustEmbedUnimplementedProcessorCall) DoAndReturn(f func()) *MockProcessormustEmbedUnimplementedProcessorCall {
	c.Call = c.Call.DoAndReturn(f)
	return c
}
