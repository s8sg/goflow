package eventhandler

import (
	"testing"
)

type noOpTraceHandler struct{}

func (m *noOpTraceHandler) StartReqSpan(reqID string)                                        {}
func (m *noOpTraceHandler) ContinueReqSpan(reqID string, header map[string][]string)         {}
func (m *noOpTraceHandler) StopReqSpan()                                                     {}
func (m *noOpTraceHandler) StartNodeSpan(node string, reqID string)                          {}
func (m *noOpTraceHandler) StopNodeSpan(node string)                                         {}
func (m *noOpTraceHandler) StartOperationSpan(node string, reqID string, operationID string) {}
func (m *noOpTraceHandler) StopOperationSpan(node string, operationID string)                {}
func (m *noOpTraceHandler) FlushTracer()                                                     {}

func TestGoFlowEventHandler_Configure(t *testing.T) {
	eh := &GoFlowEventHandler{}
	eh.Configure("testFlow", "testReq")
	if eh.flowName != "testFlow" {
		t.Errorf("expected flowName to be 'testFlow', got %v", eh.flowName)
	}
}

func TestGoFlowEventHandler_Init(t *testing.T) {
	eh := &GoFlowEventHandler{flowName: "test", TraceURI: "localhost:6831"}
	err := eh.Init()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestGoFlowEventHandler_Copy(t *testing.T) {
	eh := &GoFlowEventHandler{TraceURI: "uri", CurrentNodeID: "node", Header: map[string][]string{"k": {"v"}}}
	copy, err := eh.Copy()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	copied, ok := copy.(*GoFlowEventHandler)
	if !ok {
		t.Errorf("expected *GoFlowEventHandler, got %T", copy)
	}
	if copied.TraceURI != eh.TraceURI || copied.CurrentNodeID != eh.CurrentNodeID {
		t.Error("fields not copied correctly")
	}
}

func TestGoFlowEventHandler_ReportMethods(t *testing.T) {
	eh := &GoFlowEventHandler{Tracer: &noOpTraceHandler{}}
	eh.ReportRequestStart("req")
	eh.ReportRequestFailure("req", nil)
	eh.ReportExecutionForward("node", "req")
	eh.Header = map[string][]string{}
	eh.ReportExecutionContinuation("req")
	eh.ReportRequestEnd("req")
	eh.ReportNodeStart("node", "req")
	eh.ReportNodeEnd("node", "req")
	eh.ReportNodeFailure("node", "req", nil)
	eh.ReportOperationStart("op", "node", "req")
	eh.ReportOperationEnd("op", "node", "req")
	eh.ReportOperationFailure("op", "node", "req", nil)
	eh.Flush()
}
