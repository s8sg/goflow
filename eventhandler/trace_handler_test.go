package eventhandler

import (
	"testing"

	"github.com/opentracing/opentracing-go/mocktracer"
	"github.com/s8sg/goflow/core/runtime"
)

func TestTraceHandler_StartReqSpan(t *testing.T) {
	th := &TraceHandler{tracer: mocktracer.New()} // use mocktracer
	th.StartReqSpan("req1")
	if th.reqSpan == nil {
		t.Error("reqSpan should not be nil after StartReqSpan")
	}
}

func TestTraceHandler_ContinueReqSpan(t *testing.T) {
	th := &TraceHandler{tracer: mocktracer.New()}
	headers := map[string][]string{"uber-trace-id": {"1:2:3:4"}}
	th.ContinueReqSpan("req1", headers)
	// No panic, just check code path
}

func TestTraceHandler_StopReqSpan(t *testing.T) {
	th := &TraceHandler{tracer: mocktracer.New()}
	th.StartReqSpan("req1")
	th.StopReqSpan() // should finish span
}

func TestTraceHandler_StartNodeSpan_StopNodeSpan(t *testing.T) {
	th := &TraceHandler{tracer: mocktracer.New()}
	th.reqSpanCtx = nil // for coverage
	th.StartNodeSpan("node1", "req1")
	th.StopNodeSpan("node1")
}

func TestTraceHandler_StartOperationSpan_StopOperationSpan(t *testing.T) {
	th := &TraceHandler{tracer: mocktracer.New()}
	th.nodeSpans.Store("node1", mocktracer.New().StartSpan("node1"))
	th.StartOperationSpan("node1", "req1", "op1")
	th.StopOperationSpan("node1", "op1")
}

func TestTraceHandler_ExtendReqSpan(t *testing.T) {
	th := &TraceHandler{tracer: mocktracer.New()}
	th.nodeSpans.Store("node1", mocktracer.New().StartSpan("node1"))
	req := &runtime.Request{Header: map[string][]string{}}
	th.ExtendReqSpan("req1", "node1", "http://example.com", req)
}

func TestTraceHandler_FlushTracer(t *testing.T) {
	th := &TraceHandler{closer: mockCloser{}}
	th.FlushTracer()
}

type mockCloser struct{}

func (m mockCloser) Close() error { return nil }
