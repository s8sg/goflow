package eventhandler

import (
	"fmt"
	"github.com/s8sg/goflow/core/runtime"
	"net/http"
	"sync"

	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"

	"io"
)

type TraceHandler struct {
	tracer opentracing.Tracer
	closer io.Closer

	reqSpan    opentracing.Span
	reqSpanCtx opentracing.SpanContext

	nodeSpans      sync.Map //map[string]opentracing.Span
	operationSpans sync.Map //map[string]map[string]opentracing.Span
}

// StartReqSpan starts a request span
func (tracerObj *TraceHandler) StartReqSpan(reqID string) {
	tracerObj.reqSpan = tracerObj.tracer.StartSpan(reqID)
	tracerObj.reqSpan.SetTag("request", reqID)
	tracerObj.reqSpanCtx = tracerObj.reqSpan.Context()
}

// ContinueReqSpan continue request span
func (tracerObj *TraceHandler) ContinueReqSpan(reqID string, header map[string][]string) {
	var err error

	tracerObj.reqSpanCtx, err = tracerObj.tracer.Extract(
		opentracing.HTTPHeaders,
		opentracing.HTTPHeadersCarrier(header),
	)
	if err != nil {
		fmt.Printf("[Request %s] failed to continue req span for tracing, error %v\n", reqID, err)
		return
	}

	tracerObj.reqSpan = nil
	// TODO: Its not Supported to get span from spanContext as of now
	//       https://github.com/opentracing/specification/issues/81
	//       it will support us to extend the request span for nodes
	//reqSpan = opentracing.SpanFromContext(reqSpanCtx)
}

// ExtendReqSpan extend req span over a request
// func ExtendReqSpan(url string, req *http.Request) {
func (tracerObj *TraceHandler) ExtendReqSpan(reqID string, lastNode string, url string, req *runtime.Request) {
	// TODO: as requestSpan can't be regenerated with the span context we
	//       forward the nodes SpanContext
	// span := reqSpan
	value, ok := tracerObj.nodeSpans.Load(lastNode)
	if !ok || value == nil {
		return
	}
	span := value.(opentracing.Span)

	ext.SpanKindRPCClient.Set(span)
	ext.HTTPUrl.Set(span, url)
	ext.HTTPMethod.Set(span, "POST")

	header := make(http.Header)
	err := span.Tracer().Inject(
		span.Context(),
		opentracing.HTTPHeaders,
		opentracing.HTTPHeadersCarrier(header),
	)
	if err != nil {
		fmt.Printf("[Request %s] failed to extend req span for tracing, error %v\n", reqID, err)
	}
	if header.Get("Uber-Trace-Id") == "" {
		fmt.Printf("[Request %s] failed to extend req span for tracing, error Uber-Trace-Id not set\n",
			reqID)
	}
	req.Header["Uber-Trace-Id"] = []string{header.Get("Uber-Trace-Id")}
}

// StopReqSpan terminate a request span
func (tracerObj *TraceHandler) StopReqSpan() {
	if tracerObj.reqSpan == nil {
		return
	}

	tracerObj.reqSpan.Finish()
}

// StartNodeSpan starts a node span
func (tracerObj *TraceHandler) StartNodeSpan(node string, reqID string) {

	tracerObj.nodeSpans.Store(node, tracerObj.tracer.StartSpan(
		node, ext.RPCServerOption(tracerObj.reqSpanCtx)))

	/*
		 tracerObj.nodeSpans[node] = tracerObj.Tracer.StartSpan(
			node, opentracing.ChildOf(reqSpan.Context()))
	*/

	value, ok := tracerObj.nodeSpans.Load(node)
	if !ok {
		return
	}

	span := value.(opentracing.Span)
	span.SetTag("async", "true")
	span.SetTag("request", reqID)
	span.SetTag("node", node)

	tracerObj.nodeSpans.Store(node, span)
}

// StopNodeSpan terminates a node span
func (tracerObj *TraceHandler) StopNodeSpan(node string) {

	value, ok := tracerObj.nodeSpans.Load(node)
	if !ok {
		return
	}

	span := value.(opentracing.Span)
	span.Finish()
	tracerObj.nodeSpans.Store(node, span)
}

// StartOperationSpan starts an operation span
func (tracerObj *TraceHandler) StartOperationSpan(node string, reqID string, operationID string) {

	if _, ok := tracerObj.nodeSpans.Load(node); !ok {
		return
	}

	value2, ok := tracerObj.operationSpans.Load(node)
	var operationSpans map[string]opentracing.Span
	if !ok {
		operationSpans = make(map[string]opentracing.Span)
		tracerObj.operationSpans.Store(node, operationSpans)
	} else {
		operationSpans = value2.(map[string]opentracing.Span)
	}

	value, ok := tracerObj.nodeSpans.Load(node)
	if !ok {
		return
	}

	span := value.(opentracing.Span)
	nodeContext := span.Context()
	operationSpans[operationID] = tracerObj.tracer.StartSpan(
		operationID, opentracing.ChildOf(nodeContext))

	operationSpans[operationID].SetTag("request", reqID)
	operationSpans[operationID].SetTag("node", node)
	operationSpans[operationID].SetTag("operation", operationID)
}

// StopOperationSpan stops an operation span
func (tracerObj *TraceHandler) StopOperationSpan(node string, operationID string) {

	value, ok := tracerObj.operationSpans.Load(node)
	if !ok {
		return
	}

	operationSpans := value.(map[string]opentracing.Span)
	operationSpans[operationID].Finish()
}

// FlushTracer flush all pending traces
func (tracerObj *TraceHandler) FlushTracer() {
	tracerObj.closer.Close()
}
