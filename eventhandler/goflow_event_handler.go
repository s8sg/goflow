package eventhandler

import (
	"fmt"

	"github.com/s8sg/goflow/core/sdk"
)

// Tracer interface for testability
type Tracer interface {
	StartReqSpan(reqID string)
	ContinueReqSpan(reqID string, header map[string][]string)
	StopReqSpan()
	StartNodeSpan(node string, reqID string)
	StopNodeSpan(node string)
	StartOperationSpan(node string, reqID string, operationID string)
	StopOperationSpan(node string, operationID string)
	FlushTracer()
}

// implements core.EventHandler
type GoFlowEventHandler struct {
	CurrentNodeID string // used to inject current node id in Tracer
	Tracer        Tracer // handle traces with open-tracing
	flowName      string
	TraceURI      string
	Header        map[string][]string
}

func (eh *GoFlowEventHandler) Configure(flowName string, requestID string) {
	eh.flowName = flowName
}

func (eh *GoFlowEventHandler) Init() error {
	var err error
	var tracer *TraceHandler
	tracer, err = initRequestTracer(eh.flowName, eh.TraceURI)
	if err != nil {
		return fmt.Errorf("failed to init request Tracer, error %v", err)
	}
	eh.Tracer = tracer
	return nil
}

func (eh *GoFlowEventHandler) Copy() (sdk.EventHandler, error) {

	newHandler := &GoFlowEventHandler{}
	newHandler.TraceURI = eh.TraceURI
	newHandler.CurrentNodeID = eh.CurrentNodeID
	newHandler.Header = eh.Header

	return newHandler, nil
}

func (eh *GoFlowEventHandler) ReportRequestStart(requestID string) {
	if eh.Tracer != nil {
		eh.Tracer.StartReqSpan(requestID)
	}
}

func (eh *GoFlowEventHandler) ReportRequestFailure(requestID string, err error) {
	// TODO: add log
	if eh.Tracer != nil {
		eh.Tracer.StopReqSpan()
	}
}

func (eh *GoFlowEventHandler) ReportExecutionForward(currentNodeID string, requestID string) {
	eh.CurrentNodeID = currentNodeID
}

func (eh *GoFlowEventHandler) ReportExecutionContinuation(requestID string) {
	if eh.Tracer != nil {
		eh.Tracer.ContinueReqSpan(requestID, eh.Header)
	}
}

func (eh *GoFlowEventHandler) ReportRequestEnd(requestID string) {
	if eh.Tracer != nil {
		eh.Tracer.StopReqSpan()
	}
}

func (eh *GoFlowEventHandler) ReportNodeStart(nodeID string, requestID string) {
	if eh.Tracer != nil {
		eh.Tracer.StartNodeSpan(nodeID, requestID)
	}
}

func (eh *GoFlowEventHandler) ReportNodeEnd(nodeID string, requestID string) {
	if eh.Tracer != nil {
		eh.Tracer.StopNodeSpan(nodeID)
	}
}

func (eh *GoFlowEventHandler) ReportNodeFailure(nodeID string, requestID string, err error) {
	// TODO: add log
	if eh.Tracer != nil {
		eh.Tracer.StopNodeSpan(nodeID)
	}
}

func (eh *GoFlowEventHandler) ReportOperationStart(operationID string, nodeID string, requestID string) {
	if eh.Tracer != nil {
		eh.Tracer.StartOperationSpan(nodeID, requestID, operationID)
	}
}

func (eh *GoFlowEventHandler) ReportOperationEnd(operationID string, nodeID string, requestID string) {
	if eh.Tracer != nil {
		eh.Tracer.StopOperationSpan(nodeID, operationID)
	}
}

func (eh *GoFlowEventHandler) ReportOperationFailure(operationID string, nodeID string, requestID string, err error) {
	// TODO: add log
	if eh.Tracer != nil {
		eh.Tracer.StopOperationSpan(nodeID, operationID)
	}
}

func (eh *GoFlowEventHandler) Flush() {
	if eh.Tracer != nil {
		eh.Tracer.FlushTracer()
	}
}
