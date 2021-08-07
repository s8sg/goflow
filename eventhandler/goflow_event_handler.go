package eventhandler

import (
	"fmt"
)

// implements core.EventHandler
type GoFlowEventHandler struct {
	CurrentNodeID string        // used to inject current node id in Tracer
	Tracer        *TraceHandler // handle traces with open-tracing
	flowName      string
	TraceURI      string
	Header        map[string][]string
}

func (eh *GoFlowEventHandler) Configure(flowName string, requestID string) {
	eh.flowName = flowName
}

func (eh *GoFlowEventHandler) Init() error {
	var err error

	// initialize trace server if tracing enabled
	eh.Tracer, err = initRequestTracer(eh.flowName, eh.TraceURI)
	if err != nil {
		return fmt.Errorf("failed to init request Tracer, error %v", err)
	}
	return nil
}

func (eh *GoFlowEventHandler) ReportRequestStart(requestID string) {
	eh.Tracer.StartReqSpan(requestID)
}

func (eh *GoFlowEventHandler) ReportRequestFailure(requestID string, err error) {
	// TODO: add log
	eh.Tracer.StopReqSpan()
}

func (eh *GoFlowEventHandler) ReportExecutionForward(currentNodeID string, requestID string) {
	eh.CurrentNodeID = currentNodeID
}

func (eh *GoFlowEventHandler) ReportExecutionContinuation(requestID string) {
	eh.Tracer.ContinueReqSpan(requestID, eh.Header)
}

func (eh *GoFlowEventHandler) ReportRequestEnd(requestID string) {
	eh.Tracer.StopReqSpan()
}

func (eh *GoFlowEventHandler) ReportNodeStart(nodeID string, requestID string) {
	eh.Tracer.StartNodeSpan(nodeID, requestID)
}

func (eh *GoFlowEventHandler) ReportNodeEnd(nodeID string, requestID string) {
	eh.Tracer.StopNodeSpan(nodeID)
}

func (eh *GoFlowEventHandler) ReportNodeFailure(nodeID string, requestID string, err error) {
	// TODO: add log
	eh.Tracer.StopNodeSpan(nodeID)
}

func (eh *GoFlowEventHandler) ReportOperationStart(operationID string, nodeID string, requestID string) {
	eh.Tracer.StartOperationSpan(nodeID, requestID, operationID)
}

func (eh *GoFlowEventHandler) ReportOperationEnd(operationID string, nodeID string, requestID string) {
	eh.Tracer.StopOperationSpan(nodeID, operationID)
}

func (eh *GoFlowEventHandler) ReportOperationFailure(operationID string, nodeID string, requestID string, err error) {
	// TODO: add log
	eh.Tracer.StopOperationSpan(nodeID, operationID)
}

func (eh *GoFlowEventHandler) Flush() {
	eh.Tracer.FlushTracer()
}
