package eventhandler

import (
	"fmt"
)

// implements faasflow.EventHandler
type FaasEventHandler struct {
	CurrentNodeID string        // used to inject current node id in Tracer
	Tracer        *TraceHandler // handle traces with open-tracing
	flowName      string
	TraceURI      string
	Header        map[string][]string
}

func (eh *FaasEventHandler) Configure(flowName string, requestID string) {
	eh.flowName = flowName
}

func (eh *FaasEventHandler) Init() error {
	var err error

	// initialize trace server if tracing enabled
	eh.Tracer, err = initRequestTracer(eh.flowName, eh.TraceURI)
	if err != nil {
		return fmt.Errorf("failed to init request Tracer, error %v", err)
	}
	return nil
}

func (eh *FaasEventHandler) ReportRequestStart(requestID string) {
	eh.Tracer.StartReqSpan(requestID)
}

func (eh *FaasEventHandler) ReportRequestFailure(requestID string, err error) {
	// TODO: add log
	eh.Tracer.StopReqSpan()
}

func (eh *FaasEventHandler) ReportExecutionForward(currentNodeID string, requestID string) {
	eh.CurrentNodeID = currentNodeID
}

func (eh *FaasEventHandler) ReportExecutionContinuation(requestID string) {
	eh.Tracer.ContinueReqSpan(requestID, eh.Header)
}

func (eh *FaasEventHandler) ReportRequestEnd(requestID string) {
	eh.Tracer.StopReqSpan()
}

func (eh *FaasEventHandler) ReportNodeStart(nodeID string, requestID string) {
	eh.Tracer.StartNodeSpan(nodeID, requestID)
}

func (eh *FaasEventHandler) ReportNodeEnd(nodeID string, requestID string) {
	eh.Tracer.StopNodeSpan(nodeID)
}

func (eh *FaasEventHandler) ReportNodeFailure(nodeID string, requestID string, err error) {
	// TODO: add log
	eh.Tracer.StopNodeSpan(nodeID)
}

func (eh *FaasEventHandler) ReportOperationStart(operationID string, nodeID string, requestID string) {
	eh.Tracer.StartOperationSpan(nodeID, requestID, operationID)
}

func (eh *FaasEventHandler) ReportOperationEnd(operationID string, nodeID string, requestID string) {
	eh.Tracer.StopOperationSpan(nodeID, operationID)
}

func (eh *FaasEventHandler) ReportOperationFailure(operationID string, nodeID string, requestID string, err error) {
	// TODO: add log
	eh.Tracer.StopOperationSpan(nodeID, operationID)
}

func (eh *FaasEventHandler) Flush() {
	eh.Tracer.FlushTracer()
}
