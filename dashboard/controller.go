package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
)

// HtmlObject object to render web page
type HtmlObject struct {
	PublicURL string
	Functions []*Flow

	LocationDepths []*Location

	CurrentLocation *Location

	InnerHtml string

	DashBoard *DashboardSpec
	Flow      *FlowDesc
	Requests  *FlowRequests
	Traces    *RequestTrace
}

// Message API request query
type Message struct {
	FlowName  string `json:"function"`
	RequestID string `json:"request-id"`
	TraceID   string `json:"trace-id"`
	Data      string `json:"data"`
}

// dashboardPageHandler handle dashboard view
func dashboardPageHandler(w http.ResponseWriter, r *http.Request) {
	log.Printf("Serving request for dashboard view")

	functions, err := listGoFLows()
	if err != nil {
		log.Printf("failed to get functions, error: %v", err)
		functions = make([]*Flow, 0)
	}

	totalRequests := 0
	for _, function := range functions {
		requests, err := listFlowRequests(function.Name)
		if err != nil {
			log.Printf("failed to get requests, error: %v", err)
			continue
		}
		totalRequests = totalRequests + len(requests)
	}

	dashboardSpec := &DashboardSpec{
		TotalFlows:     len(functions),
		ReadyFlows:     len(functions),
		TotalRequests:  totalRequests,
		ActiveRequests: 0,
	}

	htmlObj := HtmlObject{
		PublicURL: publicUri,
		Functions: functions,

		InnerHtml: "dashboard",

		DashBoard: dashboardSpec,
	}

	err = gen.ExecuteTemplate(w, "index", htmlObj)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to generate requested page, error: %v", err), http.StatusInternalServerError)
	}

}

// flowInfoPageHandler handle flow info page view
func flowInfoPageHandler(w http.ResponseWriter, r *http.Request) {
	log.Printf("Serving request for dashboard view")

	flowName := r.URL.Query().Get("flow-name")

	functions, err := listGoFLows()
	if err != nil {
		log.Printf("failed to get functions, error: %v", err)
		functions = make([]*Flow, 0)
	}

	flowDesc, err := buildFlowDesc(functions, flowName)
	if err != nil {
		log.Printf("failed to get function desc, error: %v", err)
	}

	requests, err := listFlowRequests(flowName)
	if err != nil {
		log.Printf("failed to get requests, error: %v", err)
		flowDesc.InvocationCount = 0
	} else {
		flowDesc.InvocationCount = len(requests)
	}

	htmlObj := HtmlObject{
		PublicURL: publicUri,
		Functions: functions,

		CurrentLocation: &Location{
			Name: "Flow : " + flowName + "",
			Link: "/flow/info?flow-name=" + flowName,
		},

		InnerHtml: "flow-info",

		Flow: flowDesc,
	}

	err = gen.ExecuteTemplate(w, "index", htmlObj)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to generate requested page, error: %v", err), http.StatusInternalServerError)
	}
}

// flowRequestsPageHandler handle tracing view
func flowRequestsPageHandler(w http.ResponseWriter, r *http.Request) {
	log.Printf("Serving request for request list view")

	flowName := r.URL.Query().Get("flow-name")

	functions, err := listGoFLows()
	if err != nil {
		log.Printf("failed to get functions, error: %v", err)
		functions = make([]*Flow, 0)
	}

	tracingEnabled := false
	requests, err := listFlowRequests(flowName)
	if err != nil {
		log.Printf("failed to get requests, error: %v", err)
		requests = make(map[string]string)
	}

	requestsList := make(map[string]*RequestTrace)

	for request, traceId := range requests {
		requestsList[request], err = listRequestTraces(request, traceId)
		if err != nil {
			log.Printf("failed to get request traces for request %s, traceId %s, error: %v",
				request, traceId, err)
			requestsList[request] = &RequestTrace{
				TraceId: traceId,
			}
		}

		requestState, err := getRequestState(flowName, request)
		if err != nil {
			log.Printf("failed to get request state for %s, request %s, error: %v",
				flowName, request, err)
			requestState = "UNKNOWN"
		}
		requestsList[request].Status = requestState

		tracingEnabled = true
	}

	flowRequests := &FlowRequests{
		TracingEnabled: tracingEnabled,
		Flow:           flowName,
		Requests:       requestsList,
	}

	locationDepths := []*Location{
		&Location{
			Name: "Flow : " + flowName + "",
			Link: "/flow/info?flow-name=" + flowName,
		},
	}

	htmlObj := HtmlObject{
		PublicURL: publicUri,
		Functions: functions,

		LocationDepths: locationDepths,

		CurrentLocation: &Location{
			Name: "Requests",
			Link: "/flow/requests?flow-name=" + flowName,
		},

		Requests: flowRequests,

		InnerHtml: "requests",
	}

	err = gen.ExecuteTemplate(w, "index", htmlObj)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to generate requested page, error: %v", err), http.StatusInternalServerError)
	}
}

// flowRequestMonitorPageHandler handle tracing view
func flowRequestMonitorPageHandler(w http.ResponseWriter, r *http.Request) {
	log.Printf("Serving request for request monitor view")

	flowName := r.URL.Query().Get("flow-name")
	currentRequestID := r.URL.Query().Get("request")

	functions, err := listGoFLows()
	if err != nil {
		log.Printf("failed to get functions, error: %v", err)
		functions = make([]*Flow, 0)
	}

	tracingEnabled := false
	requests, err := listFlowRequests(flowName)
	if err != nil {
		log.Printf("failed to get requests, error: %v", err)
		requests = make(map[string]string)
	}

	requestsList := make(map[string]*RequestTrace)

	for request, traceId := range requests {
		requestsList[request], err = listRequestTraces(request, traceId)
		if err != nil {
			log.Printf("failed to get request traces for request %s, traceId %s, error: %v",
				request, traceId, err)
			requestsList[request] = &RequestTrace{
				TraceId: traceId,
			}
		}

		requestState, err := getRequestState(flowName, request)
		if err != nil {
			log.Printf("failed to get request state for %s, request %s, error: %v",
				flowName, request, err)
			requestState = "UNKNOWN"
		}
		requestsList[request].Status = requestState

		tracingEnabled = true
		if currentRequestID == "" {
			currentRequestID = request
		}
	}

	flowRequests := &FlowRequests{
		TracingEnabled:   tracingEnabled,
		Flow:             flowName,
		Requests:         requestsList,
		CurrentRequestID: currentRequestID,
	}

	locationDepths := []*Location{
		&Location{
			Name: "Flow : " + flowName + "",
			Link: "/flow/info?flow-name=" + flowName,
		},
		&Location{
			Name: "Requests",
			Link: "/flow/requests?flow-name=" + flowName,
		},
	}

	htmlObj := HtmlObject{
		PublicURL: publicUri,
		Functions: functions,

		LocationDepths: locationDepths,

		CurrentLocation: &Location{
			Name: "requests-choice",
		},

		Requests: flowRequests,

		Traces: requestsList[currentRequestID],

		InnerHtml: "request-monitor",
	}

	err = gen.ExecuteTemplate(w, "index", htmlObj)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to generate requested page, error: %v", err), http.StatusInternalServerError)
	}
}

// API

// listFlows handle api request to list flow function
func listFlowsHandler(w http.ResponseWriter, r *http.Request) {

	w.Header().Set("Content-Type", jsonType)
	functions, err := listGoFLows()
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to handle list request, error: %v", err), http.StatusInternalServerError)
		return
	}
	data, _ := json.MarshalIndent(functions, "", "    ")
	w.Write(data)
}

// flowDesc request handler for a flow function
func flowDescHandler(w http.ResponseWriter, r *http.Request) {

	if r.Body == nil {
		http.Error(w, "invalid request, no content", 500)
		return
	}

	var msg Message
	err := json.NewDecoder(r.Body).Decode(&msg)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	flowName := msg.FlowName

	functions, err := listGoFLows()
	if err != nil {
		log.Printf("failed to get functions, error: %v", err)
		functions = make([]*Flow, 0)
	}

	flowDesc, err := buildFlowDesc(functions, flowName)
	if err != nil {
		http.Error(w, "failed to handle request, "+err.Error(), 500)
		return
	}

	data, _ := json.MarshalIndent(flowDesc, "", "    ")
	w.Header().Set("Content-Type", jsonType)
	w.Write(data)
}

// listFlowRequestsApiHandler list the requests for a flow function
func listFlowRequestsHandler(w http.ResponseWriter, r *http.Request) {

	if r.Body == nil {
		http.Error(w, "invalid request, no content", 500)
		return
	}

	var msg Message
	err := json.NewDecoder(r.Body).Decode(&msg)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	flowFunction := msg.FlowName

	w.Header().Set("Content-Type", jsonType)
	requests, err := listFlowRequests(flowFunction)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to handle request, error: %v", err), http.StatusInternalServerError)
		return
	}
	data, _ := json.MarshalIndent(requests, "", "    ")
	w.Write(data)
	return
}

// requestTracesApiHandler request handler for traces of a request
func requestTracesHandler(w http.ResponseWriter, r *http.Request) {

	if r.Body == nil {
		http.Error(w, "invalid request, no content", 500)
		return
	}

	var msg Message
	err := json.NewDecoder(r.Body).Decode(&msg)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	traceID := msg.TraceID
	flowName := msg.FlowName
	requestId := msg.RequestID

	w.Header().Set("Content-Type", jsonType)
	trace, err := listRequestTraces(requestId, traceID)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to handle request, error: %v", err), http.StatusInternalServerError)
		return
	}

	state, err := getRequestState(flowName, requestId)
	if err != nil {
		log.Printf("failed to get request state for %s, request %s, error: %v",
			flowName, requestId, err)
		state = "UNKNOWN"
	}
	trace.Status = state

	data, _ := json.MarshalIndent(trace, "", "    ")
	w.Write(data)
	return
}

// executeRequestHandler request handler for traces of a request
func executeRequestHandler(w http.ResponseWriter, r *http.Request) {
	if r.Body == nil {
		http.Error(w, "invalid request, no content", 500)
		return
	}

	var msg Message
	err := json.NewDecoder(r.Body).Decode(&msg)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	flowName := msg.FlowName
	data := msg.Data

	requestId, err := executeFlow(flowName, []byte(data))
	if err != nil {
		log.Printf("failed to execute %s, error: %v",
			flowName, err)
		http.Error(w, fmt.Sprintf("failed to execute %s, error: %v", flowName, err),
			http.StatusInternalServerError)
	}

	w.Write([]byte(requestId))
	return
}

// pauseRequestHandler request handler for traces of a request
func pauseRequestHandler(w http.ResponseWriter, r *http.Request) {
	if r.Body == nil {
		http.Error(w, "invalid request, no content", 500)
		return
	}

	var msg Message
	err := json.NewDecoder(r.Body).Decode(&msg)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	flowName := msg.FlowName
	requestID := msg.RequestID

	err = pauseRequest(flowName, requestID)
	if err != nil {
		log.Printf("failed to pause request %s for %s, error: %v",
			requestID, flowName, err)
		http.Error(w, fmt.Sprintf("failed to pause request %s for %s, error: %v",
			requestID, flowName, err), http.StatusInternalServerError)
	}

	w.Write([]byte(""))
	return
}

// resumeRequestHandler request handler for traces of a request
func resumeRequestHandler(w http.ResponseWriter, r *http.Request) {
	if r.Body == nil {
		http.Error(w, "invalid request, no content", 500)
		return
	}

	var msg Message
	err := json.NewDecoder(r.Body).Decode(&msg)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	flowName := msg.FlowName
	requestID := msg.RequestID

	err = resumeRequest(flowName, requestID)
	if err != nil {
		log.Printf("failed to resume request %s for %s, error: %v",
			requestID, flowName, err)
		http.Error(w, fmt.Sprintf("failed to resume request %s for %s, error: %v",
			requestID, flowName, err), http.StatusInternalServerError)
	}

	w.Write([]byte(""))
	return
}

// stopRequestHandler request handler for traces of a request
func stopRequestHandler(w http.ResponseWriter, r *http.Request) {
	if r.Body == nil {
		http.Error(w, "invalid request, no content", 500)
		return
	}

	var msg Message
	err := json.NewDecoder(r.Body).Decode(&msg)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	flowName := msg.FlowName
	requestID := msg.RequestID

	err = stopRequest(flowName, requestID)
	if err != nil {
		log.Printf("failed to stop request %s for %s, error: %v",
			requestID, flowName, err)
		http.Error(w, fmt.Sprintf("failed to stop request %s for %s, error: %v",
			requestID, flowName, err), http.StatusInternalServerError)
	}

	w.Write([]byte(""))
	return
}
