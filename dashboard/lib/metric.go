package lib

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
)

// Objects to retrieve specific trace details

type SpanItem struct {
	TraceID       string `json:"traceID"`
	SpanID        string `json:"spanID"`
	OperationName string `json:"operationName"`
	StartTime     int    `json:"startTime"`
	Duration      int    `json:"duration"`
	// Other can be added based on the needs
}

type TraceItem struct {
	TraceID string      `json:"traceID"`
	Spans   []*SpanItem `json:"spans"`
}

type Traces struct {
	Data []*TraceItem `json:"data"`
}

// Objects to retrieve requests lists

type SpanOps struct {
	TraceID       string `json:"traceID"`
	SpanID        string `json:"spanID"`
	OperationName string `json:"operationName"`
}

type RequestItem struct {
	TraceID string     `json:"traceID"`
	Spans   []*SpanOps `json:"spans"`
}

type Requests struct {
	Data []*RequestItem `json:"data"`
}

// traces of each nodes in a dag
type NodeTrace struct {
	StartTime int `json:"start-time"`
	Duration  int `json:"duration"`
	// Other can be added based on the needs
}

// RequestTrace object to response traces details
type RequestTrace struct {
	RequestID  string                `json:"request-id"`
	NodeTraces map[string]*NodeTrace `json:"traces"`
	StartTime  int                   `json:"start-time"`
	Duration   int                   `json:"duration"`
}

var (
	trace_url = "http://localhost:16686/"
)

func ListRequests(function string) (map[string]string, error) {
	resp, err := http.Get(getTraceUrl() + "api/traces?service=" + function)
	if err != nil {
		return nil, fmt.Errorf("failed to request trace service, error %v ", err)
	}
	defer resp.Body.Close()
	if resp.Body == nil {
		return nil, fmt.Errorf("failed to request trace service, status code %d", resp.StatusCode)
	}

	bodyBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read trace result, read error %v", err)
	}

	if len(bodyBytes) == 0 {
		return nil, fmt.Errorf("failed to get request traces, empty result")
	}

	requests := &Requests{}
	err = json.Unmarshal(bodyBytes, requests)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal requests lists, error %v", err)
	}

	requestMap := make(map[string]string)
	for _, request := range requests.Data {
		if request.Spans == nil {
			continue
		}
		for _, span := range request.Spans {
			if span.TraceID == request.TraceID && span.TraceID == span.SpanID {
				requestMap[span.OperationName] = request.TraceID
				break
			}
		}
	}

	return requestMap, nil
}

func ListTraces(request string) (*RequestTrace, error) {
	resp, err := http.Get(getTraceUrl() + "api/traces/" + request)
	if err != nil {
		return nil, fmt.Errorf("failed to request trace service, error %v ", err)
	}
	defer resp.Body.Close()
	if resp.Body == nil {
		return nil, fmt.Errorf("failed to request trace service, status code %d", resp.StatusCode)
	}

	bodyBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read trace result, read error %v", err)
	}

	if len(bodyBytes) == 0 {
		return nil, fmt.Errorf("failed to get request traces, empty result")
	}

	traces := &Traces{}
	err = json.Unmarshal(bodyBytes, traces)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal requests lists, error %v", err)
	}

	if traces.Data == nil || len(traces.Data) == 0 {
		return nil, fmt.Errorf("failed to get request traces, empty data")
	}

	requestTrace := traces.Data[0]
	if requestTrace.TraceID != request {
		return nil, fmt.Errorf("invalid request trace %s", requestTrace.TraceID)
	}

	requestTraces := &RequestTrace{}
	requestTraces.NodeTraces = make(map[string]*NodeTrace)

	var lastSpanEnd int

	for _, span := range requestTrace.Spans {
		if span.TraceID == request && span.TraceID == span.SpanID {
			// Set RequestID, StartTime and lastestSpan start time
			requestTraces.RequestID = span.OperationName
			requestTraces.StartTime = span.StartTime
			requestTraces.Duration = span.Duration
			lastSpanEnd = span.StartTime
		} else {
			spanEndTime := span.StartTime + span.Duration
			if spanEndTime > lastSpanEnd {
				lastSpanEnd = spanEndTime
			}

			node, found := requestTraces.NodeTraces[span.OperationName]
			if found {
				nodeStartTime := node.StartTime
				nodeDuration := node.Duration
				nodeEndTime := nodeStartTime + nodeDuration
				if span.StartTime < nodeStartTime {
					nodeStartTime = span.StartTime
				}
				if spanEndTime > nodeEndTime {
					nodeDuration = spanEndTime - nodeStartTime
				}
				node.StartTime = nodeStartTime
				node.Duration = nodeDuration
			} else {
				node = &NodeTrace{}
				node.StartTime = span.StartTime
				node.Duration = span.Duration
			}
			requestTraces.NodeTraces[span.OperationName] = node
		}
	}
	if lastSpanEnd > requestTraces.StartTime {
		requestTraces.Duration = lastSpanEnd - requestTraces.StartTime
	}

	return requestTraces, nil
}

func getTraceUrl() string {
	url := os.Getenv("TRACE_URL")
	if url != "" {
		trace_url = url
	}
	return trace_url
}
