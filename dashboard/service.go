package main

import (
	"fmt"
	"github.com/rs/xid"
	lib2 "github.com/s8sg/goflow/dashboard/lib"
	goflow "github.com/s8sg/goflow/v1"
	redis "gopkg.in/redis.v5"
	"os"
	"strings"
)

var rdb *redis.Client

// listGoFLows get list of go-flows
func listGoFLows() ([]*Flow, error) {
	rdb = getRDB()
	command := rdb.Keys("goflow-flow:*")
	rdb.Process(command)
	flowKeys, err := command.Result()
	if err != nil {
		return nil, nil
	}
	flows := make([]*Flow, 0)
	for _, key := range flowKeys {
		flowName := strings.Split(key, ":")[1]
		if flowName == "" {
			continue
		}
		flow := &Flow{
			Name: flowName,
		}
		flows = append(flows, flow)
	}
	return flows, nil
}

// getDot request to dot-generator for the dag dot graph
func getDot(flowName string) (string, error) {
	rdb = getRDB()
	command := rdb.Get("goflow-flow:" + flowName)
	rdb.Process(command)
	definition, err := command.Result()
	if err != nil {
		return "", nil
	}
	dot, err := lib2.MakeDotFromDefinitionString(definition)
	return dot, err
}

// listFlowRequests get list of request for a goflow
func listFlowRequests(flow string) (map[string]string, error) {
	return lib2.ListRequests(flow)
}

// buildFlowDesc get a flow details
func buildFlowDesc(functions []*Flow, flowName string) (*FlowDesc, error) {

	var functionObj *Flow
	for _, functionObj = range functions {
		if functionObj.Name == flowName {
			break
		}
	}

	dot, dErr := getDot(flowName)
	if dErr != nil {
		return nil, fmt.Errorf("failed to get dot, %v", dErr)
	}

	flowDesc := &FlowDesc{
		Name: functionObj.Name,
		Dot:  dot,
	}

	return flowDesc, nil
}

// listRequestTraces get list of traces for a request traceID
func listRequestTraces(requestId string, requestTraceId string) (*RequestTrace, error) {
	requestTraceResponse, err := lib2.ListTraces(requestTraceId)
	if err != nil {
		return nil, err
	}
	requestTrace := &RequestTrace{
		RequestID:  requestId,
		TraceId:    requestTraceId,
		StartTime:  requestTraceResponse.StartTime,
		NodeTraces: make(map[string]*NodeTrace, 0),
		Duration:   requestTraceResponse.Duration,
	}
	for id, nodeTrace := range requestTraceResponse.NodeTraces {
		nodeTraceObj := &NodeTrace{
			StartTime: nodeTrace.StartTime,
			Duration:  nodeTrace.Duration,
		}
		requestTrace.NodeTraces[id] = nodeTraceObj
	}

	return requestTrace, nil
}

// getRequestState request the flow for the request status
func getRequestState(flow, requestId string) (string, error) {
	rdb = getRDB()
	return "", nil
}

// executeFlow execute a flow
func executeFlow(flow string, data []byte) (string, error) {
	fs := &goflow.FlowService{
		RedisURL: getRedisAddr(),
	}
	requestId := getNewId()
	request := &goflow.Request{
		Body:      data,
		RequestId: requestId,
	}

	err := fs.Execute(flow, request)
	if err != nil {
		return "", err
	}

	return requestId, nil
}

// pauseRequest pause a request
func pauseRequest(flow string, requestID string) error {
	fs := &goflow.FlowService{
		RedisURL: getRedisAddr(),
	}

	err := fs.Pause(flow, requestID)
	if err != nil {
		return err
	}

	return nil
}

// resumeRequest resumes a request
func resumeRequest(flow string, requestID string) error {
	fs := &goflow.FlowService{
		RedisURL: getRedisAddr(),
	}

	err := fs.Resume(flow, requestID)
	if err != nil {
		return err
	}

	return nil
}

// stopRequest stops a request
func stopRequest(flow string, requestID string) error {
	fs := &goflow.FlowService{
		RedisURL: getRedisAddr(),
	}

	err := fs.Stop(flow, requestID)
	if err != nil {
		return err
	}

	return nil
}

func getRDB() *redis.Client {
	addr := getRedisAddr()
	if rdb == nil {
		rdb = redis.NewClient(&redis.Options{
			Addr: addr,
			DB:   0,
		})
	}
	return rdb
}

func getRedisAddr() string {
	addr := os.Getenv("REDIS_URL")
	if addr == "" {
		addr = "localhost:6379"
	}
	return addr
}

func getNewId() string {
	guid := xid.New()
	return guid.String()
}
