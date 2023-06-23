package controller

import (
	"fmt"
	"github.com/s8sg/goflow/core/runtime"
	"log"

	"github.com/s8sg/goflow/core/sdk/executor"
)

const (
	CallbackUrlHeader   = "X-Callback-Url"
	RequestIdHeader     = "X-Reqid"
	AuthSignatureHeader = "X-Hub-Signature"
)

func ExecuteFlowHandler(response *runtime.Response, request *runtime.Request, ex executor.Executor) error {
	log.Printf("Executing flow %s\n", request.FlowName)

	var stateOption executor.ExecutionStateOption

	callbackURL := request.GetHeader(CallbackUrlHeader)
	rawRequest := &executor.RawRequest{}
	rawRequest.Data = request.Body
	rawRequest.Query = request.RawQuery
	rawRequest.AuthSignature = request.GetHeader(AuthSignatureHeader)
	if request.RequestID != "" {
		rawRequest.RequestId = request.RequestID
	}
	stateOption = executor.NewRequest(rawRequest)

	flowExecutor := executor.CreateFlowExecutor(ex, nil)
	resp, err := flowExecutor.Execute(stateOption)
	if err != nil {
		return fmt.Errorf("failed to execute request. %s", err.Error())
	}

	response.RequestID = flowExecutor.GetReqId()
	response.SetHeader(RequestIdHeader, response.RequestID)
	response.SetHeader(CallbackUrlHeader, callbackURL)
	response.Body = resp

	return nil
}
