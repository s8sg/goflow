package controller

import (
	"fmt"
	"github.com/s8sg/goflow/core/runtime"
	"log"

	"github.com/s8sg/goflow/core/sdk/executor"
)

func FlowStateHandler(response *runtime.Response, request *runtime.Request, ex executor.Executor) error {
	log.Printf("Getting state of flow %s for request: %s\n", request.FlowName, request.RequestID)

	flowExecutor := executor.CreateFlowExecutor(ex, nil)
	state, err := flowExecutor.GetState(request.RequestID)
	if err != nil {
		log.Printf(err.Error())
		return fmt.Errorf("failed to get request state for %s, check if request is active", request.RequestID)
	}

	response.Body = []byte(state)
	return nil
}
