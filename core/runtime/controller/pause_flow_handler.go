package controller

import (
	"fmt"
	"github.com/s8sg/goflow/core/runtime"
	"github.com/s8sg/goflow/core/sdk/executor"
	"log"
)

func PauseFlowHandler(response *runtime.Response, request *runtime.Request, ex executor.Executor) error {
	log.Printf("Pausing request %s of flow %s\n", request.RequestID, request.FlowName)

	flowExecutor := executor.CreateFlowExecutor(ex, nil)
	err := flowExecutor.Pause(request.RequestID)
	if err != nil {
		return fmt.Errorf("failed to pause request %s, check if request is active", request.RequestID)
	}

	response.Body = []byte("Successfully paused request " + request.RequestID)

	return nil
}
