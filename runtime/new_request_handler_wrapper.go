package runtime

import (
	"fmt"
	runtimeCommon "github.com/s8sg/goflow/runtime/common"
	"io/ioutil"
	"log"
	"net/http"

	runtimepkg "github.com/s8sg/goflow/core/runtime"

	"github.com/gin-gonic/gin"
	"github.com/s8sg/goflow/core/sdk/executor"
)

const (
	AsyncRequestHeader = "X-Async"
)

const (
	RequestTypeExecute = "REQUEST_TYPE_EXECUTE"
	RequestTypeStop    = "REQUEST_TYPE_STOP"
	RequestTypePause   = "REQUEST_TYPE_PAUSE"
	RequestTypeResume  = "REQUEST_TYPE_RESUME"
	RequestTypeList    = "REQUEST_TYPE_LIST"
)

func requestHandlerWrapper(requestType string, runtime *FlowRuntime, handler func(*runtimepkg.Response, *runtimepkg.Request, executor.Executor) error) func(*gin.Context) {
	fn := func(c *gin.Context) {
		id := c.Param("id")
		flowName := c.Param("flowName")

		switch requestType {
		case RequestTypeExecute:
			executeRequestHandler(c, id, flowName, runtime, handler)
		default:
			// TODO: Implement
			c.Writer.WriteHeader(http.StatusBadRequest)
			c.Writer.Write([]byte("Not implemented"))
		}
	}

	return fn
}

func executeRequestHandler(c *gin.Context, id string, flowName string, runtime *FlowRuntime, handler func(*runtimepkg.Response, *runtimepkg.Request, executor.Executor) error) {
	body, err := ioutil.ReadAll(c.Request.Body)
	if err != nil {
		runtimeCommon.HandleError(c.Writer, fmt.Sprintf("failed to execute request, "+err.Error()))
		return
	}

	reqParams := make(map[string][]string)
	for _, param := range c.Params {
		reqParams[param.Key] = []string{param.Value}
	}

	for key, values := range c.Request.URL.Query() {
		reqParams[key] = values
	}

	response := &runtimepkg.Response{}
	response.RequestID = id
	response.Header = make(map[string][]string)
	request := &runtimepkg.Request{
		Body:      body,
		Header:    c.Request.Header,
		FlowName:  flowName,
		RequestID: id,
		Query:     reqParams,
		RawQuery:  c.Request.URL.RawQuery,
	}

	ex, err := runtime.CreateExecutor(request)
	if err != nil {
		runtimeCommon.HandleError(c.Writer, fmt.Sprintf("failed to execute request, "+err.Error()))
		return
	}

	asyncRequest := request.GetHeader(AsyncRequestHeader)
	if asyncRequest == "true" {
		err = runtime.Execute(flowName, request)
		if err != nil {
			log.Printf("Failed to enqueue request, %v", err)
			c.Writer.WriteHeader(http.StatusInternalServerError)
			c.Writer.Write([]byte("Failed to enqueue request"))
		}
		c.Writer.WriteHeader(http.StatusOK)
		c.Writer.Write([]byte("Request queued"))
		return
	}

	err = handler(response, request, ex)
	if err != nil {
		runtimeCommon.HandleError(c.Writer, fmt.Sprintf("request failed to be processed, "+err.Error()))
		return
	}

	headers := c.Writer.Header()
	for key, values := range response.Header {
		headers[key] = values
	}

	c.Writer.WriteHeader(http.StatusOK)
	c.Writer.Write(response.Body)
}
