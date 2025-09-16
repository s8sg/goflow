package runtime

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"

	"github.com/rs/xid"
	runtimeCommon "github.com/s8sg/goflow/runtime/common"

	runtimepkg "github.com/s8sg/goflow/core/runtime"

	"github.com/gin-gonic/gin"
	"github.com/s8sg/goflow/core/sdk/executor"
)

const (
	AsyncRequestHeader  = "X-Async"
	RequestIdHeaderName = "X-Request-Id"
)

func executeRequestHandler(runtime *FlowRuntime, handler func(*runtimepkg.Response, *runtimepkg.Request, executor.Executor) error) func(*gin.Context) {
	fn := func(c *gin.Context) {
		flowName := c.Param(FlowNameParamName)
		body, err := io.ReadAll(c.Request.Body)
		if err != nil {
			runtimeCommon.HandleError(c.Writer, fmt.Sprintf("failed to execute request, %v", err))
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
		response.Header = make(map[string][]string)
		request := &runtimepkg.Request{
			Body:      body,
			Header:    c.Request.Header,
			FlowName:  flowName,
			RequestID: c.Request.Header.Get(RequestIdHeaderName),
			Query:     reqParams,
			RawQuery:  c.Request.URL.RawQuery,
		}

		ex, err := runtime.CreateExecutor(request)
		if err != nil {
			runtimeCommon.HandleError(c.Writer, fmt.Sprintf("failed to execute request, %v", err))
			return
		}

		asyncRequest := request.GetHeader(AsyncRequestHeader)

		if strings.ToUpper(asyncRequest) == "TRUE" {

			// For async request we generate a requestID and pass it to the executor
			if request.RequestID == "" {
				request.RequestID = xid.New().String()
			}

			err = runtime.Execute(flowName, request)
			if err != nil {
				log.Printf("Failed to enqueue request, %v", err)
				runtimeCommon.HandleError(c.Writer, fmt.Sprintf("Failed to enqueue request, %v", err))
				return
			}

			headers := c.Writer.Header()
			headers[RequestIdHeaderName] = []string{request.RequestID}
			c.Writer.WriteHeader(http.StatusOK)
			if _, err := c.Writer.Write([]byte("Request queued")); err != nil {
				log.Printf("Error writing response: %v", err)
			}
			return
		}

		response.RequestID = request.RequestID
		err = handler(response, request, ex)
		if err != nil {
			runtimeCommon.HandleError(c.Writer, fmt.Sprintf("request failed to be processed, %v", err))
			return
		}

		headers := c.Writer.Header()
		for key, values := range response.Header {
			headers[key] = values
		}

		c.Writer.WriteHeader(http.StatusOK)
		if _, err := c.Writer.Write(response.Body); err != nil {
			log.Printf("Error writing response: %v", err)
		}
	}

	return fn
}

func stopRequestHandler(runtime *FlowRuntime) func(*gin.Context) {
	fn := func(c *gin.Context) {
		flowName := c.Param(FlowNameParamName)
		requestId := c.Param(RequestIdParamName)

		request := &runtimepkg.Request{
			Body:      []byte(""),
			Header:    c.Request.Header,
			FlowName:  flowName,
			RequestID: requestId,
			Query:     make(map[string][]string),
			RawQuery:  c.Request.URL.RawQuery,
		}

		err := runtime.Stop(flowName, request)
		if err != nil {
			log.Printf("Failed to submit stop request for requestId %s, error %v", requestId, err)
			runtimeCommon.HandleError(c.Writer, fmt.Sprintf("Failed to submit stop requests, %v", err))
			return
		}
		c.Writer.WriteHeader(http.StatusOK)
		if _, err := c.Writer.Write([]byte("Stop request submitted")); err != nil {
			log.Printf("Error writing response: %v", err)
		}
		// no explicit return needed
	}
	return fn
}

func pauseRequestHandler(runtime *FlowRuntime) func(*gin.Context) {
	fn := func(c *gin.Context) {
		flowName := c.Param(FlowNameParamName)
		requestId := c.Param(RequestIdParamName)

		request := &runtimepkg.Request{
			Body:      []byte(""),
			Header:    c.Request.Header,
			FlowName:  flowName,
			RequestID: requestId,
			Query:     make(map[string][]string),
			RawQuery:  c.Request.URL.RawQuery,
		}

		err := runtime.Pause(flowName, request)
		if err != nil {
			log.Printf("Failed to submit pause request for requestId %s, error %v", requestId, err)
			runtimeCommon.HandleError(c.Writer, fmt.Sprintf("Failed to submit pause requests, %v", err))
			// ...existing code...
		}
		c.Writer.WriteHeader(http.StatusOK)
		if _, err := c.Writer.Write([]byte("Pause request submitted")); err != nil {
			log.Printf("Error writing response: %v", err)
		}
		// no explicit return needed
	}
	return fn
}

func resumeRequestHandler(runtime *FlowRuntime) func(*gin.Context) {
	fn := func(c *gin.Context) {
		flowName := c.Param(FlowNameParamName)
		requestId := c.Param(RequestIdParamName)

		request := &runtimepkg.Request{
			Body:      []byte(""),
			Header:    c.Request.Header,
			FlowName:  flowName,
			RequestID: requestId,
			Query:     make(map[string][]string),
			RawQuery:  c.Request.URL.RawQuery,
		}

		err := runtime.Resume(flowName, request)
		if err != nil {
			log.Printf("Failed to submit resume request for requestId %s, error %v", requestId, err)
			runtimeCommon.HandleError(c.Writer, fmt.Sprintf("Failed to submit resume requests, %v", err))
			// ...existing code...
		}
		c.Writer.WriteHeader(http.StatusOK)
		if _, err := c.Writer.Write([]byte("Resume request submitted")); err != nil {
			log.Printf("Error writing response: %v", err)
		}
		// no explicit return needed
	}
	return fn
}

func requestStateHandler(runtime *FlowRuntime) func(*gin.Context) {
	fn := func(c *gin.Context) {
		// flowName := c.Param(FlowNameParamName)
		// requestId := c.Param(RequestIdParamName)
		// TODO: implement
		c.Writer.WriteHeader(http.StatusInternalServerError)
		if _, err := c.Writer.Write([]byte("Not Implemented")); err != nil {
			log.Printf("Error writing response: %v", err)
		}
		// no explicit return needed
	}
	return fn
}

func requestListHandler(runtime *FlowRuntime) func(*gin.Context) {
	fn := func(c *gin.Context) {
		// flowName := c.Param(FlowNameParamName)
		// requestId := c.Param(RequestIdParamName)
		// TODO: implement
		c.Writer.WriteHeader(http.StatusInternalServerError)
		if _, err := c.Writer.Write([]byte("Not Implemented")); err != nil {
			log.Printf("Error writing response: %v", err)
		}
		// no explicit return needed
	}
	return fn
}
