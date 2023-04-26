package runtime

import (
	"fmt"
	"io/ioutil"
	"net/http"

	runtimepkg "github.com/s8sg/goflow/core/runtime"

	"github.com/gin-gonic/gin"
	"github.com/s8sg/goflow/core/sdk/executor"
)

func newRequestHandlerWrapper(runtime runtimepkg.Runtime, handler func(*runtimepkg.Response, *runtimepkg.Request, executor.Executor) error) func(*gin.Context) {
	fn := func(c *gin.Context) {
		id := c.Param("id")
		flowName := c.Param("flowName")

		body, err := ioutil.ReadAll(c.Request.Body)
		if err != nil {
			handleError(c.Writer, fmt.Sprintf("failed to execute request, "+err.Error()))
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
			handleError(c.Writer, fmt.Sprintf("failed to execute request, "+err.Error()))
			return
		}

		err = handler(response, request, ex)
		if err != nil {
			handleError(c.Writer, fmt.Sprintf("request failed to be processed, "+err.Error()))
			return
		}

		headers := c.Writer.Header()
		for key, values := range response.Header {
			headers[key] = values
		}

		c.Writer.WriteHeader(http.StatusOK)
		c.Writer.Write(response.Body)
	}

	return fn
}
