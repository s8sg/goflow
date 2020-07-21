package runtime

import (
	"fmt"
	"io/ioutil"
	"net/http"

	runtimepkg "github.com/faasflow/runtime"

	"github.com/faasflow/sdk/executor"
	"github.com/julienschmidt/httprouter"
)

func newRequestHandlerWrapper(runtime runtimepkg.Runtime, handler func(*runtimepkg.Response, *runtimepkg.Request, executor.Executor) error) func(http.ResponseWriter, *http.Request, httprouter.Params) {
	return func(w http.ResponseWriter, req *http.Request, params httprouter.Params) {
		id := params.ByName("id")

		body, err := ioutil.ReadAll(req.Body)
		if err != nil {
			handleError(w, fmt.Sprintf("failed to execute request "+id+" "+err.Error()))
			return
		}

		reqParams := make(map[string][]string)
		for _, param := range params {
			reqParams[param.Key] = []string{param.Value}
		}

		for key, values := range req.URL.Query() {
			reqParams[key] = values
		}

		response := &runtimepkg.Response{}
		response.RequestID = id
		response.Header = make(map[string][]string)
		request := &runtimepkg.Request{
			Body:      body,
			Header:    req.Header,
			FlowName:  getFlowName(runtime),
			RequestID: id,
			Query:     reqParams,
			RawQuery:  req.URL.RawQuery,
		}

		ex, err := runtime.CreateExecutor(request)
		if err != nil {
			handleError(w, fmt.Sprintf("failed to execute request "+id+", error: "+err.Error()))
			return
		}

		err = handler(response, request, ex)
		if err != nil {
			handleError(w, fmt.Sprintf("request failed to be processed. error: "+err.Error()))
			return
		}

		headers := w.Header()
		for key, values := range response.Header {
			headers[key] = values
		}

		w.WriteHeader(http.StatusOK)
		w.Write(response.Body)
	}
}

func getFlowName(runtime runtimepkg.Runtime) string {
	fr, ok := runtime.(*FlowRuntime)
	if !ok {
		return ""
	}
	return fr.FlowName
}