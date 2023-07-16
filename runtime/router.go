package runtime

import (
	"github.com/s8sg/goflow/core/runtime/controller"
	"io"
	"net/http"
	"os"

	"github.com/gin-gonic/gin"
)

const (
	FlowNameParamName  = "flowName"
	RequestIdParamName = "requestId"
)

func Router(fRuntime *FlowRuntime) http.Handler {
	gin.DisableConsoleColor()

	f, _ := os.Create("gin.log")
	gin.DefaultWriter = io.MultiWriter(f)

	router := gin.Default()
	// TODO: below two routes are kept to be backward compatible, and will be removed later
	router.POST(":"+FlowNameParamName, executeRequestHandler(fRuntime, controller.ExecuteFlowHandler))
	router.GET(":"+FlowNameParamName, executeRequestHandler(fRuntime, controller.ExecuteFlowHandler))
	// flow routes configuration
	router.POST("flow/:"+FlowNameParamName, executeRequestHandler(fRuntime, controller.ExecuteFlowHandler))
	router.GET("flow/:"+FlowNameParamName, executeRequestHandler(fRuntime, controller.ExecuteFlowHandler))
	router.POST("flow/:"+FlowNameParamName+"/request/stop:"+RequestIdParamName, stopRequestHandler(fRuntime))
	router.POST("flow/:"+FlowNameParamName+"/request/pause:"+RequestIdParamName, pauseRequestHandler(fRuntime))
	router.POST("flow/:"+FlowNameParamName+"/request/resume:"+RequestIdParamName, resumeRequestHandler(fRuntime))
	router.POST("flow/:"+FlowNameParamName+"/request/state:"+RequestIdParamName, requestStateHandler(fRuntime))
	router.POST("flow/:"+FlowNameParamName+"/request/list", requestListHandler(fRuntime))
	// TODO: Add health check endpoint

	return router
}
