package runtime

import (
	"github.com/s8sg/goflow/core/runtime/controller"
	"io"
	"net/http"
	"os"

	"github.com/gin-gonic/gin"
)

func Router(fRuntime *FlowRuntime) http.Handler {
	gin.DisableConsoleColor()

	f, _ := os.Create("gin.log")
	gin.DefaultWriter = io.MultiWriter(f)

	router := gin.Default()
	router.POST("flow/:flowName", requestHandlerWrapper(RequestTypeExecute, fRuntime, controller.ExecuteFlowHandler))
	router.GET("flow/:flowName", requestHandlerWrapper(RequestTypeExecute, fRuntime, controller.ExecuteFlowHandler))
	router.POST("flow/:flowName/stop", requestHandlerWrapper(RequestTypeStop, fRuntime, controller.ExecuteFlowHandler))
	router.POST("flow/:flowName/pause", requestHandlerWrapper(RequestTypePause, fRuntime, controller.ExecuteFlowHandler))
	router.POST("flow/:flowName/resume", requestHandlerWrapper(RequestTypeResume, fRuntime, controller.ExecuteFlowHandler))
	router.GET("flow/list", requestHandlerWrapper(RequestTypeList, fRuntime, controller.ExecuteFlowHandler))
	return router
}
