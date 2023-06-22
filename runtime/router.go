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
	router.POST("/:flowName", newRequestHandlerWrapper(fRuntime, controller.ExecuteFlowHandler))
	router.GET("/:flowName", newRequestHandlerWrapper(fRuntime, controller.ExecuteFlowHandler))
	return router
}
