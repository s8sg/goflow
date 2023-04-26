package runtime

import (
	"io"
	"net/http"
	"os"

	"github.com/s8sg/goflow/core/runtime/controller/handler"

	"github.com/s8sg/goflow/core/runtime"

	"github.com/gin-gonic/gin"
)

func router(runtime runtime.Runtime) http.Handler {
	gin.DisableConsoleColor()

	f, _ := os.Create("gin.log")
	gin.DefaultWriter = io.MultiWriter(f)

	router := gin.Default()
	router.POST("/:flowName", newRequestHandlerWrapper(runtime, handler.ExecuteFlowHandler))
	router.GET("/:flowName", newRequestHandlerWrapper(runtime, handler.ExecuteFlowHandler))
	return router
}
