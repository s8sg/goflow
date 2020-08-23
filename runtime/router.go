package runtime

import (
	"github.com/faasflow/runtime/controller/handler"
	"net/http"

	"github.com/faasflow/runtime"

	"github.com/julienschmidt/httprouter"
)

func router(runtime runtime.Runtime) http.Handler {
	router := httprouter.New()
	/*
		router.POST("/flow/:id/pause", newRequestHandlerWrapper(runtime, handler.PauseFlowHandler))
		router.POST("/flow/:id/resume", newRequestHandlerWrapper(runtime, handler.ResumeFlowHandler))
		router.POST("/flow/:id/stop", newRequestHandlerWrapper(runtime, handler.StopFlowHandler))
		router.GET("/flow/:id/state", newRequestHandlerWrapper(runtime, handler.FlowStateHandler))
		router.GET("/dag/export", newRequestHandlerWrapper(runtime, handler.GetDagHandler))
	*/
	router.POST("/:flowName", newRequestHandlerWrapper(runtime, handler.ExecuteFlowHandler))
	router.GET("/:flowName", newRequestHandlerWrapper(runtime, handler.ExecuteFlowHandler))
	return router
}
