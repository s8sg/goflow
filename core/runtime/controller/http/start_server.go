package http

import (
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/s8sg/goflow/core/runtime"
)

// StartServer starts the flow function
func StartServer(runtime runtime.Runtime, port int, readTimeout time.Duration, writeTimeout time.Duration) error {

	err := runtime.Init()
	if err != nil {
		log.Fatal(err)
	}

	s := &http.Server{
		Addr:           fmt.Sprintf(":%d", port),
		ReadTimeout:    readTimeout,
		WriteTimeout:   writeTimeout,
		Handler:        router(runtime),
		MaxHeaderBytes: 1 << 20, // Max header of 1MB
	}

	return s.ListenAndServe()
}
