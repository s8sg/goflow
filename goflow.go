package goflow

import (
	"fmt"
	"github.com/faasflow/goflow/runtime"
	"github.com/faasflow/sdk"
	"time"
)

type FlowService struct {
	Port                int
	RedisURL            string
	WorkerConcurrency   int
	RequestReadTimeout  time.Duration
	RequestWriteTimeout time.Duration
	OpenTraceUrl        string
	DataStore           sdk.DataStore
	Logger              sdk.Logger

	runtime *runtime.FlowRuntime
}

func (fs *FlowService) Start(handler runtime.FlowDefinitionHandler) error {
	fs.ConfigureDefault()
	fs.runtime = &runtime.FlowRuntime{
		Handler:        handler,
		OpenTracingUrl: fs.OpenTraceUrl,
		RedisURL:       fs.RedisURL,
		DataStore:      fs.DataStore,
		Logger:         fs.Logger,
	}
	errorChan := make(chan error)
	defer close(errorChan)
	go fs.queueWorker(errorChan)
	go fs.server(errorChan)
	err := <-errorChan
	return err
}

func (fs *FlowService) StartServer(handler runtime.FlowDefinitionHandler) error {
	fs.ConfigureDefault()
	fs.runtime = &runtime.FlowRuntime{
		Handler:        handler,
		OpenTracingUrl: fs.OpenTraceUrl,
		RedisURL:       fs.RedisURL,
		DataStore:      fs.DataStore,
		Logger:         fs.Logger,
	}
	err := fs.runtime.StartServer(fs.Port, fs.RequestReadTimeout, fs.RequestWriteTimeout)
	return fmt.Errorf("server has stopped, error: %v", err)
}

func (fs *FlowService) StartWorker(handler runtime.FlowDefinitionHandler) error {
	fs.ConfigureDefault()
	fs.runtime = &runtime.FlowRuntime{
		Handler:        handler,
		OpenTracingUrl: fs.OpenTraceUrl,
		RedisURL:       fs.RedisURL,
		DataStore:      fs.DataStore,
		Logger:         fs.Logger,
	}
	err := fs.runtime.StartQueueWorker("redis://"+fs.RedisURL+"/", fs.WorkerConcurrency)
	return fmt.Errorf("worker has stopped, error: %v", err)
}

func (fs *FlowService) ConfigureDefault() {
	if fs.OpenTraceUrl == "" {
		fs.OpenTraceUrl = "localhost:5775"
	}
	if fs.RedisURL == "" {
		fs.RedisURL = "localhost:6379"
	}
	if fs.WorkerConcurrency == 0 {
		fs.WorkerConcurrency = 2
	}
	if fs.Port == 0 {
		fs.Port = 8080
	}
	if fs.RequestReadTimeout == 0 {
		fs.RequestReadTimeout = 120 * time.Second
	}
	if fs.RequestWriteTimeout == 0 {
		fs.RequestWriteTimeout = 120 * time.Second
	}
}

func (fs *FlowService) queueWorker(errorChan chan error) {
	err := fs.runtime.StartQueueWorker("redis://"+fs.RedisURL+"/", fs.WorkerConcurrency)
	errorChan <- fmt.Errorf("worker has stopped, error: %v", err)
}

func (fs *FlowService) server(errorChan chan error) {
	err := fs.runtime.StartServer(fs.Port, fs.RequestReadTimeout, fs.RequestWriteTimeout)
	errorChan <- fmt.Errorf("server has stopped, error: %v", err)
}
