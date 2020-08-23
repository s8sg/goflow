package goflow

import (
	"fmt"
	runtimePkg "github.com/faasflow/runtime"
	"github.com/faasflow/sdk"
	"github.com/s8sg/goflow/runtime"
	"time"
)

type FlowService struct {
	Port                int
	RedisURL            string
	WorkerConcurrency   int
	Flows               map[string]runtime.FlowDefinitionHandler
	RequestReadTimeout  time.Duration
	RequestWriteTimeout time.Duration
	OpenTraceUrl        string
	DataStore           sdk.DataStore
	Logger              sdk.Logger

	runtime *runtime.FlowRuntime
}

type Request struct {
	Body   []byte
	Query  map[string][]string
	Header map[string][]string
}

func (fs *FlowService) Execute(flowName string, req *Request) error {
	if flowName == "" {
		return fmt.Errorf("flowName must be provided to execute flow")
	}

	fs.ConfigureDefault()
	fs.runtime = &runtime.FlowRuntime{
		RedisURL: fs.RedisURL,
	}

	request := &runtimePkg.Request{
		Header: req.Header,
		Body:   req.Body,
		Query:  req.Query,
	}

	err := fs.runtime.Execute(flowName, request)
	if err != nil {
		fmt.Errorf("failed to execute request, %v", err)
	}

	return nil
}

func (fs *FlowService) Register(flowName string, handler runtime.FlowDefinitionHandler) error {
	if flowName == "" {
		return fmt.Errorf("flow-name must not be empty")
	}
	if handler == nil {
		return fmt.Errorf("handler must not be nil")
	}

	if fs.Flows == nil {
		fs.Flows = make(map[string]runtime.FlowDefinitionHandler)
	}

	if fs.Flows[flowName] != nil {
		return fmt.Errorf("flow-name must be unique for each flow")
	}

	fs.Flows[flowName] = handler

	return nil
}

func (fs *FlowService) Start() error {
	if len(fs.Flows) == 0 {
		return fmt.Errorf("must register atleast one flow")
	}
	fs.ConfigureDefault()
	fs.runtime = &runtime.FlowRuntime{
		Flows:          fs.Flows,
		OpenTracingUrl: fs.OpenTraceUrl,
		RedisURL:       fs.RedisURL,
		DataStore:      fs.DataStore,
		Logger:         fs.Logger,
		ServerPort:     fs.Port,
		ReadTimeout:    fs.RequestReadTimeout,
		WriteTimeout:   fs.RequestWriteTimeout,
		Concurrency:    fs.WorkerConcurrency,
	}
	errorChan := make(chan error)
	defer close(errorChan)
	if err := fs.initRuntime(); err != nil {
		return err
	}
	go fs.queueWorker(errorChan)
	go fs.server(errorChan)
	err := <-errorChan
	return err
}

func (fs *FlowService) StartServer(flowName string, handler runtime.FlowDefinitionHandler) error {
	if flowName == "" {
		return fmt.Errorf("flow-name must not be empty and a unique for each flow")
	}
	fs.ConfigureDefault()
	fs.runtime = &runtime.FlowRuntime{
		Flows:          fs.Flows,
		OpenTracingUrl: fs.OpenTraceUrl,
		RedisURL:       fs.RedisURL,
		DataStore:      fs.DataStore,
		Logger:         fs.Logger,
		ServerPort:     fs.Port,
		ReadTimeout:    fs.RequestReadTimeout,
		WriteTimeout:   fs.RequestWriteTimeout,
	}
	if err := fs.initRuntime(); err != nil {
		return err
	}
	err := fs.runtime.StartServer()
	return fmt.Errorf("server has stopped, error: %v", err)
}

func (fs *FlowService) StartWorker(flowName string, handler runtime.FlowDefinitionHandler) error {
	if flowName == "" {
		return fmt.Errorf("flow-name must not be empty and a unique for each flow")
	}
	fs.ConfigureDefault()
	fs.runtime = &runtime.FlowRuntime{
		Flows:          fs.Flows,
		OpenTracingUrl: fs.OpenTraceUrl,
		RedisURL:       fs.RedisURL,
		DataStore:      fs.DataStore,
		Logger:         fs.Logger,
		Concurrency:    fs.WorkerConcurrency,
	}
	if err := fs.initRuntime(); err != nil {
		return err
	}
	err := fs.runtime.StartQueueWorker()
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

func (fs *FlowService) initRuntime() error {
	err := fs.runtime.Init()
	if err != nil {
		return err
	}
	fs.runtime.SetWorkerConfig()
	return nil
}

func (fs *FlowService) queueWorker(errorChan chan error) {
	err := fs.runtime.StartQueueWorker()
	errorChan <- fmt.Errorf("worker has stopped, error: %v", err)
}

func (fs *FlowService) server(errorChan chan error) {
	err := fs.runtime.StartServer()
	errorChan <- fmt.Errorf("server has stopped, error: %v", err)
}
