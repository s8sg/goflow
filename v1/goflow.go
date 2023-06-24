package v1

import (
	"fmt"
	runtimePkg "github.com/s8sg/goflow/core/runtime"
	"github.com/s8sg/goflow/core/sdk"
	"github.com/s8sg/goflow/runtime"
	"time"
)

type FlowService struct {
	Port                    int
	RedisURL                string
	RedisPassword           string
	RequestAuthSharedSecret string
	RequestAuthEnabled      bool
	WorkerConcurrency       int
	RetryCount              int
	Flows                   map[string]runtime.FlowDefinitionHandler
	RequestReadTimeout      time.Duration
	RequestWriteTimeout     time.Duration
	OpenTraceUrl            string
	DataStore               sdk.DataStore
	Logger                  sdk.Logger
	EnableMonitoring        bool
	DebugEnabled            bool
	GarbageCollectionPeriod time.Duration
	runtime                 *runtime.FlowRuntime
}

type Request struct {
	Body      []byte
	RequestId string
	Query     map[string][]string
	Header    map[string][]string
}

const (
	DefaultTraceUrl                      = "localhost:5775"
	DefaultRedisUrl                      = "localhost:6379"
	DefaultWorkerConcurrency             = 2
	DefaultWebServerPort                 = 8080
	DefaultReadTimeoutSecond             = 120
	DefaultWriteTimeoutSecond            = 120
	DefaultGarbageCollectionPeriodMinute = 2
)

func (fs *FlowService) Execute(flowName string, req *Request) error {
	if flowName == "" {
		return fmt.Errorf("flowName must be provided to execute flow")
	}

	fs.ConfigureDefault()
	fs.runtime = &runtime.FlowRuntime{
		RedisURL:                fs.RedisURL,
		RedisPassword:           fs.RedisPassword,
		RequestAuthEnabled:      fs.RequestAuthEnabled,
		RequestAuthSharedSecret: fs.RequestAuthSharedSecret,
	}

	request := &runtimePkg.Request{
		Header:    req.Header,
		RequestID: req.RequestId,
		Body:      req.Body,
		Query:     req.Query,
	}

	err := fs.runtime.Execute(flowName, request)
	if err != nil {
		return fmt.Errorf("failed to execute request, %v", err)
	}

	return nil
}

func (fs *FlowService) Pause(flowName string, requestId string) error {
	if flowName == "" {
		return fmt.Errorf("flowName must be provided")
	}

	if requestId == "" {
		return fmt.Errorf("request Id must be provided")
	}

	fs.ConfigureDefault()
	fs.runtime = &runtime.FlowRuntime{
		RedisURL:                fs.RedisURL,
		RedisPassword:           fs.RedisPassword,
		RequestAuthEnabled:      fs.RequestAuthEnabled,
		RequestAuthSharedSecret: fs.RequestAuthSharedSecret,
	}

	request := &runtimePkg.Request{
		RequestID: requestId,
	}

	err := fs.runtime.Pause(flowName, request)
	if err != nil {
		return fmt.Errorf("failed to pause request, %v", err)
	}

	return nil
}

func (fs *FlowService) Resume(flowName string, requestId string) error {
	if flowName == "" {
		return fmt.Errorf("flowName must be provided")
	}

	if requestId == "" {
		return fmt.Errorf("request Id must be provided")
	}

	fs.ConfigureDefault()
	fs.runtime = &runtime.FlowRuntime{
		RedisURL:                fs.RedisURL,
		RedisPassword:           fs.RedisPassword,
		RequestAuthEnabled:      fs.RequestAuthEnabled,
		RequestAuthSharedSecret: fs.RequestAuthSharedSecret,
	}

	request := &runtimePkg.Request{
		RequestID: requestId,
	}

	err := fs.runtime.Resume(flowName, request)
	if err != nil {
		return fmt.Errorf("failed to resume request, %v", err)
	}

	return nil
}

func (fs *FlowService) Stop(flowName string, requestId string) error {
	if flowName == "" {
		return fmt.Errorf("flowName must be provided")
	}

	if requestId == "" {
		return fmt.Errorf("request Id must be provided")
	}

	fs.ConfigureDefault()
	fs.runtime = &runtime.FlowRuntime{
		RedisURL:                fs.RedisURL,
		RedisPassword:           fs.RedisPassword,
		RequestAuthEnabled:      fs.RequestAuthEnabled,
		RequestAuthSharedSecret: fs.RequestAuthSharedSecret,
	}

	request := &runtimePkg.Request{
		RequestID: requestId,
	}

	err := fs.runtime.Stop(flowName, request)
	if err != nil {
		return fmt.Errorf("failed to stop request, %v", err)
	}

	return nil
}

func (fs *FlowService) UpdateWorkflow(fName string, handler runtime.FlowDefinitionHandler) error {
	if fName == "" {
		return fmt.Errorf("flow-name must not be empty")
	}
	if handler == nil {
		return fmt.Errorf("handler must not be nil")
	}

	if fs.Flows == nil {
		return fmt.Errorf("no flows registered")
	}

	if fs.Flows == nil {
		return fmt.Errorf("flow %s registered", fName)
	}
	fs.Flows[fName] = handler
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
		Flows:                   fs.Flows,
		OpenTracingUrl:          fs.OpenTraceUrl,
		RedisURL:                fs.RedisURL,
		RedisPassword:           fs.RedisPassword,
		DataStore:               fs.DataStore,
		Logger:                  fs.Logger,
		ServerPort:              fs.Port,
		ReadTimeout:             fs.RequestReadTimeout,
		WriteTimeout:            fs.RequestWriteTimeout,
		Concurrency:             fs.WorkerConcurrency,
		RequestAuthSharedSecret: fs.RequestAuthSharedSecret,
		RequestAuthEnabled:      fs.RequestAuthEnabled,
		EnableMonitoring:        fs.EnableMonitoring,
		RetryQueueCount:         fs.RetryCount,
		DebugEnabled:            fs.DebugEnabled,
		GarbageCollectionPeriod: fs.GarbageCollectionPeriod,
	}
	errorChan := make(chan error)
	defer close(errorChan)
	if err := fs.initRuntime(); err != nil {
		return err
	}
	go fs.runtimeWorker(errorChan)
	go fs.queueWorker(errorChan)
	go fs.server(errorChan)
	err := <-errorChan
	return err
}

func (fs *FlowService) StartServer() error {
	fs.ConfigureDefault()
	fs.runtime = &runtime.FlowRuntime{
		Flows:                   fs.Flows,
		OpenTracingUrl:          fs.OpenTraceUrl,
		RedisURL:                fs.RedisURL,
		RedisPassword:           fs.RedisPassword,
		DataStore:               fs.DataStore,
		Logger:                  fs.Logger,
		ServerPort:              fs.Port,
		ReadTimeout:             fs.RequestReadTimeout,
		WriteTimeout:            fs.RequestWriteTimeout,
		RequestAuthSharedSecret: fs.RequestAuthSharedSecret,
		RequestAuthEnabled:      fs.RequestAuthEnabled,
		EnableMonitoring:        fs.EnableMonitoring,
		RetryQueueCount:         fs.RetryCount,
		DebugEnabled:            fs.DebugEnabled,
		GarbageCollectionPeriod: fs.GarbageCollectionPeriod,
	}
	errorChan := make(chan error)
	defer close(errorChan)
	if err := fs.initRuntime(); err != nil {
		return err
	}
	go fs.runtimeWorker(errorChan)
	go fs.server(errorChan)
	err := <-errorChan
	return fmt.Errorf("server has stopped, error: %v", err)
}

func (fs *FlowService) StartWorker() error {
	fs.ConfigureDefault()
	fs.runtime = &runtime.FlowRuntime{
		Flows:                   fs.Flows,
		OpenTracingUrl:          fs.OpenTraceUrl,
		RedisURL:                fs.RedisURL,
		RedisPassword:           fs.RedisPassword,
		DataStore:               fs.DataStore,
		Logger:                  fs.Logger,
		Concurrency:             fs.WorkerConcurrency,
		RequestAuthSharedSecret: fs.RequestAuthSharedSecret,
		RequestAuthEnabled:      fs.RequestAuthEnabled,
		EnableMonitoring:        fs.EnableMonitoring,
		RetryQueueCount:         fs.RetryCount,
		DebugEnabled:            fs.DebugEnabled,
		GarbageCollectionPeriod: fs.GarbageCollectionPeriod,
	}
	errorChan := make(chan error)
	defer close(errorChan)
	if err := fs.initRuntime(); err != nil {
		return err
	}
	go fs.runtimeWorker(errorChan)
	go fs.queueWorker(errorChan)
	err := <-errorChan
	return fmt.Errorf("worker has stopped, error: %v", err)
}

func (fs *FlowService) ConfigureDefault() {
	if fs.OpenTraceUrl == "" {
		fs.OpenTraceUrl = DefaultTraceUrl
	}
	if fs.RedisURL == "" {
		fs.RedisURL = DefaultRedisUrl
	}
	if fs.WorkerConcurrency == 0 {
		fs.WorkerConcurrency = DefaultWorkerConcurrency
	}
	if fs.Port == 0 {
		fs.Port = DefaultWebServerPort
	}
	if fs.RequestReadTimeout == 0 {
		fs.RequestReadTimeout = DefaultReadTimeoutSecond * time.Second
	}
	if fs.RequestWriteTimeout == 0 {
		fs.RequestWriteTimeout = DefaultWriteTimeoutSecond * time.Second
	}
	if fs.GarbageCollectionPeriod == 0 {
		fs.GarbageCollectionPeriod = DefaultGarbageCollectionPeriodMinute * time.Minute
	}
}

func (fs *FlowService) initRuntime() error {
	err := fs.runtime.Init()
	if err != nil {
		return err
	}
	return nil
}

func (fs *FlowService) runtimeWorker(errorChan chan error) {
	err := fs.runtime.StartRuntime()
	errorChan <- fmt.Errorf("runtime has stopped, error: %v", err)
}

func (fs *FlowService) queueWorker(errorChan chan error) {
	err := fs.runtime.StartQueueWorker(errorChan)
	errorChan <- fmt.Errorf("worker has stopped, error: %v", err)
}

func (fs *FlowService) server(errorChan chan error) {
	err := fs.runtime.StartServer()
	errorChan <- fmt.Errorf("server has stopped, error: %v", err)
}
