package faasflow

import (
	"handler/runtime"
	"log"
	"sync"
	"time"
)

type FlowService struct {
	Port                int
	RedisURL            string
	WorkerConcurrency   int
	RequestReadTimeout  time.Duration
	RequestWriteTimeout time.Duration
	OpenTraceUrl        string

	runtime *runtime.FlowRuntime
}

func (fs *FlowService) Start(handler runtime.FlowDefinitionHandler) error {
	fs.ConfigureDefault()
	fs.runtime = &runtime.FlowRuntime{Handler: handler, OpenTracingUrl: fs.OpenTraceUrl, RedisURL: fs.RedisURL}
	var wg sync.WaitGroup
	wg.Add(1)
	go fs.queueWorker(&wg)
	fs.server()
	wg.Wait()
	return nil
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

func (fs *FlowService) queueWorker(wg *sync.WaitGroup) {
	defer wg.Done()
	log.Print(fs.runtime.StartQueueWorker("redis://" + fs.RedisURL + "/", fs.WorkerConcurrency).Error())
}

func (fs *FlowService) server() {
	log.Print(fs.runtime.StartServer(fs.Port, fs.RequestReadTimeout, fs.RequestWriteTimeout).Error())
}
