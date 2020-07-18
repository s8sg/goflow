package runtime

import (
	"encoding/json"
	"fmt"
	"github.com/benmanns/goworker"
	"github.com/faasflow/runtime"
	"github.com/faasflow/runtime/controller/handler"
	sdk "github.com/faasflow/sdk"
	"github.com/faasflow/sdk/executor"
	"github.com/faasflow/faas-flow-service/eventhandler"
	"log"
	"net/http"
	"time"
)

type FlowRuntime struct {
	Handler        FlowDefinitionHandler
	OpenTracingUrl string
	RedisURL       string
	stateStore     sdk.StateStore
	dataStore      sdk.DataStore
	eventHandler   sdk.EventHandler
}

const (
	PartialRequestQueue = "faasflow-partial-request"
)

func (fRuntime *FlowRuntime) Init() error {
	var err error

	fRuntime.stateStore, err = initStateStore(fRuntime.RedisURL)
	if err != nil {
		return fmt.Errorf("Failed to initialize the StateStore, %v", err)
	}

	fRuntime.dataStore, err = initDataStore(fRuntime.RedisURL)
	if err != nil {
		return fmt.Errorf("Failed to initialize the StateStore, %v", err)
	}

	fRuntime.eventHandler = &eventhandler.FaasEventHandler{
		TraceURI: fRuntime.OpenTracingUrl,
	}

	return nil
}

func (fRuntime *FlowRuntime) CreateExecutor(req *runtime.Request) (executor.Executor, error) {
	ex := &FlowExecutor{StateStore: fRuntime.stateStore, DataStore: fRuntime.dataStore, EventHandler: fRuntime.eventHandler, Handler: fRuntime.Handler}
	error := ex.Init(req)
	return ex, error
}

// StartServer starts listening for new request
func (fRuntime *FlowRuntime) StartServer(port int, readTimeout time.Duration, writeTimeout time.Duration) error {

	err := fRuntime.Init()
	if err != nil {
		return err
	}

	s := &http.Server{
		Addr:           fmt.Sprintf(":%d", port),
		ReadTimeout:    readTimeout,
		WriteTimeout:   writeTimeout,
		Handler:        router(fRuntime),
		MaxHeaderBytes: 1 << 20, // Max header of 1MB
	}

	return s.ListenAndServe()
}

// StartQueueWorker starts listening for request in queue
func (fRuntime *FlowRuntime) StartQueueWorker(redisUri string, concurrency int) error {
	settings := goworker.WorkerSettings{
		URI:            redisUri,
		Connections:    100,
		Queues:         []string{PartialRequestQueue},
		UseNumber:      true,
		ExitOnComplete: false,
		Concurrency:    concurrency,
		Namespace:      "resque:",
		Interval:       1.0,
	}
	goworker.SetSettings(settings)
	goworker.Register("QueueWorker", fRuntime.queueReceiver)
	return goworker.Work()
}

func EnqueueRequest(pr *runtime.Request) error {
	encodedRequest, error := json.Marshal(pr)
	if error != nil {
		return fmt.Errorf("failed to marshal request while enqueing, error %v", error)
	}

	return goworker.Enqueue(&goworker.Job{
		Queue: PartialRequestQueue,
		Payload: goworker.Payload{
			Class: "QueueWorker",
			Args:  []interface{}{encodedRequest},
		},
	})
}

func (fRuntime *FlowRuntime) queueReceiver(queue string, args ...interface{}) error {
	if queue != "partial-request" {
		return nil
	}

	reqData, ok := args[0].([]byte)
	if !ok {
		return fmt.Errorf("failed to conver args to []byte, argument %v", args[0])
	}

	request := &runtime.Request{}
	err := json.Unmarshal(reqData, request)
	if err != nil {
		return fmt.Errorf("failed to unmarshal request, error %v", err)
	}

	executor, err := fRuntime.CreateExecutor(request)
	if err != nil {
		log.Print("failed to execute request " + request.RequestID + ", error: " + err.Error())
		return fmt.Errorf("failed to execute request " + request.RequestID + ", error: " + err.Error())
	}
	response := &runtime.Response{}
	response.RequestID = request.RequestID
	response.Header = make(map[string][]string)

	err = handler.PartialExecuteFlowHandler(response, request, executor)
	if err != nil {
		return fmt.Errorf("request failed to be processed. error: " + err.Error())
	}

	return nil
}
