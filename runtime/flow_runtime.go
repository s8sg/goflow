package runtime

import (
	"encoding/json"
	"fmt"
	"github.com/benmanns/goworker"
	"github.com/faasflow/goflow/eventhandler"
	log2 "github.com/faasflow/goflow/log"
	"github.com/faasflow/runtime"
	"github.com/faasflow/runtime/controller/handler"
	sdk "github.com/faasflow/sdk"
	"github.com/faasflow/sdk/executor"
	"net/http"
	"time"
)

type FlowRuntime struct {
	Handler        FlowDefinitionHandler
	OpenTracingUrl string
	RedisURL       string
	stateStore     sdk.StateStore
	DataStore      sdk.DataStore
	Logger         sdk.Logger
	eventHandler   sdk.EventHandler
	settings       goworker.WorkerSettings
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

	if fRuntime.DataStore == nil {
		fRuntime.DataStore, err = initDataStore(fRuntime.RedisURL)
		if err != nil {
			return fmt.Errorf("Failed to initialize the StateStore, %v", err)
		}
	}

	if fRuntime.Logger == nil {
		fRuntime.Logger = &log2.StdErrLogger{}
	}

	fRuntime.eventHandler = &eventhandler.FaasEventHandler{
		TraceURI: fRuntime.OpenTracingUrl,
	}

	return nil
}

func (fRuntime *FlowRuntime) CreateExecutor(req *runtime.Request) (executor.Executor, error) {
	ex := &FlowExecutor{
		StateStore:   fRuntime.stateStore,
		DataStore:    fRuntime.DataStore,
		EventHandler: fRuntime.eventHandler,
		Handler:      fRuntime.Handler,
		Logger:       fRuntime.Logger,
		Runtime:      fRuntime,
	}
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
	fRuntime.settings = goworker.WorkerSettings{
		URI:            redisUri,
		Connections:    100,
		Queues:         []string{PartialRequestQueue},
		UseNumber:      true,
		ExitOnComplete: false,
		Concurrency:    concurrency,
		Namespace:      "resque:",
		Interval:       1.0,
	}
	goworker.SetSettings(fRuntime.settings)
	goworker.Register("QueueWorker", fRuntime.queueReceiver)
	return goworker.Work()
}

func (fRuntime *FlowRuntime) EnqueueRequest(pr *runtime.Request) error {
	encodedRequest, error := json.Marshal(pr)
	if error != nil {
		return fmt.Errorf("failed to marshal request while enqueing, error %v", error)
	}
	return goworker.Enqueue(&goworker.Job{
		Queue: PartialRequestQueue,
		Payload: goworker.Payload{
			Class: "QueueWorker",
			Args:  []interface{}{string(encodedRequest)},
		},
	})
}

func (fRuntime *FlowRuntime) queueReceiver(queue string, args ...interface{}) error {
	fRuntime.Logger.Log(fmt.Sprintf("Request received by worker at queue %v", queue))
	if queue != PartialRequestQueue {
		return nil
	}

	reqData, ok := args[0].(string)
	if !ok {
		fRuntime.Logger.Log(fmt.Sprintf("failed to load argument %v", reqData))
		return fmt.Errorf("failed to load argument %v", args[0])
	}

	request := &runtime.Request{}
	err := json.Unmarshal([]byte(reqData), request)
	if err != nil {
		fRuntime.Logger.Log(fmt.Sprintf("failed to unmarshal request, error %v", err))
		return fmt.Errorf("failed to unmarshal request, error %v", err)
	}

	executor, err := fRuntime.CreateExecutor(request)
	if err != nil {
		fRuntime.Logger.Log(fmt.Sprintf("[Request `%s`]  failed to execute request, error: %v", request.RequestID, err))
		return fmt.Errorf("failed to execute request " + request.RequestID + ", error: " + err.Error())
	}
	response := &runtime.Response{}
	response.RequestID = request.RequestID
	response.Header = make(map[string][]string)

	err = handler.PartialExecuteFlowHandler(response, request, executor)
	if err != nil {
		fRuntime.Logger.Log(fmt.Sprint("request failed to be processed. error: " + err.Error()))
		return fmt.Errorf("request failed to be processed. error: " + err.Error())
	}

	return nil
}
