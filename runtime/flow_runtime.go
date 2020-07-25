package runtime

import (
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
	FlowName       string
	Handler        FlowDefinitionHandler
	OpenTracingUrl string
	RedisURL       string
	stateStore     sdk.StateStore
	DataStore      sdk.DataStore
	Logger         sdk.Logger
	Concurrency    int
	ServerPort     int
	ReadTimeout    time.Duration
	WriteTimeout   time.Duration

	eventHandler sdk.EventHandler
	settings     goworker.WorkerSettings
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
func (fRuntime *FlowRuntime) StartServer() error {

	err := fRuntime.Init()
	if err != nil {
		return err
	}

	s := &http.Server{
		Addr:           fmt.Sprintf(":%d", fRuntime.ServerPort),
		ReadTimeout:    fRuntime.ReadTimeout,
		WriteTimeout:   fRuntime.WriteTimeout,
		Handler:        router(fRuntime),
		MaxHeaderBytes: 1 << 20, // Max header of 1MB
	}

	fRuntime.settings = goworker.WorkerSettings{
		URI:            "redis://" + fRuntime.RedisURL + "/",
		Connections:    100,
		Queues:         []string{fRuntime.queueId()},
		UseNumber:      true,
		ExitOnComplete: false,
		Concurrency:    fRuntime.Concurrency,
		Namespace:      "resque:",
		Interval:       1.0,
	}
	goworker.SetSettings(fRuntime.settings)

	return s.ListenAndServe()
}

// StartQueueWorker starts listening for request in queue
func (fRuntime *FlowRuntime) StartQueueWorker() error {
	err := fRuntime.Init()
	if err != nil {
		return err
	}

	fRuntime.settings = goworker.WorkerSettings{
		URI:            "redis://" + fRuntime.RedisURL + "/",
		Connections:    100,
		Queues:         []string{fRuntime.queueId()},
		UseNumber:      true,
		ExitOnComplete: false,
		Concurrency:    fRuntime.Concurrency,
		Namespace:      "resque:",
		Interval:       1.0,
	}
	goworker.SetSettings(fRuntime.settings)
	goworker.Register("QueueWorker", fRuntime.queueReceiver)

	return goworker.Work()
}

func (fRuntime *FlowRuntime) EnqueueRequest(pr *runtime.Request) error {
	return goworker.Enqueue(&goworker.Job{
		Queue: fRuntime.queueId(),
		Payload: goworker.Payload{
			Class: "QueueWorker",
			Args:  []interface{}{pr.FlowName, pr.RequestID, string(pr.Body), pr.Header, pr.RawQuery, pr.Query},
		},
	})
}

func (fRuntime *FlowRuntime) queueReceiver(queue string, args ...interface{}) error {
	fRuntime.Logger.Log(fmt.Sprintf("Request received by worker at queue %v", queue))
	if queue != fRuntime.queueId() {
		fRuntime.Logger.Log(fmt.Sprintf("Request queue mismatch %s/%s", queue, fRuntime.queueId()))
		return nil
	}

	request := &runtime.Request{}

	if args[0] != nil {
		flowName, ok := args[0].(string)
		if !ok {
			fRuntime.Logger.Log(fmt.Sprintf("failed to load flowname from arguments %v", args[0]))
			return fmt.Errorf("failed to load flowname from arguments %v", args[0])
		}
		request.FlowName = flowName
	}

	if args[1] != nil {
		requestId, ok := args[1].(string)
		if !ok {
			fRuntime.Logger.Log(fmt.Sprintf("failed to load requestId from arguments %v", args[1]))
			return fmt.Errorf("failed to load requestId from arguments %v", args[1])
		}
		request.RequestID = requestId
	}

	if args[2] != nil {
		body, ok := args[2].(string)
		if !ok {
			fRuntime.Logger.Log(fmt.Sprintf("failed to load body from arguments %v", args[2]))
			return fmt.Errorf("failed to load body from arguments %v", args[2])
		}
		request.Body = []byte(body)
	}

	if args[3] != nil {
		header, ok := args[3].(map[string][]string)
		if !ok {
			fRuntime.Logger.Log(fmt.Sprintf("failed to load header from arguments %v", args[3]))
			return fmt.Errorf("failed to load header from arguments %v", args[3])
		}
		request.Header = header
	} else {
		request.Header = make(map[string][]string)
	}

	if args[4] != nil {
		rawQuery, ok := args[4].(string)
		if !ok {
			fRuntime.Logger.Log(fmt.Sprintf("failed to load raw-query from arguments %v", args[4]))
			return fmt.Errorf("failed to load raw-query from arguments %v", args[4])
		}
		request.RawQuery = rawQuery
	}

	if args[5] != nil {
		query, ok := args[5].(map[string][]string)
		if !ok {
			fRuntime.Logger.Log(fmt.Sprintf("failed to load query from arguments %v", args[5]))
			return fmt.Errorf("failed to load query from arguments %v", args[5])
		}
		request.Query = query
	} else {
		request.Query = make(map[string][]string)
	}

	executor, err := fRuntime.CreateExecutor(request)
	if err != nil {
		fRuntime.Logger.Log(fmt.Sprintf("[Request `%s`] failed to execute request, error: %v", request.RequestID, err))
		return fmt.Errorf("failed to execute request " + request.RequestID + ", error: " + err.Error())
	}
	response := &runtime.Response{}
	response.RequestID = request.RequestID
	response.Header = make(map[string][]string)

	err = handler.PartialExecuteFlowHandler(response, request, executor)
	if err != nil {
		fRuntime.Logger.Log(fmt.Sprintf("[Request `%s`] failed to be processed. error: %v", request.RequestID, err.Error()))
		return fmt.Errorf("request failed to be processed. error: " + err.Error())
	}

	return nil
}

func (fRuntime *FlowRuntime) queueId() string {
	return fmt.Sprintf("%s_%s", PartialRequestQueue, fRuntime.FlowName)
}
