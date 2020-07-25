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
	PartialRequestQueue = "goflow-partial-request"
	NewRequestQueue     = "goflow-request"
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

func (fRuntime *FlowRuntime) Execute(request *runtime.Request) error {
	fRuntime.settings = goworker.WorkerSettings{
		URI:            "redis://" + fRuntime.RedisURL + "/",
		Connections:    100,
		Queues:         []string{fRuntime.newRequestQueueId()},
		UseNumber:      true,
		ExitOnComplete: false,
		Namespace:      "resque:",
		Interval:       1.0,
	}
	goworker.SetSettings(fRuntime.settings)
	return goworker.Enqueue(&goworker.Job{
		Queue: fRuntime.newRequestQueueId(),
		Payload: goworker.Payload{
			Class: "GoFlow",
			Args:  []interface{}{request.RequestID, string(request.Body), request.Header, request.RawQuery, request.Query},
		},
	})
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
		Queues:         []string{fRuntime.partialRequestQueueId()},
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
		Queues:         []string{fRuntime.requestQueueId(), fRuntime.partialRequestQueueId(), fRuntime.newRequestQueueId()},
		UseNumber:      true,
		ExitOnComplete: false,
		Concurrency:    fRuntime.Concurrency,
		Namespace:      "resque:",
		Interval:       1.0,
	}
	goworker.SetSettings(fRuntime.settings)
	goworker.Register("GoFlow", fRuntime.queueReceiver)

	return goworker.Work()
}

func (fRuntime *FlowRuntime) EnqueuePartialRequest(pr *runtime.Request) error {
	return goworker.Enqueue(&goworker.Job{
		Queue: fRuntime.partialRequestQueueId(),
		Payload: goworker.Payload{
			Class: "GoFlow",
			Args:  []interface{}{pr.RequestID, string(pr.Body), pr.Header, pr.RawQuery, pr.Query},
		},
	})
}

func (fRuntime *FlowRuntime) queueReceiver(queue string, args ...interface{}) error {
	fRuntime.Logger.Log(fmt.Sprintf("Request received by worker at queue %v", queue))
	var err error

	switch queue {
	case fRuntime.partialRequestQueueId():
		request, err := makeRequestFromArgs(args...)
		if err != nil {
			fRuntime.Logger.Log(err.Error())
			return err
		}
		request.FlowName = fRuntime.FlowName
		err = fRuntime.handlePartialRequest(request)
	case fRuntime.newRequestQueueId():
		request, err := makeRequestFromArgs(args...)
		if err != nil {
			fRuntime.Logger.Log(err.Error())
			return err
		}
		request.FlowName = fRuntime.FlowName
		err = fRuntime.handleNewRequest(request)
	case fRuntime.requestQueueId():
		request := &runtime.Request{}
		body, ok := args[0].(string)
		if !ok {
			fRuntime.Logger.Log(fmt.Sprintf("failed to load request body as string from %v", args[0]))
			return fmt.Errorf("failed to load request body as string from %v", args[0])
		}
		request.Body = []byte(body)
		request.FlowName = fRuntime.FlowName
		err = fRuntime.handleNewRequest(request)
	default:
		fRuntime.Logger.Log(fmt.Sprintf("Request queue mismatch %s/%s", queue, fRuntime.partialRequestQueueId()))
		return nil
	}

	return err
}

func (fRuntime *FlowRuntime) handleNewRequest(request *runtime.Request) error {
	executor, err := fRuntime.CreateExecutor(request)
	if err != nil {
		return fmt.Errorf("failed to execute request " + request.RequestID + ", error: " + err.Error())
	}

	response := &runtime.Response{}
	response.RequestID = request.RequestID
	response.Header = make(map[string][]string)

	err = handler.ExecuteFlowHandler(response, request, executor)
	if err != nil {
		return fmt.Errorf("equest failed to be processed. error: " + err.Error())
	}

	return nil
}

func (fRuntime *FlowRuntime) handlePartialRequest(request *runtime.Request) error {
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

func (fRuntime *FlowRuntime) partialRequestQueueId() string {
	return fmt.Sprintf("%s_%s", PartialRequestQueue, fRuntime.FlowName)
}

func (fRuntime *FlowRuntime) newRequestQueueId() string {
	return fmt.Sprintf("%s_%s", NewRequestQueue, fRuntime.FlowName)
}

func (fRuntime *FlowRuntime) requestQueueId() string {
	return fRuntime.FlowName
}

func makeRequestFromArgs(args ...interface{}) (*runtime.Request, error) {
	request := &runtime.Request{}

	if args[0] != nil {
		requestId, ok := args[0].(string)
		if !ok {
			return nil, fmt.Errorf("failed to load requestId from arguments %v", args[0])
		}
		request.RequestID = requestId
	}

	if args[1] != nil {
		body, ok := args[1].(string)
		if !ok {
			return nil, fmt.Errorf("failed to load body from arguments %v", args[1])
		}
		request.Body = []byte(body)
	}

	if args[2] != nil {
		header, ok := args[2].(map[string][]string)
		if !ok {

			return nil, fmt.Errorf("failed to load header from arguments %v", args[2])
		}
		request.Header = header
	} else {
		request.Header = make(map[string][]string)
	}

	if args[3] != nil {
		rawQuery, ok := args[3].(string)
		if !ok {

			return nil, fmt.Errorf("failed to load raw-query from arguments %v", args[3])
		}
		request.RawQuery = rawQuery
	}

	if args[4] != nil {
		query, ok := args[4].(map[string][]string)
		if !ok {

			return nil, fmt.Errorf("failed to load query from arguments %v", args[4])
		}
		request.Query = query
	} else {
		request.Query = make(map[string][]string)
	}

	return request, nil
}
