package runtime

import (
	"context"
	"fmt"
	"github.com/benmanns/goworker"
	"github.com/faasflow/runtime"
	"github.com/faasflow/runtime/controller/handler"
	sdk "github.com/faasflow/sdk"
	"github.com/faasflow/sdk/executor"
	"github.com/s8sg/goflow/eventhandler"
	log2 "github.com/s8sg/goflow/log"
	"net/http"
	"strings"
	"time"
)

type FlowRuntime struct {
	Flows          map[string]FlowDefinitionHandler
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
	srv          *http.Server
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
	flowHandler, ok := fRuntime.Flows[req.FlowName]
	if !ok {
		return nil, fmt.Errorf("could not find handler for flow %s", req.FlowName)
	}
	ex := &FlowExecutor{
		StateStore:   fRuntime.stateStore,
		DataStore:    fRuntime.DataStore,
		EventHandler: fRuntime.eventHandler,
		Handler:      flowHandler,
		Logger:       fRuntime.Logger,
		Runtime:      fRuntime,
	}
	error := ex.Init(req)
	return ex, error
}

func (fRuntime *FlowRuntime) Execute(flowName string, request *runtime.Request) error {
	settings := goworker.WorkerSettings{
		URI:         "redis://" + fRuntime.RedisURL + "/",
		Connections: 10,
		Queues:      []string{fRuntime.newRequestQueueId(flowName)},
		UseNumber:   true,
		Namespace:   "resque:",
	}
	goworker.SetSettings(settings)
	return goworker.Enqueue(&goworker.Job{
		Queue: fRuntime.newRequestQueueId(flowName),
		Payload: goworker.Payload{
			Class: "GoFlow",
			Args:  []interface{}{flowName, request.RequestID, string(request.Body), request.Header, request.RawQuery, request.Query},
		},
	})
}

func (fRuntime *FlowRuntime) SetWorkerConfig() {
	var queues []string
	for flowName, _ := range fRuntime.Flows {
		queues = append(queues,
			fRuntime.requestQueueId(flowName),
			fRuntime.partialRequestQueueId(flowName),
			fRuntime.newRequestQueueId(flowName),
		)
	}
	fRuntime.settings = goworker.WorkerSettings{
		URI:            "redis://" + fRuntime.RedisURL + "/",
		Connections:    100,
		Queues:         queues,
		UseNumber:      true,
		ExitOnComplete: false,
		Concurrency:    fRuntime.Concurrency,
		Namespace:      "resque:",
		Interval:       1.0,
	}
	goworker.SetSettings(fRuntime.settings)
}

// StartServer starts listening for new request
func (fRuntime *FlowRuntime) StartServer() error {
	fRuntime.srv = &http.Server{
		Addr:           fmt.Sprintf(":%d", fRuntime.ServerPort),
		ReadTimeout:    fRuntime.ReadTimeout,
		WriteTimeout:   fRuntime.WriteTimeout,
		Handler:        router(fRuntime),
		MaxHeaderBytes: 1 << 20, // Max header of 1MB
	}

	return fRuntime.srv.ListenAndServe()
}

// StopServer stops the server
func (fRuntime *FlowRuntime) StopServer() error {
	if err := fRuntime.srv.Shutdown(context.Background()); err != nil {
		return err
	}
	return nil
}

// StartQueueWorker starts listening for request in queue
func (fRuntime *FlowRuntime) StartQueueWorker() error {
	goworker.Register("GoFlow", fRuntime.queueReceiver)
	return goworker.Work()
}

func (fRuntime *FlowRuntime) EnqueuePartialRequest(pr *runtime.Request) error {
	return goworker.Enqueue(&goworker.Job{
		Queue: fRuntime.partialRequestQueueId(pr.FlowName),
		Payload: goworker.Payload{
			Class: "GoFlow",
			Args:  []interface{}{pr.FlowName, pr.RequestID, string(pr.Body), pr.Header, pr.RawQuery, pr.Query},
		},
	})
}

func (fRuntime *FlowRuntime) queueReceiver(queue string, args ...interface{}) error {
	fRuntime.Logger.Log(fmt.Sprintf("Request received by worker at queue %v", queue))
	var err error

	switch {
	case isPartialRequest(queue):
		request, err := makeRequestFromArgs(args...)
		if err != nil {
			fRuntime.Logger.Log(err.Error())
			return err
		}
		err = fRuntime.handlePartialRequest(request)
	case isNewRequest(queue):
		request, err := makeRequestFromArgs(args...)
		if err != nil {
			fRuntime.Logger.Log(err.Error())
			return err
		}
		err = fRuntime.handleNewRequest(request)
	default:
		request := &runtime.Request{}
		body, ok := args[0].(string)
		if !ok {
			fRuntime.Logger.Log(fmt.Sprintf("failed to load request body as string from %v", args[0]))
			return fmt.Errorf("failed to load request body as string from %v", args[0])
		}
		request.Body = []byte(body)
		request.FlowName = queue
		err = fRuntime.handleNewRequest(request)
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

func (fRuntime *FlowRuntime) partialRequestQueueId(flowName string) string {
	return fmt.Sprintf("%s:%s", PartialRequestQueue, flowName)
}

func (fRuntime *FlowRuntime) newRequestQueueId(flowName string) string {
	return fmt.Sprintf("%s:%s", NewRequestQueue, flowName)
}

func (fRuntime *FlowRuntime) requestQueueId(flowName string) string {
	return flowName
}

func makeRequestFromArgs(args ...interface{}) (*runtime.Request, error) {
	request := &runtime.Request{}

	if args[0] != nil {
		flowName, ok := args[0].(string)
		if !ok {
			return nil, fmt.Errorf("failed to load flowName from arguments %v", args[0])
		}
		request.FlowName = flowName
	}

	if args[1] != nil {
		requestId, ok := args[1].(string)
		if !ok {
			return nil, fmt.Errorf("failed to load requestId from arguments %v", args[0])
		}
		request.RequestID = requestId
	}

	if args[2] != nil {
		body, ok := args[2].(string)
		if !ok {
			return nil, fmt.Errorf("failed to load body from arguments %v", args[1])
		}
		request.Body = []byte(body)
	}

	if args[3] != nil {
		header, ok := args[3].(map[string][]string)
		if !ok {

			return nil, fmt.Errorf("failed to load header from arguments %v", args[2])
		}
		request.Header = header
	} else {
		request.Header = make(map[string][]string)
	}

	if args[4] != nil {
		rawQuery, ok := args[4].(string)
		if !ok {

			return nil, fmt.Errorf("failed to load raw-query from arguments %v", args[3])
		}
		request.RawQuery = rawQuery
	}

	if args[5] != nil {
		query, ok := args[5].(map[string][]string)
		if !ok {

			return nil, fmt.Errorf("failed to load query from arguments %v", args[4])
		}
		request.Query = query
	} else {
		request.Query = make(map[string][]string)
	}

	return request, nil
}

func isPartialRequest(queue string) bool {
	return strings.HasPrefix(queue, PartialRequestQueue)
}

func isNewRequest(queue string) bool {
	return strings.HasPrefix(queue, NewRequestQueue)
}
