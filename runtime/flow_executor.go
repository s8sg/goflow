package runtime

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/s8sg/goflow/core/runtime"
	"github.com/s8sg/goflow/core/sdk"
	"github.com/s8sg/goflow/core/sdk/executor"
	"github.com/s8sg/goflow/eventhandler"
	v1 "github.com/s8sg/goflow/flow/v1"
)

type FlowExecutor struct {
	gateway                 string
	flowName                string // the name of the function
	reqID                   string // the request id
	CallbackURL             string // the callback url
	RequestAuthSharedSecret string
	RequestAuthEnabled      bool
	EnableMonitoring        bool
	IsLoggingEnabled        bool
	StateStore              sdk.StateStore
	DataStore               sdk.DataStore
	EventHandler            sdk.EventHandler
	Logger                  sdk.Logger
	Handler                 FlowDefinitionHandler
	Runtime                 *FlowRuntime
}

type FlowDefinitionHandler func(flow *v1.Workflow, context *v1.Context) error

func (fe *FlowExecutor) HandleNextNode(partial *executor.PartialState) error {
	var err error
	request := &runtime.Request{}
	request.Body, err = partial.Encode()
	if err != nil {
		return fmt.Errorf("failed to encode partial state, error %v", err)
	}
	request.RequestID = fe.reqID
	request.FlowName = fe.flowName
	request.Header = make(map[string][]string)
	// Monitoring logic can be added here if needed
	err = fe.Runtime.EnqueuePartialRequest(request)
	if err != nil {
		return fmt.Errorf("failed to enqueue request, error %v", err)
	}
	return nil
}

func (fe *FlowExecutor) GetExecutionOption(_ sdk.Operation) map[string]interface{} {
	options := make(map[string]interface{})
	options["gateway"] = fe.gateway
	options["request-id"] = fe.reqID

	return options
}

func (fe *FlowExecutor) HandleExecutionCompletion(data []byte) error {
	if fe.CallbackURL == "" {
		return nil
	}

	log.Printf("calling callback url (%s) with result", fe.CallbackURL)
	httpreq, _ := http.NewRequest(http.MethodPost, fe.CallbackURL, bytes.NewReader(data))
	httpreq.Header.Add("X-Faas-Flow-ReqiD", fe.reqID)
	client := &http.Client{}

	res, resErr := client.Do(httpreq)
	if resErr != nil {
		return resErr
	}
	defer func() {
		if err := res.Body.Close(); err != nil {
			log.Printf("Error closing response body: %v", err)
		}
	}()
	resData, _ := io.ReadAll(res.Body)

	if res.StatusCode != http.StatusOK && res.StatusCode != http.StatusAccepted {
		return fmt.Errorf("failed to call callback %d: %s", res.StatusCode, string(resData))
	}

	return nil
}

func (fe *FlowExecutor) Configure(requestID string) {
	fe.reqID = requestID
}

func (fe *FlowExecutor) GetFlowName() string {
	return fe.flowName
}

func (fe *FlowExecutor) GetFlowDefinition(pipeline *sdk.Pipeline, context *sdk.Context) error {
	workflow := v1.GetWorkflow(pipeline)
	faasflowContext := (*v1.Context)(context)
	return fe.Handler(workflow, faasflowContext)
}

func (fe *FlowExecutor) ReqValidationEnabled() bool {
	return false
}

func (fe *FlowExecutor) GetValidationKey() (string, error) {
	return "", nil
}

func (fe *FlowExecutor) ReqAuthEnabled() bool {
	return fe.RequestAuthEnabled
}

func (fe *FlowExecutor) GetReqAuthKey() (string, error) {
	return fe.RequestAuthSharedSecret, nil
}

func (fe *FlowExecutor) MonitoringEnabled() bool {
	return fe.EnableMonitoring
}

func (fe *FlowExecutor) GetEventHandler() (sdk.EventHandler, error) {
	return fe.EventHandler.Copy()
}

func (fe *FlowExecutor) LoggingEnabled() bool {
	return fe.IsLoggingEnabled
}

func (fe *FlowExecutor) GetLogger() (sdk.Logger, error) {
	return fe.Logger, nil
}

func (fe *FlowExecutor) GetStateStore() (sdk.StateStore, error) {
	return fe.StateStore, nil
}

func (fe *FlowExecutor) GetDataStore() (sdk.DataStore, error) {
	return fe.DataStore, nil
}

func (fe *FlowExecutor) Init(request *runtime.Request) error {
	fe.flowName = request.FlowName

	callbackURL := request.GetHeader("X-Faas-Flow-Callback-Url")
	fe.CallbackURL = callbackURL

	faasHandler := fe.EventHandler.(*eventhandler.GoFlowEventHandler)
	faasHandler.Header = request.Header

	return nil
}
