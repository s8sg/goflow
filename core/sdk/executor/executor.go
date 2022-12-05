package executor

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"strconv"

	hmac "github.com/alexellis/hmac"
	xid "github.com/rs/xid"
	sdk "github.com/s8sg/goflow/core/sdk"
)

// RawRequest a raw request for the flow
type RawRequest struct {
	Data          []byte
	AuthSignature string
	Query         string
	RequestId     string // RequestId is Optional, if provided faas-flow will reuse it
}

// PartialState a partial request for the flow
type PartialState struct {
	uprequest *Request
}

func DecodePartialReq(encodedState []byte) (*PartialState, error) {
	request, err := decodeRequest(encodedState)
	if err != nil {
		return nil, err
	}
	req := &PartialState{uprequest: request}
	return req, err
}

func (req *PartialState) Encode() ([]byte, error) {
	return req.uprequest.encode()
}

// ExecutionRuntime implements how operation executed and handle next nodes in async
type ExecutionRuntime interface {
	// HandleNextNode handles execution of next nodes based on partial state
	HandleNextNode(state *PartialState) (err error)
	// Provide an execution option that will be passed to the operation
	GetExecutionOption(operation sdk.Operation) map[string]interface{}
	// Handle the completion of execution of data
	HandleExecutionCompletion(data []byte) error
}

// Executor implements a faas-flow executor
type Executor interface {
	// Configure configure an executor with request id
	Configure(requestId string)
	// GetFlowName get name of the flow
	GetFlowName() string
	// GetFlowDefinition get definition of the faas-flow
	GetFlowDefinition(*sdk.Pipeline, *sdk.Context) error
	// ReqValidationEnabled check if request validation enabled
	ReqValidationEnabled() bool
	// GetValidationKey get request validation key
	GetValidationKey() (string, error)
	// ReqAuthEnabled check if request auth enabled
	ReqAuthEnabled() bool
	// GetReqAuthKey get the request auth key
	GetReqAuthKey() (string, error)
	// MonitoringEnabled check if request monitoring enabled
	MonitoringEnabled() bool
	// GetEventHandler get the event handler for request monitoring
	GetEventHandler() (sdk.EventHandler, error)
	// LoggingEnabled check if logging is enabled
	LoggingEnabled() bool
	// GetLogger get the logger
	GetLogger() (sdk.Logger, error)
	// GetStateStore get the state store
	GetStateStore() (sdk.StateStore, error)
	// GetDataStore get the data store
	GetDataStore() (sdk.DataStore, error)

	ExecutionRuntime
}

// FlowExecutor faas-flow executor
type FlowExecutor struct {
	flow *sdk.Pipeline // the faas-flow

	// the pipeline properties
	hasBranch       bool // State if the pipeline dag has at-least one branch
	hasEdge         bool // State if pipeline dag has at-least one edge
	isExecutionFlow bool // State if pipeline has execution only branches

	flowName string // the name of the flow
	id       string // the unique request id
	query    string // the query to the flow

	eventHandler sdk.EventHandler // Handler flow events
	logger       sdk.Logger       // Handle flow logs
	stateStore   sdk.StateStore   // the state store
	dataStore    sdk.DataStore    // the data store

	partial      bool          // denotes the flow is in partial execution state
	newRequest   *RawRequest   // holds the new request
	partialState *PartialState // holds the partially completed state
	finished     bool          // denote the flow has finished execution

	executor   Executor    // executor
	notifyChan chan string // notify about execution complete, if not nil
}

const (
	STATE_RUNNING  = "RUNNING"
	STATE_FINISHED = "FINISHED"
	STATE_PAUSED   = "PAUSED"
)

type ExecutionStateOptions struct {
	newRequest   *RawRequest
	partialState *PartialState
}

type ExecutionStateOption func(*ExecutionStateOptions)

func NewRequest(request *RawRequest) ExecutionStateOption {
	return func(o *ExecutionStateOptions) {
		o.newRequest = request
	}
}

func PartialRequest(partialState *PartialState) ExecutionStateOption {
	return func(o *ExecutionStateOptions) {
		o.partialState = partialState
	}
}

const (
	// signature of SHA265 equivalent of "github.com/s8sg/faas-flow"
	defaultHmacKey = "71F1D3011F8E6160813B4997BA29856744375A7F26D427D491E1CCABD4627E7C"
	// max retry count to update counter
	counterUpdateRetryCount = 10
)

// log logs using logger if logging enabled
func (fexec *FlowExecutor) log(str string, a ...interface{}) {
	if fexec.executor.LoggingEnabled() {
		str := fmt.Sprintf(str, a...)
		fexec.logger.Log(str)
	}
}

// setRequestState set the request state
func (fexec *FlowExecutor) setRequestState(state string) error {
	return fexec.stateStore.Set("request-state", state)
}

// getRequestState get state of the request
func (fexec *FlowExecutor) getRequestState() (string, error) {
	value, err := fexec.stateStore.Get("request-state")
	return value, err
}

// setDynamicBranchOptions set dynamic options for a dynamic node
func (fexec *FlowExecutor) setDynamicBranchOptions(nodeUniqueId string, options []string) error {
	encoded, err := json.Marshal(options)
	if err != nil {
		return err
	}
	return fexec.stateStore.Set(nodeUniqueId, string(encoded))
}

// getDynamicBranchOptions get dynamic options for a dynamic node
func (fexec *FlowExecutor) getDynamicBranchOptions(nodeUniqueId string) ([]string, error) {
	encoded, err := fexec.stateStore.Get(nodeUniqueId)
	if err != nil {
		return nil, err
	}
	var option []string
	err = json.Unmarshal([]byte(encoded), &option)
	return option, err
}

// incrementCounter increment counter by given term, if doesn't exist init with increment by
func (fexec *FlowExecutor) incrementCounter(counter string, incrementBy int) (count int, err error) {

	var oldCount int64
	for i := 0; i < counterUpdateRetryCount; i++ {
		oldCount, err = fexec.stateStore.IncrBy(counter, int64(incrementBy))
		if err != nil {
			count = 0
			err = fmt.Errorf("failed to update counter %s, error %v", counter, err)
			return
		}
	}

	count = int(oldCount)
	return
}

// retrieveCounter retrieves a counter value
func (fexec *FlowExecutor) retrieveCounter(counter string) (int, error) {
	encoded, err := fexec.stateStore.Get(counter)
	if err != nil {
		return 0, fmt.Errorf("failed to get counter %s, error %v", counter, err)
	}
	current, err := strconv.Atoi(encoded)
	if err != nil {
		return 0, fmt.Errorf("failed to get counter %s, error %v", counter, err)
	}
	return current, nil
}

func (fexec *FlowExecutor) storePartialState(partialState *PartialState) error {

	data, _ := partialState.Encode()
	enState := string(data)

	partialStates := []string{enState}
	key := "partial-state"

	var serr error
	for i := 0; i < counterUpdateRetryCount; i++ {
		encoded, err := fexec.stateStore.Get(key)
		if err != nil {

			data, _ := json.Marshal(partialStates)
			// if doesn't exist try to create
			err := fexec.stateStore.Set(key, string(data))
			if err != nil {
				serr = fmt.Errorf("failed to update partial-state, error %v", err)
				continue
			}
			return nil
		}

		err = json.Unmarshal([]byte(encoded), &partialStates)
		if err != nil {
			return fmt.Errorf("failed to update partial-state, error %v", err)
		}

		partialStates = append(partialStates, enState)

		data, _ := json.Marshal(partialStates)

		err = fexec.stateStore.Update(key, encoded, string(data))
		if err == nil {
			return nil
		}
		serr = err
	}
	return fmt.Errorf("failed to update partial-state after max retry, error %v", serr)
}

func (fexec *FlowExecutor) retrievePartialStates() ([]*PartialState, error) {

	key := "partial-state"
	var encodedStates []string
	var partialStates []*PartialState

	encoded, err := fexec.stateStore.Get(key)
	if err != nil {
		return nil, fmt.Errorf("failed to retrive partial-state, error %v", err)
	}

	err = json.Unmarshal([]byte(encoded), &encodedStates)
	if err != nil {
		return nil, fmt.Errorf("failed to retrive partial-state, error %v", err)
	}

	for _, state := range encodedStates {
		ps, err := DecodePartialReq([]byte(state))
		if err != nil {
			return nil, fmt.Errorf("failed to retrive partial-state, error %v", err)
		}
		partialStates = append(partialStates, ps)
	}
	return partialStates, nil
}

// isActive check if flow is active
func (fexec *FlowExecutor) isActive() bool {
	state, err := fexec.getRequestState()
	if err != nil {
		fexec.log("[request `%s`] failed to obtain pipeline state, error %v\n", fexec.id, err)
		return false
	}

	return state == STATE_RUNNING || state == STATE_PAUSED
}

// isRunning check if flow is running
func (fexec *FlowExecutor) isRunning() bool {
	state, err := fexec.getRequestState()
	if err != nil {
		fexec.log("[request `%s`] failed to obtain pipeline state\n", fexec.id)
		return false
	}

	return state == STATE_RUNNING
}

// isPaused check if flow is paused
func (fexec *FlowExecutor) isPaused() bool {
	state, err := fexec.getRequestState()
	if err != nil {
		fexec.log("[request `%s`] failed to obtain pipeline state\n", fexec.id)
		return false
	}

	return state == STATE_PAUSED
}

// executeNode  executes a node on a faas-flow dag
func (fexec *FlowExecutor) executeNode(request []byte) ([]byte, error) {
	var result []byte
	var err error

	pipeline := fexec.flow

	currentNode, _ := pipeline.GetCurrentNodeDag()

	// mark as start of node
	if fexec.executor.MonitoringEnabled() {
		fexec.eventHandler.ReportNodeStart(currentNode.GetUniqueId(), fexec.id)
	}

	for _, operation := range currentNode.Operations() {
		// Check if request is terminate
		if !fexec.isActive() {
			fexec.log("[request `%s`] pipeline is not active\n", fexec.id)
			panic(fmt.Sprintf("[request `%s`] Pipeline is not active", fexec.id))
		}

		if fexec.executor.MonitoringEnabled() {
			fexec.eventHandler.ReportOperationStart(operation.GetId(), currentNode.GetUniqueId(), fexec.id)
		}

		options := fexec.executor.GetExecutionOption(operation)

		if result == nil {
			result, err = operation.Execute(request, options)
		} else {
			result, err = operation.Execute(result, options)
		}
		if err != nil {
			if fexec.executor.MonitoringEnabled() {
				fexec.eventHandler.ReportOperationFailure(operation.GetId(), currentNode.GetUniqueId(), fexec.id, err)
			}
			err = fmt.Errorf("Node(%s), Operation (%s), error: execution failed, %v",
				currentNode.GetUniqueId(), operation.GetId(), err)
			return nil, err
		}
		if fexec.executor.MonitoringEnabled() {
			fexec.eventHandler.ReportOperationEnd(operation.GetId(), currentNode.GetUniqueId(), fexec.id)
		}
	}

	fexec.log("[request `%s`] completed execution of node %s\n", fexec.id, currentNode.GetUniqueId())

	return result, nil
}

// findCurrentNodeToExecute find right node to execute based on state
func (fexec *FlowExecutor) findCurrentNodeToExecute() {
	currentNode, currentDag := fexec.flow.GetCurrentNodeDag()

	fexec.log("[request `%s`] executing node %s\n", fexec.id, currentNode.GetUniqueId())

	// recurse to the subdag - if a node is dynamic stop to evaluate it
	for true {
		// break if request is dynamic
		if currentNode.Dynamic() {
			return
		}
		subdag := currentNode.SubDag()
		if subdag == nil {
			break
		}
		// trace node - mark as start of the parent node
		if fexec.executor.MonitoringEnabled() {
			fexec.eventHandler.ReportNodeStart(currentNode.GetUniqueId(), fexec.id)
		}
		fexec.log("[request `%s`] executing node %s\n", fexec.id, currentNode.GetUniqueId())
		currentDag = subdag
		currentNode = currentDag.GetInitialNode()
		fexec.flow.UpdatePipelineExecutionPosition(sdk.DEPTH_INCREMENT, currentNode.Id)
	}
}

// forwardState forward async request to core
func (fexec *FlowExecutor) forwardState(currentNodeId string, nextNodeId string, result []byte) error {
	var sign string
	store := make(map[string][]byte)

	// get pipeline
	pipeline := fexec.flow

	// Get pipeline state
	pipelineState := pipeline.GetState()

	defaultStore, ok := fexec.dataStore.(*requestEmbedDataStore)
	if ok {
		store = defaultStore.store
	}

	// Check if request validation used
	if fexec.executor.ReqValidationEnabled() {
		key, err := fexec.executor.GetValidationKey()
		if err != nil {
			return fmt.Errorf("failed to get key, error %v", err)
		}
		hash := hmac.Sign([]byte(pipelineState), []byte(key))
		sign = "sha1=" + hex.EncodeToString(hash)
	}

	// Build request
	uprequest := buildRequest(fexec.id, string(pipelineState), fexec.query, result, store, sign)

	if fexec.executor.MonitoringEnabled() {
		fexec.eventHandler.ReportExecutionForward(currentNodeId, fexec.id)
	}

	partialState := &PartialState{uprequest: uprequest}

	var err error

	if fexec.isPaused() {
		// if request is paused, store the partial state in the StateStore
		fexec.log("[request `%s`] Request is paused, storing partial state for node: %s\n", fexec.id, nextNodeId)
		err = fexec.storePartialState(partialState)
		if err != nil {
			return err
		}
	}
	err = fexec.executor.HandleNextNode(partialState)
	if err != nil {
		return err
	}

	return nil
}

// executeDynamic executes a dynamic node
func (fexec *FlowExecutor) executeDynamic(context *sdk.Context, result []byte) ([]byte, error) {
	// get pipeline
	pipeline := fexec.flow

	currentNode, _ := pipeline.GetCurrentNodeDag()

	// trace node - mark as start of the dynamic node
	if fexec.executor.MonitoringEnabled() {
		fexec.eventHandler.ReportNodeStart(currentNode.GetUniqueId(),
			fexec.id)
	}

	currentNodeUniqueId := currentNode.GetUniqueId()
	if fexec.executor.MonitoringEnabled() {
		defer fexec.eventHandler.ReportNodeEnd(currentNodeUniqueId, fexec.id)
	}

	fexec.log("[request `%s`] processing dynamic node %s\n", fexec.id, currentNodeUniqueId)

	// sub results and sub dags
	subresults := make(map[string][]byte)
	subdags := make(map[string]*sdk.Dag)
	options := []string{}

	condition := currentNode.GetCondition()
	foreach := currentNode.GetForEach()

	switch {
	case condition != nil:
		fexec.log("[request `%s`] executing condition\n", fexec.id)
		conditions := condition(result)
		if conditions == nil {
			panic(fmt.Sprintf("Condition function at %s returned nil, failed to proceed",
				currentNodeUniqueId))
		}
		for _, conditionKey := range conditions {
			if len(conditionKey) == 0 {
				panic(fmt.Sprintf("Condition function at %s returned invalid condiiton, failed to proceed",
					currentNodeUniqueId))
			}
			subdags[conditionKey] = currentNode.GetConditionalDag(conditionKey)
			if subdags[conditionKey] == nil {
				panic(fmt.Sprintf("Condition function at %s returned invalid condiiton, failed to proceed",
					currentNodeUniqueId))
			}
			subresults[conditionKey] = result
			options = append(options, conditionKey)
		}
	case foreach != nil:
		fexec.log("[request `%s`] executing foreach\n", fexec.id)
		foreachResults := foreach(result)
		if foreachResults == nil {
			panic(fmt.Sprintf("Foreach function at %s returned nil, failed to proceed",
				currentNodeUniqueId))
		}
		for foreachKey, foreachResult := range foreachResults {
			if len(foreachKey) == 0 {
				panic(fmt.Sprintf("Foreach function at %s returned invalid key, failed to proceed",
					currentNodeUniqueId))
			}
			subdags[foreachKey] = currentNode.SubDag()
			subresults[foreachKey] = foreachResult
			options = append(options, foreachKey)
		}
	}

	branchCount := len(options)
	if branchCount == 0 {
		return nil, fmt.Errorf("[request `%s`] Dynamic Node %s, failed to execute as condition/foreach returned no option",
			fexec.id, currentNodeUniqueId)
	}

	// Set the no of branch completion for the current dynamic node
	key := pipeline.GetNodeExecutionUniqueId(currentNode) + "-branch-completion"
	_, err := fexec.incrementCounter(key, 0)
	if err != nil {
		return nil, fmt.Errorf("[request `%s`] Failed to initiate dynamic in-degree count for %s, err %v",
			fexec.id, key, err)
	}

	fexec.log("[request `%s`] dynamic in-degree count initiated as %s\n",
		fexec.id, key)

	// Set all the dynamic options for the current dynamic node
	key = pipeline.GetNodeExecutionUniqueId(currentNode) + "-dynamic-branch-options"
	err = fexec.setDynamicBranchOptions(key, options)
	if err != nil {
		return nil, fmt.Errorf("[request `%s`] Dynamic Node %s, failed to store dynamic options",
			fexec.id, currentNodeUniqueId)
	}

	fexec.log("[request `%s`] dynamic options initiated as %s\n",
		fexec.id, key)

	for option, subdag := range subdags {

		subNode := subdag.GetInitialNode()
		intermediateData := subresults[option]

		// If forwarder is not nil its not an execution flow
		if currentNode.GetForwarder("dynamic") != nil {
			key := fmt.Sprintf("%s--%s--%s", option,
				pipeline.GetNodeExecutionUniqueId(currentNode), subNode.GetUniqueId())

			serr := context.Set(key, intermediateData)
			if serr != nil {
				return []byte(""), fmt.Errorf("failed to store intermediate result, error %v", serr)
			}
			fexec.log("[request `%s`] intermediate result for option %s from Node %s to %s stored as %s\n",
				fexec.id, option, currentNodeUniqueId, subNode.GetUniqueId(), key)

			// intermediateData is set to blank once its stored in storage
			intermediateData = []byte("")
		}

		// Increment the depth to execute the dynamic branch
		pipeline.UpdatePipelineExecutionPosition(sdk.DEPTH_INCREMENT, subNode.Id)
		// Set the option the dynamic branch is performing
		pipeline.CurrentDynamicOption[currentNode.GetUniqueId()] = option

		// forward the flow request
		forwardErr := fexec.forwardState(currentNode.GetUniqueId(), subNode.GetUniqueId(),
			intermediateData)
		if forwardErr != nil {
			// reset dag execution position
			pipeline.UpdatePipelineExecutionPosition(sdk.DEPTH_DECREMENT, currentNode.Id)
			return nil, fmt.Errorf("Node(%s): error: %v",
				currentNode.GetUniqueId(), forwardErr)
		}

		fexec.log("[request `%s`] request submitted for node %s option %s\n",
			fexec.id, subNode.GetUniqueId(), option)

		// reset pipeline
		pipeline.UpdatePipelineExecutionPosition(sdk.DEPTH_DECREMENT, currentNode.Id)
		delete(pipeline.CurrentDynamicOption, currentNode.GetUniqueId())
	}

	return []byte(""), nil
}

// findNextNodeToExecute find the next node(s) to execute after the current node
func (fexec *FlowExecutor) findNextNodeToExecute() bool {
	// get pipeline
	pipeline := fexec.flow

	// Check if pipeline is active in state-store
	if !fexec.isActive() {
		fexec.log("[request `%s`] pipeline is not active\n", fexec.id)
		panic(fmt.Sprintf("[request `%s`] Pipeline is not active", fexec.id))
	}

	currentNode, currentDag := pipeline.GetCurrentNodeDag()
	// Check if the pipeline has completed execution return
	// else change depth and continue executing
	for true {
		if fexec.executor.MonitoringEnabled() {
			defer fexec.eventHandler.ReportNodeEnd(currentNode.GetUniqueId(), fexec.id)
		}

		// If nodes left in current dag return
		if currentNode.Children() != nil {
			return true
		}

		// If depth 0 then pipeline has finished
		if pipeline.ExecutionDepth == 0 {
			fexec.finished = true
			return false
		} else {
			// Update position to lower depth
			currentNode = currentDag.GetParentNode()
			currentDag = currentNode.ParentDag()
			pipeline.UpdatePipelineExecutionPosition(sdk.DEPTH_DECREMENT, currentNode.Id)
			fexec.log("[request `%s`] executing node %s", fexec.id, currentNode.GetUniqueId())

			// mark execution of the node for new depth
			if fexec.executor.MonitoringEnabled() {
				defer fexec.eventHandler.ReportNodeStart(currentNode.GetUniqueId(), fexec.id)
			}

			// If current node is a dynamic node, forward the request for its end
			if currentNode.Dynamic() {
				break
			}
		}
	}
	return false
}

// handleDynamicEnd handles the end of a dynamic node
func (fexec *FlowExecutor) handleDynamicEnd(context *sdk.Context, result []byte) ([]byte, error) {

	pipeline := fexec.flow
	currentNode, _ := pipeline.GetCurrentNodeDag()

	// Get dynamic options computed for the current dynamic node
	key := pipeline.GetNodeExecutionUniqueId(currentNode) + "-dynamic-branch-options"
	options, err := fexec.getDynamicBranchOptions(key)
	if err != nil {
		return nil, fmt.Errorf("failed to retrive dynamic options for %v, error %v",
			currentNode.GetUniqueId(), err)
	}
	// Get unique execution id of the node
	branchkey := pipeline.GetNodeExecutionUniqueId(currentNode) + "-branch-completion"

	// if in-degree is > 1 then use state-store to get in-degree completion state
	if len(options) > 1 {

		// Get unique execution id of the node
		key = pipeline.GetNodeExecutionUniqueId(currentNode) + "-branch-completion"
		// Update the state of in-degree completion and get the updated state

		// Skip if dynamic node data forwarding is not disabled
		if currentNode.GetForwarder("dynamic") != nil {
			option := pipeline.CurrentDynamicOption[currentNode.GetUniqueId()]
			key = fmt.Sprintf("%s--%s--%s", option, pipeline.GetNodeExecutionUniqueId(currentNode), currentNode.GetUniqueId())

			err = context.Set(key, result)
			if err != nil {
				return nil, fmt.Errorf("failed to store branch result of dynamic node %s for option %s, error %v",
					currentNode.GetUniqueId(), option, err)
			}
			fexec.log("[request `%s`] intermediate result from branch to dynamic node %s for option %s stored as %s\n",
				fexec.id, currentNode.GetUniqueId(), option, key)
		}
		realIndegree, err := fexec.incrementCounter(branchkey, 1)
		if err != nil {
			return []byte(""), fmt.Errorf("failed to update inDegree counter for node %s", currentNode.GetUniqueId())
		}

		fexec.log("[request `%s`] executing end of dynamic node %s, completed in-degree: %d/%d\n",
			fexec.id, currentNode.GetUniqueId(), realIndegree, len(options))

		//not last branch return
		if realIndegree < len(options) {
			return nil, nil
		}
	} else {
		fexec.log("[request `%s`] executing end of dynamic node %s, branch count 1\n",
			fexec.id, currentNode.GetUniqueId())
	}
	// get current execution option
	currentOption := pipeline.CurrentDynamicOption[currentNode.GetUniqueId()]

	// skip aggregating if Data Forwarder is disabled for dynamic node
	if currentNode.GetForwarder("dynamic") == nil {
		return []byte(""), nil
	}

	subDataMap := make(map[string][]byte)
	// Store the current data
	subDataMap[currentOption] = result

	// Receive data from a dynamic graph for each options
	for _, option := range options {
		key := fmt.Sprintf("%s--%s--%s",
			option, pipeline.GetNodeExecutionUniqueId(currentNode), currentNode.GetUniqueId())

		// skip retrieving data for current option
		if option == currentOption {
			context.Del(key)
			continue
		}

		idata := context.GetBytes(key)
		fexec.log("[request `%s`] intermediate result from branch to dynamic node %s for option %s retrieved from %s\n",
			fexec.id, currentNode.GetUniqueId(), option, key)
		// delete Intermediate data after retrieval
		context.Del(key)

		subDataMap[option] = idata
	}

	// Get SubAggregator and call
	fexec.log("[request `%s`] executing aggregator of dynamic node %s\n",
		fexec.id, currentNode.GetUniqueId())
	aggregator := currentNode.GetSubAggregator()
	data, serr := aggregator(subDataMap)
	if serr != nil {
		serr := fmt.Errorf("failed to aggregate dynamic node data, error %v", serr)
		return nil, serr
	}
	if data == nil {
		data = []byte("")
	}

	return data, nil
}

// handleNextNodes Handle request Response for a faas-flow perform response/async-forward
func (fexec *FlowExecutor) handleNextNodes(context *sdk.Context, result []byte) ([]byte, error) {
	// get pipeline
	pipeline := fexec.flow

	currentNode, _ := pipeline.GetCurrentNodeDag()
	nextNodes := currentNode.Children()

	for _, node := range nextNodes {

		var intermediateData []byte

		// Node's total In-degree
		inDegree := node.Indegree()

		// Get forwarder for child node
		forwarder := currentNode.GetForwarder(node.Id)

		if forwarder != nil {
			// call default or user defined forwarder
			intermediateData = forwarder(result)
			key := fmt.Sprintf("%s--%s", pipeline.GetNodeExecutionUniqueId(currentNode),
				node.GetUniqueId())

			serr := context.Set(key, intermediateData)
			if serr != nil {
				return []byte(""), fmt.Errorf("failed to store intermediate result, error %v", serr)
			}
			fexec.log("[request `%s`] intermediate result from node %s to %s stored as %s\n",
				fexec.id, currentNode.GetUniqueId(), node.GetUniqueId(), key)

			// intermediateData is set to blank once its stored in storage
			intermediateData = []byte("")
		} else {
			// in case NoneForward forwarder in nil
			intermediateData = []byte("")
		}

		// if in-degree is > 1 then use state-store to get in-degree completion state
		if inDegree > 1 {
			// Update the state of in-degree completion and get the updated state
			key := pipeline.GetNodeExecutionUniqueId(node)
			inDegreeUpdatedCount, err := fexec.incrementCounter(key, 1)
			if err != nil {
				return []byte(""), fmt.Errorf("failed to update inDegree counter for node %s", node.GetUniqueId())
			}

			// If all in-degree has finished call that node
			if inDegree > inDegreeUpdatedCount {
				fexec.log("[request `%s`] request for Node %s is delayed, completed indegree: %d/%d\n",
					fexec.id, node.GetUniqueId(), inDegreeUpdatedCount, inDegree)
				continue
			} else {
				fexec.log("[request `%s`] performing request for Node %s, completed indegree: %d/%d\n",
					fexec.id, node.GetUniqueId(), inDegreeUpdatedCount, inDegree)
			}
		} else {
			fexec.log("[request `%s`] performing request for Node %s, indegree count is 1\n",
				fexec.id, node.GetUniqueId())
		}
		// Set the DagExecutionPosition as of next Node
		pipeline.UpdatePipelineExecutionPosition(sdk.DEPTH_SAME, node.Id)

		// forward the flow request
		forwardErr := fexec.forwardState(currentNode.GetUniqueId(), node.GetUniqueId(),
			intermediateData)
		if forwardErr != nil {
			// reset dag execution position
			pipeline.UpdatePipelineExecutionPosition(sdk.DEPTH_SAME, currentNode.Id)
			return nil, fmt.Errorf("Node(%s): error: %v",
				node.GetUniqueId(), forwardErr)
		}

		fexec.log("[request `%s`] request submitted for Node %s\n",
			fexec.id, node.GetUniqueId())

	}

	// reset dag execution position
	pipeline.UpdatePipelineExecutionPosition(sdk.DEPTH_SAME, currentNode.Id)

	return []byte(""), nil
}

// handleFailure handles failure with failure handler and call finally
func (fexec *FlowExecutor) handleFailure(context *sdk.Context, err error) {
	var data []byte

	context.State = sdk.StateFailure
	// call failure handler if available
	if fexec.flow.FailureHandler != nil {
		fexec.log("[request `%s`] calling failure handler for error, %v\n",
			fexec.id, err)
		data, err = fexec.flow.FailureHandler(err)
	}

	fexec.finished = true

	// call finally handler if available
	if fexec.flow.Finally != nil {
		fexec.log("[request `%s`] calling Finally handler with state: %s\n",
			fexec.id, sdk.StateFailure)
		fexec.flow.Finally(sdk.StateFailure)
	}
	if data != nil {
		fexec.log("%s", string(data))
	}

	// Cleanup data and state for failure
	if fexec.stateStore != nil {
		fexec.stateStore.Cleanup()
	}
	fexec.dataStore.Cleanup()

	if fexec.executor.MonitoringEnabled() {
		fexec.eventHandler.ReportRequestFailure(fexec.id, err)
		fexec.eventHandler.Flush()
	}

	fmt.Sprintf("[request `%s`] Failed, %v\n", fexec.id, err)
}

// getDagIntermediateData gets the intermediate data from earlier vertex
func (fexec *FlowExecutor) getDagIntermediateData(context *sdk.Context) ([]byte, error) {
	var data []byte

	pipeline := fexec.flow
	currentNode, dag := pipeline.GetCurrentNodeDag()
	dataMap := make(map[string][]byte)

	var scenarioFirstNodeOfDynamicBranch bool

	// if current node is the initial node of a dynamic branch dag
	dagNode := dag.GetParentNode()
	if dagNode != nil && dagNode.Dynamic() &&
		dag.GetInitialNode() == currentNode {
		scenarioFirstNodeOfDynamicBranch = true
	}

	switch {
	// handle if current node is the initial node of a dynamic branch dag
	case scenarioFirstNodeOfDynamicBranch:
		// Skip if NoDataForward is specified
		if dagNode.GetForwarder("dynamic") != nil {
			option := pipeline.CurrentDynamicOption[dagNode.GetUniqueId()]
			// Option not get appended in key as its already in state
			key := fmt.Sprintf("%s--%s", pipeline.GetNodeExecutionUniqueId(dagNode), currentNode.GetUniqueId())
			data = context.GetBytes(key)
			fexec.log("[request `%s`] intermediate result from Node %s to Node %s for option %s retrieved from %s\n",
				fexec.id, dagNode.GetUniqueId(), currentNode.GetUniqueId(),
				option, key)
			// delete intermediate data after retrieval
			context.Del(key)
		}

	// handle normal scenario
	default:
		dependencies := currentNode.Dependency()
		// current node has dependencies in same dag
		for _, node := range dependencies {

			// Skip if NoDataForward is specified
			if node.GetForwarder(currentNode.Id) == nil {
				continue
			}

			key := fmt.Sprintf("%s--%s", pipeline.GetNodeExecutionUniqueId(node), currentNode.GetUniqueId())
			idata := context.GetBytes(key)
			fexec.log("[request `%s`] intermediate result from Node %s to Node %s retrieved from %s\n",
				fexec.id, node.GetUniqueId(), currentNode.GetUniqueId(), key)
			// delete intermediate data after retrieval
			context.Del(key)

			dataMap[node.Id] = idata

		}

		// Avail the non aggregated input at context
		context.NodeInput = dataMap

		// If it has only one in-degree assign the result as a data
		if len(dependencies) == 1 {
			data = dataMap[dependencies[0].Id]
		}

		// If Aggregator is available get aggregator
		aggregator := currentNode.GetAggregator()
		if aggregator != nil {
			sdata, serr := aggregator(dataMap)
			if serr != nil {
				serr := fmt.Errorf("failed to aggregate data, error %v", serr)
				return data, serr
			}
			data = sdata
		}

	}

	return data, nil
}

// initializeStore initialize the store
func (fexec *FlowExecutor) initializeStore() (stateSDefined bool, dataSOverride bool, err error) {

	stateSDefined = false
	dataSOverride = false

	// Initialize the stateS
	stateS, err := fexec.executor.GetStateStore()
	if err != nil {
		return
	}

	if stateS != nil {
		fexec.stateStore = stateS
		stateSDefined = true
		fexec.stateStore.Configure(fexec.flowName, fexec.id)
		// If request is not partial initialize the stateStore
		if !fexec.partial {
			err = fexec.stateStore.Init()
			if err != nil {
				return
			}
		}
	}

	// Initialize the dataS
	dataS, err := fexec.executor.GetDataStore()
	if err != nil {
		return
	}
	if dataS != nil {
		fexec.dataStore = dataS
		dataSOverride = true
	}
	fexec.dataStore.Configure(fexec.flowName, fexec.id)
	// If request is not partial initialize the dataStore
	if !fexec.partial {
		err = fexec.dataStore.Init()
	}

	return
}

// createContext create a context from request handler
func (fexec *FlowExecutor) createContext() *sdk.Context {
	context := sdk.CreateContext(fexec.id, "",
		fexec.flowName, fexec.dataStore)
	context.Query, _ = url.ParseQuery(fexec.query)

	return context
}

// init initialize the executor object and context
func (fexec *FlowExecutor) init() ([]byte, error) {
	requestId := ""
	var requestData []byte
	var err error

	switch fexec.partial {
	case false: // new request
		rawRequest := fexec.newRequest
		if fexec.executor.ReqAuthEnabled() {
			signature := rawRequest.AuthSignature
			key, err := fexec.executor.GetReqAuthKey()
			if err != nil {
				return nil, fmt.Errorf("failed to get auth key, error %v", err)
			}
			err = hmac.Validate(rawRequest.Data, signature, key)
			if err != nil {
				return nil, fmt.Errorf("failed to authenticate request, error %v", err)
			}
		}

		// Use request Id if already provided
		requestId = rawRequest.RequestId
		if requestId == "" {
			requestId = xid.New().String()
		}
		fexec.executor.Configure(requestId)

		fexec.flowName = fexec.executor.GetFlowName()
		fexec.flow = sdk.CreatePipeline()
		fexec.id = requestId
		fexec.query = rawRequest.Query
		fexec.dataStore = createDataStore()

		requestData = rawRequest.Data

		if fexec.executor.MonitoringEnabled() {
			fexec.eventHandler, err = fexec.executor.GetEventHandler()
			if err != nil {
				return nil, fmt.Errorf("failed to initialize EventHandler, error %v", err)
			}
			fexec.eventHandler.Configure(fexec.flowName, fexec.id)
			err := fexec.eventHandler.Init()
			if err != nil {
				return nil, fmt.Errorf("failed to initialize EventHandler, error %v", err)
			}
			fexec.eventHandler.ReportRequestStart(fexec.id)
		}

		if fexec.executor.LoggingEnabled() {
			fexec.logger, err = fexec.executor.GetLogger()
			if err != nil {
				return nil, fmt.Errorf("failed to initiate logger, error %v", err)
			}
			fexec.logger.Configure(requestId, fexec.flowName)
			err = fexec.logger.Init()
			if err != nil {
				return nil, fmt.Errorf("failed to initiate logger, error %v", err)
			}
		}

		fexec.log("[request `%s`] new request received\n", requestId)

	case true: // partial request

		request := fexec.partialState.uprequest

		if fexec.executor.ReqValidationEnabled() {
			key, err := fexec.executor.GetValidationKey()
			if err != nil {
				return nil, fmt.Errorf("failed to get validation key, error %v", err)
			}
			err = hmac.Validate([]byte(request.ExecutionState), request.Sign, key)
			if err != nil {
				return nil, fmt.Errorf("failed to validate partial request, error %v", err)
			}
		}

		requestId = request.getID()
		fexec.executor.Configure(requestId)

		fexec.flowName = fexec.executor.GetFlowName()
		fexec.flow = sdk.CreatePipeline()
		fexec.flow.ApplyState(request.getExecutionState())
		fexec.id = requestId
		fexec.query = request.Query
		fexec.dataStore = retrieveDataStore(request.getContextStore())

		requestData = request.getData()

		if fexec.executor.MonitoringEnabled() {
			fexec.eventHandler, err = fexec.executor.GetEventHandler()
			if err != nil {
				return nil, fmt.Errorf("failed to get EventHandler, error %v", err)
			}
			fexec.eventHandler.Configure(fexec.flowName, fexec.id)
			err := fexec.eventHandler.Init()
			if err != nil {
				return nil, fmt.Errorf("failed to initialize EventHandler, error %v", err)
			}
			fexec.eventHandler.ReportExecutionContinuation(fexec.id)
		}

		if fexec.executor.LoggingEnabled() {
			fexec.logger, err = fexec.executor.GetLogger()
			if err != nil {
				return nil, fmt.Errorf("failed to initiate logger, error %v", err)
			}
			fexec.logger.Configure(requestId, fexec.flowName)
			err = fexec.logger.Init()
			if err != nil {
				return nil, fmt.Errorf("failed to initiate logger, error %v", err)
			}
		}

		fexec.log("[request `%s`] partial request received\n", requestId)

	}

	fexec.flowName = fexec.executor.GetFlowName()

	return requestData, nil
}

// applyExecutionState apply an execution state
func (fexec *FlowExecutor) applyExecutionState(state *ExecutionStateOptions) error {

	switch {
	case state.newRequest != nil:
		fexec.partial = false
		fexec.newRequest = state.newRequest

	case state.partialState != nil:
		fexec.partial = true
		fexec.partialState = state.partialState
	default:
		return fmt.Errorf("invalid execution state")
	}
	return nil
}

// GetReqId get request id
func (fexec *FlowExecutor) GetReqId() string {
	return fexec.id
}

// Execute start faas-flow execution
func (fexec *FlowExecutor) Execute(state ExecutionStateOption) ([]byte, error) {
	var resp []byte
	var gerr error

	// Get State
	executionState := &ExecutionStateOptions{}
	state(executionState)
	err := fexec.applyExecutionState(executionState)
	if err != nil {
		return nil, err
	}

	// Init Flow: Create flow object
	data, err := fexec.init()
	if err != nil {
		return nil, err
	}

	// Init Stores: Get definition of StateStore and DataStore from user
	stateSDefined, dataSOverride, err := fexec.initializeStore()
	if err != nil {
		return nil, fmt.Errorf("[request `%s`] Failed to init flow, %v", fexec.id, err)
	}

	// Make Context: make the request context from flow
	context := fexec.createContext()

	// Get Definition: Get Pipeline definition from user implemented Define()
	err = fexec.executor.GetFlowDefinition(fexec.flow, context)
	if err != nil {
		return nil, fmt.Errorf("[request `%s`] Failed to define flow, %v", fexec.id, err)
	}

	// Validate Definition: Validate Pipeline Definition
	err = fexec.flow.Dag.Validate()
	if err != nil {
		return nil, fmt.Errorf("[request `%s`] Invalid dag, %v", fexec.id, err)
	}

	// Check Configuration: Check if executor is properly configured to execute flow

	fexec.hasBranch = fexec.flow.Dag.HasBranch()
	fexec.hasEdge = fexec.flow.Dag.HasEdge()
	fexec.isExecutionFlow = fexec.flow.Dag.IsExecutionFlow()

	// hence we Check if the pipeline is running
	if fexec.partial && !fexec.isActive() {
		return nil, fmt.Errorf("[request `%s`] flow is not running", fexec.id)
	}

	// StateStore need to be defined
	if !stateSDefined {
		return nil, fmt.Errorf("[request `%s`] Failed, StateStore need to be defined", fexec.id)
	}

	// If dags has at-least one edge
	// and nodes forwards data, data store need to be external
	if fexec.hasEdge && !fexec.isExecutionFlow && !dataSOverride {
		return nil, fmt.Errorf("[request `%s`] Failed not an execution flow, DAG data flow need external DataStore", fexec.id)
	}

	// Execute: Start execution

	// If not a partial request
	if !fexec.partial {

		// For a new dag pipeline that has edges Create the vertex in stateStore
		serr := fexec.setRequestState(STATE_RUNNING)
		if serr != nil {
			return nil, fmt.Errorf("[request `%s`] Failed to mark dag state, error %v", fexec.id, serr)
		}
		fexec.log("[request `%s`] dag state initiated at StateStore\n", fexec.id)

		// set the execution position to initial node
		// On the 0th depth set the initial node as the current execution position
		fexec.flow.UpdatePipelineExecutionPosition(sdk.DEPTH_SAME,
			fexec.flow.GetInitialNodeId())
	}

	// if not an execution only dag, for partial request get intermediate data
	if fexec.partial && !fexec.flow.Dag.IsExecutionFlow() {

		// Get intermediate data from data store
		data, gerr = fexec.getDagIntermediateData(context)
		if gerr != nil {
			gerr := fmt.Errorf("failed to retrive intermediate result, error %v", gerr)
			fexec.log("[request `%s`] Failed: %v\n", fexec.id, gerr)
			fexec.handleFailure(context, gerr)
			return nil, gerr
		}
	}

	// Find the right node to execute now
	fexec.findCurrentNodeToExecute()
	currentNode, _ := fexec.flow.GetCurrentNodeDag()
	result := []byte{}

	switch {
	// Execute the dynamic node
	case currentNode.Dynamic():
		result, err = fexec.executeDynamic(context, data)
		if err != nil {
			fexec.log("[request `%s`] failed: %v\n", fexec.id, err)
			fexec.handleFailure(context, err)
			return nil, err
		}
		// Execute the node
	default:
		result, err = fexec.executeNode(data)
		if err != nil {
			fexec.log("[request `%s`] failed: %v\n", fexec.id, err)
			fexec.handleFailure(context, err)
			return nil, err
		}

		// Find the right node to execute next
		fexec.findNextNodeToExecute()

	NodeCompletionLoop:
		for !fexec.finished {
			currentNode, _ := fexec.flow.GetCurrentNodeDag()
			switch {
			// Execute a end of dynamic node
			case currentNode.Dynamic():
				result, err = fexec.handleDynamicEnd(context, result)
				if err != nil {
					fexec.log("[request `%s`] failed: %v\n", fexec.id, err)
					fexec.handleFailure(context, err)
					return nil, err
				}
				// in case dynamic end can not be executed
				if result == nil {
					if fexec.executor.MonitoringEnabled() {
						fexec.eventHandler.ReportNodeEnd(currentNode.GetUniqueId(), fexec.id)
					}

					break NodeCompletionLoop
				}
				// in case dynamic nodes end has finished execution,
				// find next node and continue
				// although if next is a children handle it
				notLast := fexec.findNextNodeToExecute()
				if !notLast {
					continue
				}
				fallthrough
			default:
				// handle the execution iteration and update state
				result, err = fexec.handleNextNodes(context, result)
				if err != nil {
					fexec.log("[request `%s`] failed: %v\n", fexec.id, err)
					fexec.handleFailure(context, err)
					return nil, err
				}
				break NodeCompletionLoop
			}
		}
	}

	// Check if execution finished
	if fexec.finished {
		fexec.log("[request `%s`] completed successfully\n", fexec.id)
		context.State = sdk.StateSuccess
		if fexec.flow.Finally != nil {
			fexec.log("[request `%s`] calling Finally handler with state: %s\n",
				fexec.id, sdk.StateSuccess)
			fexec.flow.Finally(sdk.StateSuccess)
		}

		// Cleanup data and state for failure
		if fexec.stateStore != nil {
			fexec.stateStore.Cleanup()
		}
		fexec.dataStore.Cleanup()

		// Call execution completion handler
		fexec.log("[request `%s`] calling completion handler\n", fexec.id)
		err = fexec.executor.HandleExecutionCompletion(result)
		if err != nil {
			fexec.log("[request `%s`] completion handler failed, error %v\n", fexec.id, err)
		}
		if fexec.notifyChan != nil {
			fexec.notifyChan <- fexec.id
		}

		resp = result
	}

	if fexec.executor.MonitoringEnabled() {
		fexec.eventHandler.ReportRequestEnd(fexec.id)
		fexec.eventHandler.Flush()
	}

	return resp, nil
}

// Stop marks end of an active dag execution
func (fexec *FlowExecutor) Stop(reqId string) error {

	fexec.executor.Configure(reqId)
	fexec.flowName = fexec.executor.GetFlowName()
	fexec.id = reqId
	fexec.partial = true

	// Init Stores: Get definition of StateStore and DataStore from user
	_, _, err := fexec.initializeStore()
	if err != nil {
		return fmt.Errorf("[request `%s`] Failed to init stores, %v", fexec.id, err)
	}

	err = fexec.setRequestState(STATE_FINISHED)
	if err != nil {
		return fmt.Errorf("[request `%s`] Failed to mark dag state, error %v", fexec.id, err)
	}
	fexec.stateStore.Cleanup()

	if fexec.dataStore != nil {
		fexec.dataStore.Cleanup()
	}

	if fexec.notifyChan != nil {
		fexec.notifyChan <- fexec.id
	}

	return nil
}

// Pause pauses an active dag execution
func (fexec *FlowExecutor) Pause(reqId string) error {

	fexec.executor.Configure(reqId)
	fexec.flowName = fexec.executor.GetFlowName()
	fexec.id = reqId
	fexec.partial = true

	// Init Stores: Get definition of StateStore and DataStore from user
	_, _, err := fexec.initializeStore()
	if err != nil {
		return fmt.Errorf("[request `%s`] Failed to init stores, %v", fexec.id, err)
	}

	if !fexec.isRunning() {
		return fmt.Errorf("[request `%s`] Failed to pause, request is not running", fexec.id)
	}

	err = fexec.setRequestState(STATE_PAUSED)
	if err != nil {
		return fmt.Errorf("[request `%s`] Failed to mark dag state, error %v", fexec.id, err)
	}

	return nil
}

// Resume resumes a paused dag execution
func (fexec *FlowExecutor) Resume(reqId string) error {

	fexec.executor.Configure(reqId)
	fexec.flowName = fexec.executor.GetFlowName()
	fexec.id = reqId
	fexec.partial = true

	// Init Stores: Get definition of StateStore and DataStore from user
	_, _, err := fexec.initializeStore()
	if err != nil {
		return fmt.Errorf("[request `%s`] Failed to init stores, %v", fexec.id, err)
	}

	err = fexec.setRequestState(STATE_RUNNING)
	if err != nil {
		return fmt.Errorf("[request `%s`] Failed to mark dag state, error %v", fexec.id, err)
	}

	partialStates, err := fexec.retrievePartialStates()
	if err != nil {
		return fmt.Errorf("[request `%s`] Failed to retrive partial state, error %v", fexec.id, err)
	}

	for _, ps := range partialStates {
		err = fexec.executor.HandleNextNode(ps)
		if err != nil {
			return fmt.Errorf("[request `%s`] Failed to forward partial state, error %v", fexec.id, err)
		}
	}

	return nil
}

// GetState returns the state of the request
func (fexec *FlowExecutor) GetState(reqId string) (string, error) {
	fexec.executor.Configure(reqId)
	fexec.flowName = fexec.executor.GetFlowName()
	fexec.id = reqId
	fexec.partial = true

	// Init Stores: Get definition of StateStore and DataStore from user
	_, _, err := fexec.initializeStore()
	if err != nil {
		return "", fmt.Errorf("[request `%s`] Failed to init stores, %v", fexec.id, err)
	}

	state, err := fexec.getRequestState()
	if err != nil {
		log.Printf("[request `%s`] Failed to load state, %v. State returned STATE_FINISHED", fexec.id, err)
		return STATE_FINISHED, nil
	}

	return state, nil
}

// CreateFlowExecutor initiate a FlowExecutor with a provided Executor
func CreateFlowExecutor(executor Executor, notifyChan chan string) (fexec *FlowExecutor) {
	fexec = &FlowExecutor{executor: executor, notifyChan: notifyChan}

	return fexec
}
