package sdk

// DataStore for Storing Data
type DataStore interface {
	// Configure the DaraStore with flow name and request ID
	Configure(flowName string, requestId string)
	// Initialize the DataStore (called only once in a request span)
	Init() error
	// Set store a value for key, in failure returns error
	Set(key string, value []byte) error
	// Get retrieves a value by key, if failure returns error
	Get(key string) ([]byte, error)
	// Del deletes a value by a key
	Del(key string) error
	// Cleanup all the resources in DataStore
	Cleanup() error
}

// StateStore for saving execution state
type StateStore interface {
	// Configure the StateStore with flow name and request ID
	Configure(flowName string, requestId string)
	// Initialize the StateStore (called only once in a request span)
	Init() error
	// Set a value (override existing, or create one)
	Set(key string, value string) error
	// Get a value
	Get(key string) (string, error)
	// Increase the value of key with a given increment
	IncrBy(key string, value int64) (int64, error)
	// Compare and Update a value
	Update(key string, oldValue string, newValue string) error
	// Cleanup all the resources in StateStore (called only once in a request span)
	Cleanup() error
}

// EventHandler handle flow events
type EventHandler interface {
	// Configure the EventHandler with flow name and request ID
	Configure(flowName string, requestId string)
	// Initialize an EventHandler (called only once in a request span)
	Init() error
	// ReportRequestStart report a start of request
	ReportRequestStart(requestId string)
	// ReportRequestEnd reports an end of request
	ReportRequestEnd(requestId string)
	// ReportRequestFailure reports a failure of a request with error
	ReportRequestFailure(requestId string, err error)
	// ReportExecutionForward report that an execution is forwarded
	ReportExecutionForward(nodeId string, requestId string)
	// ReportExecutionContinuation report that an execution is being continued
	ReportExecutionContinuation(requestId string)
	// ReportNodeStart report a start of a Node execution
	ReportNodeStart(nodeId string, requestId string)
	// ReportNodeStart report an end of a node execution
	ReportNodeEnd(nodeId string, requestId string)
	// ReportNodeFailure report a Node execution failure with error
	ReportNodeFailure(nodeId string, requestId string, err error)
	// ReportOperationStart reports start of an operation
	ReportOperationStart(operationId string, nodeId string, requestId string)
	// ReportOperationEnd reports an end of an operation
	ReportOperationEnd(operationId string, nodeId string, requestId string)
	// ReportOperationFailure reports failure of an operation with error
	ReportOperationFailure(operationId string, nodeId string, requestId string, err error)
	// Flush flush the reports
	Flush()
}

// Logger logs the flow logs
type Logger interface {
	// Configure configure a logger with flowname and requestID
	Configure(flowName string, requestId string)
	// Init initialize a logger
	Init() error
	// Log logs a flow log
	Log(str string)
}
