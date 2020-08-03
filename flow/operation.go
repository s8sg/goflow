package flow

import (
	"fmt"
)

var (
	BLANK_MODIFIER = func(data []byte) ([]byte, error) { return data, nil }
)

// FuncErrorHandler the error handler for OnFailure() options
type FuncErrorHandler func(error) error

// Modifier definition for Modify() call
type Modifier func([]byte, map[string][]string) ([]byte, error)

type ServiceOperation struct {
	Id      string              // ID
	Mod     Modifier            // Modifier
	Options map[string][]string // The option as a input to workload

	FailureHandler FuncErrorHandler // The Failure handler of the operation
}

// createWorkload Create a function with execution name
func createWorkload(id string, mod Modifier) *ServiceOperation {
	operation := &ServiceOperation{}
	operation.Mod = mod
	operation.Id = id
	operation.Options = make(map[string][]string)
	return operation
}

func (operation *ServiceOperation) addOptions(key string, value string) {
	array, ok := operation.Options[key]
	if !ok {
		operation.Options[key] = make([]string, 1)
		operation.Options[key][0] = value
	} else {
		operation.Options[key] = append(array, value)
	}
}

func (operation *ServiceOperation) addFailureHandler(handler FuncErrorHandler) {
	operation.FailureHandler = handler
}

func (operation *ServiceOperation) GetOptions() map[string][]string {
	return operation.Options
}

func (operation *ServiceOperation) GetId() string {
	return operation.Id
}

func (operation *ServiceOperation) Encode() []byte {
	return []byte("")
}

// executeWorkload executes a function call
func executeWorkload(operation *ServiceOperation, data []byte) ([]byte, error) {
	var err error
	var result []byte

	options := operation.GetOptions()
	result, err = operation.Mod(data, options)

	return result, err
}

func (operation *ServiceOperation) Execute(data []byte, option map[string]interface{}) ([]byte, error) {
	var result []byte
	var err error

	if operation.Mod != nil {
		result, err = executeWorkload(operation, data)
		if err != nil {
			err = fmt.Errorf("function(%s), error: function execution failed, %v",
				operation.Id, err)
			if operation.FailureHandler != nil {
				err = operation.FailureHandler(err)
			}
			if err != nil {
				return nil, err
			}
		}
	}

	return result, nil
}

func (operation *ServiceOperation) GetProperties() map[string][]string {

	result := make(map[string][]string)

	isMod := "false"
	isFunction := "false"
	isHttpRequest := "false"
	hasFailureHandler := "false"

	if operation.Mod != nil {
		isFunction = "true"
	}
	if operation.FailureHandler != nil {
		hasFailureHandler = "true"
	}

	result["isMod"] = []string{isMod}
	result["isFunction"] = []string{isFunction}
	result["isHttpRequest"] = []string{isHttpRequest}
	result["hasFailureHandler"] = []string{hasFailureHandler}

	return result
}

// Apply adds a new workload to the given vertex
func (node *Node) Apply(id string, workload Modifier, opts ...Option) *Node {

	newWorkload := createWorkload(id, workload)

	o := &Options{}
	for _, opt := range opts {
		o.reset()
		opt(o)
		if len(o.option) != 0 {
			for key, array := range o.option {
				for _, value := range array {
					newWorkload.addOptions(key, value)
				}
			}
		}
		if o.failureHandler != nil {
			newWorkload.addFailureHandler(o.failureHandler)
		}
	}

	node.unode.AddOperation(newWorkload)
	return node
}
