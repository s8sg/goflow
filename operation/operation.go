package operation

import (
	"fmt"
)

// FuncErrorHandler the error handler for OnFailure() options
type FuncErrorHandler func(error) error

// Modifier definition for Modify() call
type Modifier func([]byte, map[string][]string) ([]byte, error)

type GoFlowOperation struct {
	Id      string              // ID
	Mod     Modifier            // Modifier
	Options map[string][]string // The option as a input to workload

	FailureHandler FuncErrorHandler // The Failure handler of the operation
}

func (operation *GoFlowOperation) addOptions(key string, value string) {
	array, ok := operation.Options[key]
	if !ok {
		operation.Options[key] = make([]string, 1)
		operation.Options[key][0] = value
	} else {
		operation.Options[key] = append(array, value)
	}
}

func (operation *GoFlowOperation) AddFailureHandler(handler FuncErrorHandler) {
	operation.FailureHandler = handler
}

func (operation *GoFlowOperation) GetOptions() map[string][]string {
	return operation.Options
}

func (operation *GoFlowOperation) GetId() string {
	return operation.Id
}

func (operation *GoFlowOperation) Encode() []byte {
	return []byte("")
}

// executeWorkload executes a function call
func executeWorkload(operation *GoFlowOperation, data []byte) ([]byte, error) {
	var err error
	var result []byte

	options := operation.GetOptions()
	result, err = operation.Mod(data, options)

	return result, err
}

func (operation *GoFlowOperation) Execute(data []byte, _ map[string]interface{}) ([]byte, error) {
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

func (operation *GoFlowOperation) GetProperties() map[string][]string {

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

