package main

import (
	"fmt"
	goflow "github.com/s8sg/goflow/v1"
	flow "github.com/s8sg/goflow/flow/v1"
)

// Workload function
func doSomething(data []byte, option map[string][]string) ([]byte, error) {
	return []byte(fmt.Sprintf("you said \"%s\"", string(data))), nil
}

// Define provide definition of the workflow
func DefineWorkflow(workflow *flow.Workflow, context *flow.Context) error {
    dag := workflow.Dag()
    dag.Node("test", doSomething)
    return nil
}

func main() {
    fs := &goflow.FlowService{
        Port:                8080,
        RedisURL:            "localhost:6379",
        OpenTraceUrl:        "localhost:5775",
        WorkerConcurrency:   5,
    }
    fs.Register("myflow", DefineWorkflow)
    fs.Start()
}