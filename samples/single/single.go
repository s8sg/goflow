package single

import (
	"fmt"
	flow "github.com/s8sg/goflow/flow/v1"
)

// Workload function
func node1(data []byte, option map[string][]string) ([]byte, error) {
	result := fmt.Sprintf("(Executing node 1 with data (%s))", string(data))
	fmt.Println(result)
	return []byte(result), nil
}

// DefineWorkflow Define provide definition of the workflow
func DefineWorkflow(workflow *flow.Workflow, context *flow.Context) error {
	dag := workflow.Dag()
	dag.Node("node1", node1)
	return nil
}
