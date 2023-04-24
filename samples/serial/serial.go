package serial

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

// Workload function
func node2(data []byte, option map[string][]string) ([]byte, error) {
	result := fmt.Sprintf("(Executing node 2 with data (%s))", string(data))
	fmt.Println(result)
	return []byte(result), nil
}

// Workload function
func node3(data []byte, option map[string][]string) ([]byte, error) {
	result := fmt.Sprintf("(Executing node 3 with data (%s))", string(data))
	fmt.Println(result)
	return []byte(result), nil
}

// DefineWorkflow Define provide definition of the workflow
func DefineWorkflow(workflow *flow.Workflow, context *flow.Context) error {
	dag := workflow.Dag()
	dag.Node("node1", node1)
	dag.Node("node2", node2)
	dag.Node("node3", node3)
	dag.Edge("node1", "node2")
	dag.Edge("node2", "node3")
	return nil
}
