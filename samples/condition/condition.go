package condition

import (
	"fmt"
	flow "github.com/s8sg/goflow/flow/v1"
	"math/rand"
)

const (
	CASE1 = "case1"
	CASE2 = "case2"
)

// Workload function
func node1(data []byte, option map[string][]string) ([]byte, error) {
	result := fmt.Sprintf("(Executing node 1 with data (%s))", string(data))
	fmt.Println(result)
	return []byte(result), nil
}

// Condition determine the condition branch to take
// by returning the list of all matched conditions
func conditionCheck(response []byte) []string {
	result := CASE1
	if rand.Int()%2 == 0 {
		result = CASE2
	}
	fmt.Println("Matched condition: " + result)
	return []string{result}
}

// Workload function to handle case1
func handleCase1(data []byte, option map[string][]string) ([]byte, error) {
	result := fmt.Sprintf("(Executing case 1 with data (%s))", string(data))
	fmt.Println(result)
	return []byte(result), nil
}

// Workload function to handle case 2
func handleCase2(data []byte, option map[string][]string) ([]byte, error) {
	result := fmt.Sprintf("(Executing case 2 with data (%s))", string(data))
	fmt.Println(result)
	return []byte(result), nil
}

// Workload function
func result(data []byte, option map[string][]string) ([]byte, error) {
	result := fmt.Sprintf("(Executing result with data (%s))", string(data))
	fmt.Println(result)
	return []byte(result), nil
}

// conditionAggregator can be used to collect and map response from multiple in-degree as a request to node
// here conditionAggregator aggregates the result from different condition branches before forwarding
// it to the next node in the dag
func conditionAggregator(data map[string][]byte) ([]byte, error) {
	case1Data, _ := data["case1"]
	case2Data, _ := data["case2"]
	aggregatedResult := fmt.Sprintf("(case1: %s, case2: %s)", case1Data, case2Data)
	return []byte(aggregatedResult), nil
}

// DefineWorkflow Define provide definition of the workflow
func DefineWorkflow(workflow *flow.Workflow, context *flow.Context) error {
	dag := workflow.Dag()
	dag.Node("node1", node1)
	branches := dag.ConditionalBranch("condition",
		[]string{CASE1, CASE2},
		conditionCheck,
		flow.Aggregator(conditionAggregator))
	branches[CASE1].Node("case1", handleCase1)
	branches[CASE2].Node("case2", handleCase2)
	dag.Node("result", result)
	dag.Edge("node1", "condition")
	dag.Edge("condition", "result")
	return nil
}
