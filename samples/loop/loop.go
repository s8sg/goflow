package loop

import (
	"fmt"
	flow "github.com/s8sg/goflow/flow/v1"
	"math/rand"
)

// Workload function
func node1(data []byte, option map[string][]string) ([]byte, error) {
	result := fmt.Sprintf("(Executing node 1 with data (%s))", string(data))
	fmt.Println(result)
	return []byte(result), nil
}

func loopFunction(response []byte) map[string][]byte {
	forEachCondition := make(map[string][]byte)
	randNos := rand.Intn(10)
	fmt.Printf("loop will be executed %d time(s)\n", randNos)
	for i := 0; i < randNos; i++ {
		forEachCondition[fmt.Sprint("%d", i)] = response
	}
	return forEachCondition
}

// Workload function
func function(data []byte, option map[string][]string) ([]byte, error) {
	result := fmt.Sprintf("(Executing case 1 with data (%s))", string(data))
	fmt.Println(result)
	return []byte(result), nil
}

// Workload function
func result(data []byte, option map[string][]string) ([]byte, error) {
	result := fmt.Sprintf("(Executing result with data (%s))", string(data))
	fmt.Println(result)
	return []byte(result), nil
}

// loopAggregator can be used to collect and map response from multiple in-degree as a request to node
// here loopAggregator aggregates the result from different looping branches before forwarding
// it to the next node in the dag
func loopAggregator(data map[string][]byte) ([]byte, error) {
	aggregatedResult := "("
	for key, value := range data {
		aggregatedResult = aggregatedResult + fmt.Sprintf("%s: %s,", key, value)
	}
	aggregatedResult = aggregatedResult + ")"
	return []byte(aggregatedResult), nil
}

// DefineWorkflow Define provide definition of the workflow
func DefineWorkflow(workflow *flow.Workflow, context *flow.Context) error {
	dag := workflow.Dag()
	dag.Node("node1", node1)
	loopDag := dag.ForEachBranch("foreach",
		loopFunction,
		flow.Aggregator(loopAggregator))
	loopDag.Node("function", function)
	dag.Node("result", result)
	dag.Edge("node1", "foreach")
	dag.Edge("foreach", "result")
	return nil
}
