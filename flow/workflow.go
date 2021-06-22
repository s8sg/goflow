package flow

import (
	"fmt"
	"github.com/faasflow/sdk"
	"github.com/s8sg/goflow/operation"
)

type Context sdk.Context
type StateStore sdk.StateStore
type DataStore sdk.DataStore

// ExecutionOptions options for branching in DAG
type ExecutionOptions struct {
	aggregator     sdk.Aggregator
	forwarder      sdk.Forwarder
	noForwarder    bool
	failureHandler operation.FuncErrorHandler
}

type Workflow struct {
	pipeline *sdk.Pipeline // underline pipeline definition object
}

type Dag struct {
	udag *sdk.Dag
}

type Node struct {
	unode *sdk.Node
}

type Option func(*ExecutionOptions)

var (
	// Execution specify a edge doesn't forwards a data
	// but rather mention a execution direction
	Execution = InvokeEdge()
)

// reset reset the ExecutionOptions
func (o *ExecutionOptions) reset() {
	o.aggregator = nil
	o.noForwarder = false
	o.forwarder = nil
}

// Aggregator aggregates all outputs into one
func Aggregator(aggregator sdk.Aggregator) Option {
	return func(o *ExecutionOptions) {
		o.aggregator = aggregator
	}
}

// InvokeEdge denotes a edge doesn't forwards a data,
// but rather provides only an execution flow
func InvokeEdge() Option {
	return func(o *ExecutionOptions) {
		o.noForwarder = true
	}
}

// Forwarder encodes request based on need for children vertex
// by default the data gets forwarded as it is
func Forwarder(forwarder sdk.Forwarder) Option {
	return func(o *ExecutionOptions) {
		o.forwarder = forwarder
	}
}

// OnFailure Specify a function failure handler
func OnFailure(handler operation.FuncErrorHandler) Option {
	return func(o *ExecutionOptions) {
		o.failureHandler = handler
	}
}

// GetWorkflow initiates a flow with a pipeline
func GetWorkflow(pipeline *sdk.Pipeline) *Workflow {
	workflow := &Workflow{}
	workflow.pipeline = pipeline
	return workflow
}

// OnFailure set a failure handler routine for the pipeline
func (flow *Workflow) OnFailure(handler sdk.PipelineErrorHandler) {
	flow.pipeline.FailureHandler = handler
}

// Finally sets an execution finish handler routine
// it will be called once the execution has finished with state either Success/Failure
func (flow *Workflow) Finally(handler sdk.PipelineHandler) {
	flow.pipeline.Finally = handler
}

// GetPipeline expose the underlying pipeline object
func (flow *Workflow) GetPipeline() *sdk.Pipeline {
	return flow.pipeline
}

// Dag provides the workflow dag object
func (flow *Workflow) Dag() *Dag {
	dag := &Dag{}
	dag.udag = flow.pipeline.Dag
	return dag
}

// SetDag apply a predefined dag, and override the default dag
func (flow *Workflow) SetDag(dag *Dag) {
	pipeline := flow.pipeline
	pipeline.SetDag(dag.udag)
}

// NewDag creates a new dag separately from pipeline
func NewDag() *Dag {
	dag := &Dag{}
	dag.udag = sdk.NewDag()
	return dag
}

// Append generalizes a separate dag by appending its properties into current dag.
// Provided dag should be mutually exclusive
func (currentDag *Dag) Append(dag *Dag) {
	err := currentDag.udag.Append(dag.udag)
	if err != nil {
		panic(fmt.Sprintf("Error at AppendDag, %v", err))
	}
}

// Node adds a new vertex by id
func (currentDag *Dag) Node(vertex string, workload operation.Modifier, options ...Option) *Node {
	node := currentDag.udag.GetNode(vertex)
	if node == nil {
		node = currentDag.udag.AddVertex(vertex, []sdk.Operation{})
	}
	newWorkload := createWorkload(vertex, workload)
	node.AddOperation(newWorkload)
	o := &ExecutionOptions{}
	for _, opt := range options {
		o.reset()
		opt(o)
		if o.aggregator != nil {
			node.AddAggregator(o.aggregator)
		}
		if o.failureHandler != nil {
			newWorkload.AddFailureHandler(o.failureHandler)
		}
	}
	return &Node{unode: node}
}

// Edge adds a directed edge between two vertex as <from>-><to>
func (currentDag *Dag) Edge(from, to string, opts ...Option) {
	err := currentDag.udag.AddEdge(from, to)
	if err != nil {
		panic(fmt.Sprintf("Error at AddEdge for %s-%s, %v", from, to, err))
	}
	o := &ExecutionOptions{}
	for _, opt := range opts {
		o.reset()
		opt(o)
		if o.noForwarder == true {
			fromNode := currentDag.udag.GetNode(from)
			// Add a nil forwarder overriding the default forwarder
			fromNode.AddForwarder(to, nil)
		}

		// in case there is a override
		if o.forwarder != nil {
			fromNode := currentDag.udag.GetNode(from)
			fromNode.AddForwarder(to, o.forwarder)
		}
	}
}

// SubDag composites a separate dag as a node.
func (currentDag *Dag) SubDag(vertex string, dag *Dag) {
	node := currentDag.udag.AddVertex(vertex, []sdk.Operation{})
	err := node.AddSubDag(dag.udag)
	if err != nil {
		panic(fmt.Sprintf("Error at AddSubDag for %s, %v", vertex, err))
	}
	return
}

// ForEachBranch composites a sub-dag which executes for each value
// returned by ForEach function dynamically
// It returns the sub-dag that will be executed for each value
func (currentDag *Dag) ForEachBranch(vertex string, foreach sdk.ForEach, options ...Option) (dag *Dag) {
	node := currentDag.udag.AddVertex(vertex, []sdk.Operation{})
	if foreach == nil {
		panic(fmt.Sprintf("Error at AddForEachBranch for %s, foreach function not specified", vertex))
	}
	node.AddForEach(foreach)

	for _, option := range options {
		o := &ExecutionOptions{}
		o.reset()
		option(o)
		if o.aggregator != nil {
			node.AddSubAggregator(o.aggregator)
		}
		if o.noForwarder == true {
			node.AddForwarder("dynamic", nil)
		}
	}

	dag = NewDag()
	err := node.AddForEachDag(dag.udag)
	if err != nil {
		panic(fmt.Sprintf("Error at AddForEachBranch for %s, %v", vertex, err))
	}
	return
}

// ConditionalBranch composites multiple dags as a sub-dag which executes for each
// conditions returned by the Condition function dynamically
// It returns the set of dags based on the set of condition passed
func (currentDag *Dag) ConditionalBranch(vertex string, conditions []string, condition sdk.Condition,
	options ...Option) (conditionDags map[string]*Dag) {

	node := currentDag.udag.AddVertex(vertex, []sdk.Operation{})
	if condition == nil {
		panic(fmt.Sprintf("Error at AddConditionalBranch for %s, condition function not specified", vertex))
	}
	node.AddCondition(condition)

	for _, option := range options {
		o := &ExecutionOptions{}
		o.reset()
		option(o)
		if o.aggregator != nil {
			node.AddSubAggregator(o.aggregator)
		}
		if o.noForwarder == true {
			node.AddForwarder("dynamic", nil)
		}
	}
	conditionDags = make(map[string]*Dag)
	for _, conditionKey := range conditions {
		dag := NewDag()
		node.AddConditionalDag(conditionKey, dag.udag)
		conditionDags[conditionKey] = dag
	}
	return
}


// createWorkload Create a function with execution name
func createWorkload(id string, mod operation.Modifier) *operation.GoFlowOperation {
	operation := &operation.GoFlowOperation{}
	operation.Mod = mod
	operation.Id = id
	operation.Options = make(map[string][]string)
	return operation
}