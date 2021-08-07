package sdk

import (
	"fmt"
)

var (
	// ERR_NO_VERTEX
	ERR_NO_VERTEX = fmt.Errorf("dag has no vertex set")
	// ERR_CYCLIC denotes that dag has a cycle
	ERR_CYCLIC = fmt.Errorf("dag has cyclic dependency")
	// ERR_DUPLICATE_EDGE denotes that a dag edge is duplicate
	ERR_DUPLICATE_EDGE = fmt.Errorf("edge redefined")
	// ERR_DUPLICATE_VERTEX denotes that a dag edge is duplicate
	ERR_DUPLICATE_VERTEX = fmt.Errorf("vertex redefined")
	// ERR_MULTIPLE_START denotes that a dag has more than one start point
	ERR_MULTIPLE_START = fmt.Errorf("only one start vertex is allowed")
	// ERR_RECURSIVE_DEP denotes that dag has a recursive dependecy
	ERR_RECURSIVE_DEP = fmt.Errorf("dag has recursive dependency")
	// Default forwarder
	DefaultForwarder = func(data []byte) []byte { return data }
)

// Aggregator definition for the data aggregator of nodes
type Aggregator func(map[string][]byte) ([]byte, error)

// Forwarder definition for the data forwarder of nodes
type Forwarder func([]byte) []byte

// ForEach definition for the foreach function
type ForEach func([]byte) map[string][]byte

// Condition definition for the condition function
type Condition func([]byte) []string

// Dag The whole dag
type Dag struct {
	Id    string
	nodes map[string]*Node // the nodes in a dag

	parentNode *Node // In case the dag is a sub dag the node reference

	initialNode *Node // The start of a valid dag
	endNode     *Node // The end of a valid dag
	hasBranch   bool  // denotes the dag or its subdag has a branch
	hasEdge     bool  // denotes the dag or its subdag has edge
	validated   bool  // denotes the dag has been validated

	executionFlow      bool // Flag to denote if none of the node forwards data
	dataForwarderCount int  // Count of nodes that forwards data

	nodeIndex int // NodeIndex
}

// Node The vertex
type Node struct {
	Id       string // The id of the vertex
	index    int    // The index of the vertex
	uniqueId string // The unique Id of the node

	// Execution modes ([]operation / Dag)
	subDag          *Dag            // Subdag
	conditionalDags map[string]*Dag // Conditional subdags
	operations      []Operation     // The list of operations

	dynamic       bool                 // Denotes if the node is dynamic
	aggregator    Aggregator           // The aggregator aggregates multiple inputs to a node into one
	foreach       ForEach              // If specified foreach allows to execute the vertex in parralel
	condition     Condition            // If specified condition allows to execute only selected subdag
	subAggregator Aggregator           // Aggregates foreach/condition outputs into one
	forwarder     map[string]Forwarder // The forwarder handle forwarding output to a children

	parentDag       *Dag    // The reference of the dag this node part of
	indegree        int     // The vertex dag indegree
	dynamicIndegree int     // The vertex dag dynamic indegree
	outdegree       int     // The vertex dag outdegree
	children        []*Node // The children of the vertex
	dependsOn       []*Node // The parents of the vertex

	next []*Node
	prev []*Node
}

// NewDag Creates a Dag
func NewDag() *Dag {
	this := new(Dag)
	this.nodes = make(map[string]*Node)
	this.Id = "0"
	this.executionFlow = true
	return this
}

// Append appends another dag into an existing dag
// Its a way to define and reuse subdags
// append causes disconnected dag which must be linked with edge in order to execute
func (this *Dag) Append(dag *Dag) error {
	for nodeId, node := range dag.nodes {
		_, duplicate := this.nodes[nodeId]
		if duplicate {
			return ERR_DUPLICATE_VERTEX
		}
		// add the node
		this.nodes[nodeId] = node
	}
	return nil
}

// AddVertex create a vertex with id and operations
func (this *Dag) AddVertex(id string, operations []Operation) *Node {

	node := &Node{Id: id, operations: operations, index: this.nodeIndex + 1}
	node.forwarder = make(map[string]Forwarder, 0)
	node.parentDag = this
	this.nodeIndex = this.nodeIndex + 1
	this.nodes[id] = node
	return node
}

// AddEdge add a directed edge as (from)->(to)
// If vertex doesn't exists creates them
func (this *Dag) AddEdge(from, to string) error {
	fromNode := this.nodes[from]
	if fromNode == nil {
		fromNode = this.AddVertex(from, []Operation{})
	}
	toNode := this.nodes[to]
	if toNode == nil {
		toNode = this.AddVertex(to, []Operation{})
	}

	// CHeck if duplicate (TODO: Check if one way check is enough)
	if toNode.inSlice(fromNode.children) || fromNode.inSlice(toNode.dependsOn) {
		return ERR_DUPLICATE_EDGE
	}

	// Check if cyclic dependency (TODO: Check if one way check if enough)
	if fromNode.inSlice(toNode.next) || toNode.inSlice(fromNode.prev) {
		return ERR_CYCLIC
	}

	// Update references recursively
	fromNode.next = append(fromNode.next, toNode)
	fromNode.next = append(fromNode.next, toNode.next...)
	for _, b := range fromNode.prev {
		b.next = append(b.next, toNode)
		b.next = append(b.next, toNode.next...)
	}

	// Update references recursively
	toNode.prev = append(toNode.prev, fromNode)
	toNode.prev = append(toNode.prev, fromNode.prev...)
	for _, b := range toNode.next {
		b.prev = append(b.prev, fromNode)
		b.prev = append(b.prev, fromNode.prev...)
	}

	fromNode.children = append(fromNode.children, toNode)
	toNode.dependsOn = append(toNode.dependsOn, fromNode)
	toNode.indegree++
	if fromNode.Dynamic() {
		toNode.dynamicIndegree++
	}
	fromNode.outdegree++

	// Add default forwarder for from node
	fromNode.AddForwarder(to, DefaultForwarder)

	// set has branch property
	if toNode.indegree > 1 || fromNode.outdegree > 1 {
		this.hasBranch = true
	}

	this.hasEdge = true

	return nil
}

// GetNode get a node by Id
func (this *Dag) GetNode(id string) *Node {
	return this.nodes[id]
}

// GetParentNode returns parent node for a subdag
func (this *Dag) GetParentNode() *Node {
	return this.parentNode
}

// GetInitialNode gets the initial node
func (this *Dag) GetInitialNode() *Node {
	return this.initialNode
}

// GetEndNode gets the end node
func (this *Dag) GetEndNode() *Node {
	return this.endNode
}

// HasBranch check if dag or its subdags has branch
func (this *Dag) HasBranch() bool {
	return this.hasBranch
}

// HasEdge check if dag or its subdags has edge
func (this *Dag) HasEdge() bool {
	return this.hasEdge
}

// Validate validates a dag and all subdag as per faas-flow dag requirments
// A validated graph has only one initialNode and one EndNode set
// if a graph has more than one endnode, a seperate endnode gets added
func (this *Dag) Validate() error {
	initialNodeCount := 0
	var endNodes []*Node

	if this.validated {
		return nil
	}

	if len(this.nodes) == 0 {
		return ERR_NO_VERTEX
	}

	for _, b := range this.nodes {
		b.uniqueId = b.generateUniqueId(this.Id)
		if b.indegree == 0 {
			initialNodeCount = initialNodeCount + 1
			this.initialNode = b
		}
		if b.outdegree == 0 {
			endNodes = append(endNodes, b)
		}
		if b.subDag != nil {
			if this.Id != "0" {
				// Dag Id : <parent-dag-id>_<parent-node-unique-id>
				b.subDag.Id = fmt.Sprintf("%s_%d", this.Id, b.index)
			} else {
				// Dag Id : <parent-node-unique-id>
				b.subDag.Id = fmt.Sprintf("%d", b.index)
			}

			err := b.subDag.Validate()
			if err != nil {
				return err
			}

			if b.subDag.hasBranch {
				this.hasBranch = true
			}

			if b.subDag.hasEdge {
				this.hasEdge = true
			}

			if !b.subDag.executionFlow {
				//  Subdag have data edge
				this.executionFlow = false
			}
		}
		if b.dynamic && b.forwarder["dynamic"] != nil {
			this.executionFlow = false
		}
		for condition, cdag := range b.conditionalDags {
			if this.Id != "0" {
				// Dag Id : <parent-dag-id>_<parent-node-unique-id>_<condition_key>
				cdag.Id = fmt.Sprintf("%s_%d_%s", this.Id, b.index, condition)
			} else {
				// Dag Id : <parent-node-unique-id>_<condition_key>
				cdag.Id = fmt.Sprintf("%d_%s", b.index, condition)
			}

			err := cdag.Validate()
			if err != nil {
				return err
			}

			if cdag.hasBranch {
				this.hasBranch = true
			}

			if cdag.hasEdge {
				this.hasEdge = true
			}

			if !cdag.executionFlow {
				// Subdag have data edge
				this.executionFlow = false
			}
		}
	}

	if initialNodeCount > 1 {
		return fmt.Errorf("%v, dag: %s", ERR_MULTIPLE_START, this.Id)
	}

	// If there is multiple ends add a virtual end node to combine them
	if len(endNodes) > 1 {
		endNodeId := fmt.Sprintf("end_%s", this.Id)
		blank := &BlankOperation{}
		endNode := this.AddVertex(endNodeId, []Operation{blank})
		for _, b := range endNodes {
			// Create a edge
			this.AddEdge(b.Id, endNodeId)
			// mark the edge as execution dependency
			b.AddForwarder(endNodeId, nil)
		}
		this.endNode = endNode
	} else {
		this.endNode = endNodes[0]
	}

	this.validated = true

	return nil
}

// GetNodes returns a list of nodes (including subdags) belong to the dag
func (this *Dag) GetNodes(dynamicOption string) []string {
	var nodes []string
	for _, b := range this.nodes {
		nodeId := ""
		if dynamicOption == "" {
			nodeId = b.GetUniqueId()
		} else {
			nodeId = b.GetUniqueId() + "_" + dynamicOption
		}
		nodes = append(nodes, nodeId)
		// excludes the dynamic subdag
		if b.dynamic {
			continue
		}
		if b.subDag != nil {
			subDagNodes := b.subDag.GetNodes(dynamicOption)
			nodes = append(nodes, subDagNodes...)
		}
	}
	return nodes
}

// IsExecutionFlow check if a dag doesn't use intermediate data
func (this *Dag) IsExecutionFlow() bool {
	return this.executionFlow
}

// inSlice check if a node belongs in a slice
func (this *Node) inSlice(list []*Node) bool {
	for _, b := range list {
		if b.Id == this.Id {
			return true
		}
	}
	return false
}

// Children get all children node for a node
func (this *Node) Children() []*Node {
	return this.children
}

// Dependency get all dependency node for a node
func (this *Node) Dependency() []*Node {
	return this.dependsOn
}

// Value provides the ordered list of functions for a node
func (this *Node) Operations() []Operation {
	return this.operations
}

// Indegree returns the no of input in a node
func (this *Node) Indegree() int {
	return this.indegree
}

// DynamicIndegree returns the no of dynamic input in a node
func (this *Node) DynamicIndegree() int {
	return this.dynamicIndegree
}

// Outdegree returns the no of output in a node
func (this *Node) Outdegree() int {
	return this.outdegree
}

// SubDag returns the subdag added in a node
func (this *Node) SubDag() *Dag {
	return this.subDag
}

// Dynamic checks if the node is dynamic
func (this *Node) Dynamic() bool {
	return this.dynamic
}

// ParentDag returns the parent dag of the node
func (this *Node) ParentDag() *Dag {
	return this.parentDag
}

// AddOperation adds an operation
func (this *Node) AddOperation(operation Operation) {
	this.operations = append(this.operations, operation)
}

// AddAggregator add a aggregator to a node
func (this *Node) AddAggregator(aggregator Aggregator) {
	this.aggregator = aggregator
}

// AddForEach add a aggregator to a node
func (this *Node) AddForEach(foreach ForEach) {
	this.foreach = foreach
	this.dynamic = true
	this.AddForwarder("dynamic", DefaultForwarder)
}

// AddCondition add a condition to a node
func (this *Node) AddCondition(condition Condition) {
	this.condition = condition
	this.dynamic = true
	this.AddForwarder("dynamic", DefaultForwarder)
}

// AddSubAggregator add a foreach aggregator to a node
func (this *Node) AddSubAggregator(aggregator Aggregator) {
	this.subAggregator = aggregator
}

// AddForwarder adds a forwarder for a specific children
func (this *Node) AddForwarder(children string, forwarder Forwarder) {
	this.forwarder[children] = forwarder
	if forwarder != nil {
		this.parentDag.dataForwarderCount = this.parentDag.dataForwarderCount + 1
		this.parentDag.executionFlow = false
	} else {
		this.parentDag.dataForwarderCount = this.parentDag.dataForwarderCount - 1
		if this.parentDag.dataForwarderCount == 0 {
			this.parentDag.executionFlow = true
		}
	}
}

// AddSubDag adds a subdag to the node
func (this *Node) AddSubDag(subDag *Dag) error {
	parentDag := this.parentDag
	// Continue till there is no parent dag
	for parentDag != nil {
		// check if recursive inclusion
		if parentDag == subDag {
			return ERR_RECURSIVE_DEP
		}
		// Check if the parent dag is a subdag and has a parent node
		parentNode := parentDag.parentNode
		if parentNode != nil {
			// If a subdag, move to the parent dag
			parentDag = parentNode.parentDag
			continue
		}
		break
	}
	// Set the subdag in the node
	this.subDag = subDag
	// Set the node the subdag belongs to
	subDag.parentNode = this

	return nil
}

// AddForEachDag adds a foreach subdag to the node
func (this *Node) AddForEachDag(subDag *Dag) error {
	// Set the subdag in the node
	this.subDag = subDag
	// Set the node the subdag belongs to
	subDag.parentNode = this

	this.parentDag.hasBranch = true
	this.parentDag.hasEdge = true

	return nil
}

// AddConditionalDag adds conditional dag to node
func (this *Node) AddConditionalDag(condition string, dag *Dag) {
	// Set the conditional subdag in the node
	if this.conditionalDags == nil {
		this.conditionalDags = make(map[string]*Dag)
	}
	this.conditionalDags[condition] = dag
	// Set the node the subdag belongs to
	dag.parentNode = this

	this.parentDag.hasBranch = true
	this.parentDag.hasEdge = true
}

// GetAggregator get a aggregator from a node
func (this *Node) GetAggregator() Aggregator {
	return this.aggregator
}

// GetForwarder gets a forwarder for a children
func (this *Node) GetForwarder(children string) Forwarder {
	return this.forwarder[children]
}

// GetSubAggregator gets the subaggregator for condition and foreach
func (this *Node) GetSubAggregator() Aggregator {
	return this.subAggregator
}

// GetCondition get the condition function
func (this *Node) GetCondition() Condition {
	return this.condition
}

// GetForEach get the foreach function
func (this *Node) GetForEach() ForEach {
	return this.foreach
}

// GetAllConditionalDags get all the subdags for all conditions
func (this *Node) GetAllConditionalDags() map[string]*Dag {
	return this.conditionalDags
}

// GetConditionalDag get the sundag for a specific condition
func (this *Node) GetConditionalDag(condition string) *Dag {
	if this.conditionalDags == nil {
		return nil
	}
	return this.conditionalDags[condition]
}

// generateUniqueId returns a unique ID of node throughout the DAG
func (this *Node) generateUniqueId(dagId string) string {
	// Node Id : <dag-id>_<node_index_in_dag>_<node_id>
	return fmt.Sprintf("%s_%d_%s", dagId, this.index, this.Id)
}

// GetUniqueId returns a unique ID of the node
func (this *Node) GetUniqueId() string {
	return this.uniqueId
}
