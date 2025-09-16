package sdk

import (
	"encoding/json"
	"fmt"
)

var (
	// ERR_NO_VERTEX
	ErrNoVertex = fmt.Errorf("dag has no vertex set")
	// ERR_CYCLIC denotes that dag has a cycle
	ErrCyclic = fmt.Errorf("dag has cyclic dependency")
	// ERR_DUPLICATE_EDGE denotes that a dag edge is duplicate
	ErrDuplicateEdge = fmt.Errorf("edge redefined")
	// ERR_DUPLICATE_VERTEX denotes that a dag edge is duplicate
	ErrDuplicateVertex = fmt.Errorf("vertex redefined")
	// ERR_MULTIPLE_START denotes that a dag has more than one start point
	ErrMultipleStart = fmt.Errorf("only one start vertex is allowed")
	// ERR_RECURSIVE_DEP denotes that dag has a recursive dependecy
	ErrRecursiveDep = fmt.Errorf("dag has recursive dependency")
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
	foreach       ForEach              // If specified foreach allows to execute the vertex in parallel
	condition     Condition            // If specified condition allows to execute only selected sub-dag
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
func (d *Dag) Append(dag *Dag) error {
	for nodeId, node := range dag.nodes {
		_, duplicate := d.nodes[nodeId]
		if duplicate {
			return ErrDuplicateVertex
		}
		// add the node
		d.nodes[nodeId] = node
	}
	return nil
}

// AddVertex create a vertex with id and operations
func (d *Dag) AddVertex(id string, operations []Operation) *Node {

	node := &Node{Id: id, operations: operations, index: d.nodeIndex + 1}
	node.forwarder = make(map[string]Forwarder, 0)
	node.parentDag = d
	d.nodeIndex = d.nodeIndex + 1
	d.nodes[id] = node
	return node
}

// AddEdge add a directed edge as (from)->(to)
// If vertex doesn't exists creates them
func (d *Dag) AddEdge(from, to string) error {
	fromNode := d.nodes[from]
	if fromNode == nil {
		fromNode = d.AddVertex(from, []Operation{})
	}
	toNode := d.nodes[to]
	if toNode == nil {
		toNode = d.AddVertex(to, []Operation{})
	}

	// CHeck if duplicate (TODO: Check if one way check is enough)
	if toNode.inSlice(fromNode.children) || fromNode.inSlice(toNode.dependsOn) {
		return ErrDuplicateEdge
	}

	// Check if cyclic dependency (TODO: Check if one way check if enough)
	if fromNode.inSlice(toNode.next) || toNode.inSlice(fromNode.prev) {
		return ErrCyclic
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
		d.hasBranch = true
	}

	d.hasEdge = true

	return nil
}

// GetNode get a node by Id
func (d *Dag) GetNode(id string) *Node {
	return d.nodes[id]
}

// GetParentNode returns parent node for a subdag
func (d *Dag) GetParentNode() *Node {
	return d.parentNode
}

// GetInitialNode gets the initial node
func (d *Dag) GetInitialNode() *Node {
	return d.initialNode
}

// GetEndNode gets the end node
func (d *Dag) GetEndNode() *Node {
	return d.endNode
}

// HasBranch check if dag or its sub-dags has branch
func (d *Dag) HasBranch() bool {
	return d.hasBranch
}

// HasEdge check if dag or its sub-dags has edge
func (d *Dag) HasEdge() bool {
	return d.hasEdge
}

// Validate validates a dag and all sub-dag as per faas-flow dag requirements
// A validated graph has only one initialNode and one EndNode set
// if a graph has more than one end-node, a separate end-node gets added
func (d *Dag) Validate() error {
	initialNodeCount := 0
	var endNodes []*Node

	if d.validated {
		return nil
	}

	if len(d.nodes) == 0 {
		return ErrNoVertex
	}

	for _, b := range d.nodes {
		b.uniqueId = b.generateUniqueId(d.Id)
		if b.indegree == 0 {
			initialNodeCount = initialNodeCount + 1
			d.initialNode = b
		}
		if b.outdegree == 0 {
			endNodes = append(endNodes, b)
		}
		if b.subDag != nil {
			if d.Id != "0" {
				// Dag Id : <parent-dag-id>_<parent-node-unique-id>
				b.subDag.Id = fmt.Sprintf("%s_%d", d.Id, b.index)
			} else {
				// Dag Id : <parent-node-unique-id>
				b.subDag.Id = fmt.Sprintf("%d", b.index)
			}

			err := b.subDag.Validate()
			if err != nil {
				return err
			}

			if b.subDag.hasBranch {
				d.hasBranch = true
			}

			if b.subDag.hasEdge {
				d.hasEdge = true
			}

			if !b.subDag.executionFlow {
				//  Subdag have data edge
				d.executionFlow = false
			}
		}
		if b.dynamic && b.forwarder["dynamic"] != nil {
			d.executionFlow = false
		}
		for condition, cdag := range b.conditionalDags {
			if d.Id != "0" {
				// Dag Id : <parent-dag-id>_<parent-node-unique-id>_<condition_key>
				cdag.Id = fmt.Sprintf("%s_%d_%s", d.Id, b.index, condition)
			} else {
				// Dag Id : <parent-node-unique-id>_<condition_key>
				cdag.Id = fmt.Sprintf("%d_%s", b.index, condition)
			}

			err := cdag.Validate()
			if err != nil {
				return err
			}

			if cdag.hasBranch {
				d.hasBranch = true
			}

			if cdag.hasEdge {
				d.hasEdge = true
			}

			if !cdag.executionFlow {
				// Subdag have data edge
				d.executionFlow = false
			}
		}
	}

	if initialNodeCount > 1 {
		return fmt.Errorf("%v, dag: %s", ErrMultipleStart, d.Id)
	}

	// If there is multiple ends add a virtual end node to combine them
	if len(endNodes) > 1 {
		endNodeId := fmt.Sprintf("end_%s", d.Id)
		blank := &BlankOperation{}
		endNode := d.AddVertex(endNodeId, []Operation{blank})
		for _, b := range endNodes {
			// Create a edge
			err := d.AddEdge(b.Id, endNodeId)
			if err != nil {
				d.validated = false
				break
			}
			// mark the edge as execution dependency
			b.AddForwarder(endNodeId, nil)
		}
		d.endNode = endNode
	} else {
		d.endNode = endNodes[0]
	}

	d.validated = true

	return nil
}

// GetNodes returns a list of nodes (including subdags) belong to the dag
func (d *Dag) GetNodes(dynamicOption string) []string {
	var nodes []string
	for _, b := range d.nodes {
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
func (d *Dag) IsExecutionFlow() bool {
	return d.executionFlow
}

// GetDefinitionJson generate DAG definition as a json
func (dag *Dag) GetDefinitionJson() ([]byte, error) {
	root := &DagExporter{}

	// Validate the dag
	root.IsValid = true
	err := dag.Validate()
	if err != nil {
		root.IsValid = false
		root.ValidationError = err.Error()
	}

	exportDag(root, dag)
	encoded, err := json.MarshalIndent(root, "", "    ")
	return encoded, err
}

// GetDefinition represent DAG definition with exporter
func (dag *Dag) GetDefinition() (*DagExporter, error) {
	root := &DagExporter{}

	// Validate the dag
	root.IsValid = true
	err := dag.Validate()
	if err != nil {
		root.IsValid = false
		root.ValidationError = err.Error()
	}

	exportDag(root, dag)
	return root, err
}

// inSlice check if a node belongs in a slice
func (n *Node) inSlice(list []*Node) bool {
	for _, b := range list {
		if b.Id == n.Id {
			return true
		}
	}
	return false
}

// Children get all children node for a node
func (n *Node) Children() []*Node {
	return n.children
}

// Dependency get all dependency node for a node
func (n *Node) Dependency() []*Node {
	return n.dependsOn
}

// Value provides the ordered list of functions for a node
func (n *Node) Operations() []Operation {
	return n.operations
}

// Indegree returns the no of input in a node
func (n *Node) Indegree() int {
	return n.indegree
}

// DynamicIndegree returns the no of dynamic input in a node
func (n *Node) DynamicIndegree() int {
	return n.dynamicIndegree
}

// Outdegree returns the no of output in a node
func (n *Node) Outdegree() int {
	return n.outdegree
}

// SubDag returns the subdag added in a node
func (n *Node) SubDag() *Dag {
	return n.subDag
}

// Dynamic checks if the node is dynamic
func (n *Node) Dynamic() bool {
	return n.dynamic
}

// ParentDag returns the parent dag of the node
func (n *Node) ParentDag() *Dag {
	return n.parentDag
}

// AddOperation adds an operation
func (n *Node) AddOperation(operation Operation) {
	n.operations = append(n.operations, operation)
}

// AddAggregator add a aggregator to a node
func (n *Node) AddAggregator(aggregator Aggregator) {
	n.aggregator = aggregator
}

// AddForEach add a aggregator to a node
func (n *Node) AddForEach(foreach ForEach) {
	n.foreach = foreach
	n.dynamic = true
	n.AddForwarder("dynamic", DefaultForwarder)
}

// AddCondition add a condition to a node
func (n *Node) AddCondition(condition Condition) {
	n.condition = condition
	n.dynamic = true
	n.AddForwarder("dynamic", DefaultForwarder)
}

// AddSubAggregator add a foreach aggregator to a node
func (n *Node) AddSubAggregator(aggregator Aggregator) {
	n.subAggregator = aggregator
}

// AddForwarder adds a forwarder for a specific children
func (n *Node) AddForwarder(children string, forwarder Forwarder) {
	n.forwarder[children] = forwarder
	if forwarder != nil {
		n.parentDag.dataForwarderCount = n.parentDag.dataForwarderCount + 1
		n.parentDag.executionFlow = false
	} else {
		n.parentDag.dataForwarderCount = n.parentDag.dataForwarderCount - 1
		if n.parentDag.dataForwarderCount == 0 {
			n.parentDag.executionFlow = true
		}
	}
}

// AddSubDag adds a subdag to the node
func (n *Node) AddSubDag(subDag *Dag) error {
	parentDag := n.parentDag
	// Continue till there is no parent dag
	for parentDag != nil {
		// check if recursive inclusion
		if parentDag == subDag {
			return ErrRecursiveDep
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
	n.subDag = subDag
	// Set the node the subdag belongs to
	subDag.parentNode = n

	return nil
}

// AddForEachDag adds a foreach subdag to the node
func (n *Node) AddForEachDag(subDag *Dag) error {
	// Set the subdag in the node
	n.subDag = subDag
	// Set the node the subdag belongs to
	subDag.parentNode = n

	n.parentDag.hasBranch = true
	n.parentDag.hasEdge = true

	return nil
}

// AddConditionalDag adds conditional dag to node
func (n *Node) AddConditionalDag(condition string, dag *Dag) {
	// Set the conditional subdag in the node
	if n.conditionalDags == nil {
		n.conditionalDags = make(map[string]*Dag)
	}
	n.conditionalDags[condition] = dag
	// Set the node the subdag belongs to
	dag.parentNode = n

	n.parentDag.hasBranch = true
	n.parentDag.hasEdge = true
}

// GetAggregator get a aggregator from a node
func (n *Node) GetAggregator() Aggregator {
	return n.aggregator
}

// GetForwarder gets a forwarder for a children
func (n *Node) GetForwarder(children string) Forwarder {
	return n.forwarder[children]
}

// GetSubAggregator gets the subaggregator for condition and foreach
func (n *Node) GetSubAggregator() Aggregator {
	return n.subAggregator
}

// GetCondition get the condition function
func (n *Node) GetCondition() Condition {
	return n.condition
}

// GetForEach get the foreach function
func (n *Node) GetForEach() ForEach {
	return n.foreach
}

// GetAllConditionalDags get all the subdags for all conditions
func (n *Node) GetAllConditionalDags() map[string]*Dag {
	return n.conditionalDags
}

// GetConditionalDag get the sundag for a specific condition
func (n *Node) GetConditionalDag(condition string) *Dag {
	if n.conditionalDags == nil {
		return nil
	}
	return n.conditionalDags[condition]
}

// generateUniqueId returns a unique ID of node throughout the DAG
func (n *Node) generateUniqueId(dagId string) string {
	// Node Id : <dag-id>_<node_index_in_dag>_<node_id>
	return fmt.Sprintf("%s_%d_%s", dagId, n.index, n.Id)
}

// GetUniqueId returns a unique ID of the node
func (n *Node) GetUniqueId() string {
	return n.uniqueId
}
