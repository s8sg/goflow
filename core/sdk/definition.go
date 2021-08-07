package sdk

import (
	"encoding/json"
)

type DagExporter struct {
	Id               string                   `json:"id"`
	StartNode        string                   `json:"start-node"`
	EndNode          string                   `json:"end-node"`
	HasBranch        bool                     `json:"has-branch"`
	HasEdge          bool                     `json:"has-edge"`
	ExecutionOnlyDag bool                     `json:"exec-only-dag"`
	Nodes            map[string]*NodeExporter `json:"nodes"`

	IsValid         bool   `json:"is-valid"`
	ValidationError string `json:"validation-error,omitempty"`
}

type NodeExporter struct {
	Id       string `json:"id"`
	Index    int    `json:"node-index"`
	UniqueId string `json:"unique-id"` // required to fetch intermediate data and state

	IsDynamic        bool `json:"is-dynamic"`
	IsCondition      bool `json:"is-condition"`
	IsForeach        bool `json:"is-foreach"`
	HasAggregator     bool `json:"has-aggregator"`
	HasSubAggregator bool `json:"has-sub-aggregator"`
	HasSubDag        bool `json:"has-subdag"`
	InDegree         int  `json:"in-degree"`
	OutDegree        int  `json:"out-degree"`

	SubDag          *DagExporter            `json:"sub-dag,omitempty"`
	ForeachDag      *DagExporter            `json:"foreach-dag,omitempty"`
	ConditionalDags map[string]*DagExporter `json:"conditional-dags,omitempty"`
	DynamicExecOnly bool                    `json:"dynamic-exec-only"`
	Operations      []*OperationExporter    `json:"operations,omitempty"`

	Children         []string        `json:"childrens,omitempty"`
	ChildrenExecOnly map[string]bool `json:"child-exec-only"`
}

type OperationExporter struct {
	Name       string              `json:"name"`
	Properties map[string][]string `json:"properties"`
}

func exportOperation(exportOperation *OperationExporter, operation Operation) {
	exportOperation.Name = operation.GetId()
	exportOperation.Properties = operation.GetProperties()
}

func exportNode(exportNode *NodeExporter, node *Node) {
	exportNode.Id = node.Id
	exportNode.Index = node.index
	exportNode.UniqueId = node.uniqueId

	exportNode.IsDynamic = node.dynamic
	if node.GetCondition() != nil {
		exportNode.IsCondition = true
		if node.forwarder["dynamic"] == nil {
			exportNode.DynamicExecOnly = true
		}
		for condition, sdag := range node.conditionalDags {
			if exportNode.ConditionalDags == nil {
				exportNode.ConditionalDags = make(map[string]*DagExporter)
			}
			exportNode.ConditionalDags[condition] = &DagExporter{}
			exportDag(exportNode.ConditionalDags[condition], sdag)
		}
	}
	if node.GetForEach() != nil {
		exportNode.IsForeach = true
		exportNode.ForeachDag = &DagExporter{}
		exportDag(exportNode.ForeachDag, node.subDag)
		if node.forwarder["dynamic"] == nil {
			exportNode.DynamicExecOnly = true
		}
	}
	if node.aggregator != nil {
		exportNode.HasAggregator = true
	}
	if node.subAggregator != nil {
		exportNode.HasSubAggregator = true
	}
	if node.subDag != nil && !node.dynamic {
		exportNode.HasSubDag = true
		exportNode.SubDag = &DagExporter{}
		exportDag(exportNode.SubDag, node.subDag)
	}

	for _, operation := range node.operations {
		exportedOperation := &OperationExporter{}
		exportOperation(exportedOperation, operation)
		exportNode.Operations = append(exportNode.Operations, exportedOperation)
	}

	exportNode.ChildrenExecOnly = make(map[string]bool)
	for _, snode := range node.children {
		exportNode.Children = append(exportNode.Children, snode.Id)
		if node.forwarder[snode.Id] == nil {
			exportNode.ChildrenExecOnly[snode.Id] = true
		} else {
			exportNode.ChildrenExecOnly[snode.Id] = false
		}
	}
}

func exportDag(exportDag *DagExporter, dag *Dag) {
	exportDag.Id = dag.Id
	if dag.initialNode != nil {
		exportDag.StartNode = dag.initialNode.Id
	}
	if dag.endNode != nil {
		exportDag.EndNode = dag.endNode.Id
	}
	exportDag.HasBranch = dag.hasBranch
	exportDag.HasEdge = dag.hasEdge
	exportDag.ExecutionOnlyDag = dag.executionFlow

	for nodeId, node := range dag.nodes {
		if exportDag.Nodes == nil {
			exportDag.Nodes = make(map[string]*NodeExporter)
		}
		exportedNode := &NodeExporter{}
		exportNode(exportedNode, node)
		exportDag.Nodes[nodeId] = exportedNode
	}
}

// GetPipelineDefinition generate pipeline DAG defintion as a json
func GetPipelineDefinition(pipeline *Pipeline) string {
	root := &DagExporter{}

	// Validate the dag
	root.IsValid = true
	err := pipeline.Dag.Validate()
	if err != nil {
		root.IsValid = false
		root.ValidationError = err.Error()
	}

	exportDag(root, pipeline.Dag)
	encoded, _ := json.MarshalIndent(root, "", "    ")
	return string(encoded)
}
