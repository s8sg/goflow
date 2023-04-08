package lib

import (
	"encoding/json"
	"fmt"
	sdk "github.com/s8sg/faas-flow/sdk"
	"strings"
)

const (
	GRAPH_NODESPEC = 0
	GRAPH_RANKSPEC = 0
	GRAPH_PAD      = 1
	GRAPH_PACK     = 1

	NODE_CLUSTER_BORDER_COLOR = "grey"
	NODE_CLUSTER_STYLE        = "rounded"

	OPERATION_SHAPE = "rectangle"
	OPERATION_COLOR = "\"#68b8e2\""
	OPERATION_STYLE = "filled"

	EDGE_COLOR      = "\"#152730\""
	EXEC_EDGE_STYLE = "dotted"
	DATA_EDGE_STYLE = "solid"

	CONDITION_SHAPE = "diamond"
	CONDITION_STYLE = "filled"
	CONDITION_COLOR = "\"#f9af4d\""

	FOREACH_SHAPE = "diamond"
	FOREACH_STYLE = "filled"
	FOREACH_COLOR = "\"#f9af4d\""

	DYNAMIC_END_SHAPE = "invhouse"
	DYNAMIC_END_STYLE = "filled"
	DYNAMIC_END_COLOR = "pink"

	CONDITION_CLUSTER_BORDER_COLOR = "grey"
	CONDITION_CLUSTER_STYLE        = "rounded"
)

// generateOperationKey generate a unique key for an operation
func generateOperationKey(dagId string, nodeIndex int, opsIndex int, operation *sdk.OperationExporter, operationStr string) string {
	if operation != nil {
		switch {
		case operation.Properties["isFunction"][0] == "true":
			operationStr = "func-" + operation.Name
		case operation.Properties["isHttpRequest"][0] == "true":
			operationStr = "callback-" + operation.Name
		default:
			operationStr = "modifier"
		}
	}
	operationKey := ""
	if dagId != "0" {
		if opsIndex != 0 {
			operationKey = fmt.Sprintf("%s.%d.%d-%s", dagId, nodeIndex, opsIndex, operationStr)
		} else {
			operationKey = fmt.Sprintf("%s.%d-%s", dagId, nodeIndex, operationStr)
		}
	} else {
		operationKey = fmt.Sprintf("%d.%d-%s", nodeIndex, opsIndex, operationStr)
	}
	return operationKey
}

// generateOperationLebel generate a operation lebel
func generateOperationLebel(operation *sdk.OperationExporter) string {
	operationStr := ""
	if operation != nil {
		switch {
		case operation.Properties["isFunction"][0] == "true":
			operationStr = operation.Name
		case operation.Properties["isHttpRequest"][0] == "true":
			operationStr = operation.Name
		default:
			operationStr = "modifier"
		}
	}
	return operationStr
}

// generateConditionalDag generate dag element of a condition vertex
func generateConditionalDag(node *sdk.NodeExporter, dag *sdk.DagExporter, sb *strings.Builder, indent string) string {
	// Create a condition vertex
	conditionKey := generateOperationKey(dag.Id, node.Index, 0, nil, "conditions")
	sb.WriteString(fmt.Sprintf("\n%s\"%s\" [shape=%s style=%s color=%s label=\"condition\"];",
		indent, conditionKey, CONDITION_SHAPE, CONDITION_STYLE, CONDITION_COLOR))

	// Create a end operation vertex
	conditionEndKey := generateOperationKey(dag.Id, node.Index, 0, nil, "end")
	sb.WriteString(fmt.Sprintf("\n%s\"%s\" [shape=%s style=%s color=%s label=\"end\"];",
		indent, conditionEndKey, DYNAMIC_END_SHAPE, DYNAMIC_END_STYLE, DYNAMIC_END_COLOR))

	// Create condition graph
	for condition, conditionDag := range node.ConditionalDags {
		nextOperationDag := conditionDag
		startNodeId := nextOperationDag.StartNode
		nextOperationNode := nextOperationDag.Nodes[startNodeId]

		// Find out the next node with operation (recursively)
		for nextOperationNode.SubDag != nil && !nextOperationNode.IsDynamic {
			nextOperationDag = nextOperationNode.SubDag
			startNodeId := nextOperationDag.StartNode
			nextOperationNode = nextOperationDag.Nodes[startNodeId]
		}

		operationKey := ""
		if nextOperationNode.IsDynamic {
			if nextOperationNode.IsCondition {
				operationKey = generateOperationKey(nextOperationDag.Id, nextOperationNode.Index, 0, nil, "conditions")
			}
			if nextOperationNode.IsForeach {
				operationKey = generateOperationKey(nextOperationDag.Id, nextOperationNode.Index, 0, nil, "foreach")
			}
		} else {
			operation := nextOperationNode.Operations[0]
			operationKey = generateOperationKey(nextOperationDag.Id, nextOperationNode.Index, 1, operation, "")
		}

		edgeStyle := DATA_EDGE_STYLE
		if node.DynamicExecOnly {
			edgeStyle = EXEC_EDGE_STYLE
		}

		sb.WriteString(fmt.Sprintf("\n%s\"%s\" -> \"%s\" [label=%s color=%s style=%s];",
			indent, conditionKey, operationKey, condition, EDGE_COLOR, edgeStyle))

		sb.WriteString(fmt.Sprintf("\n%ssubgraph cluster_%s_%d_%s {", indent, dag.Id, node.Index, condition))

		sb.WriteString(fmt.Sprintf("\n%slabel=\"%s\";", indent+"\t", condition))
		sb.WriteString(fmt.Sprintf("\n%scolor=%s;", indent+"\t", CONDITION_CLUSTER_BORDER_COLOR))
		sb.WriteString(fmt.Sprintf("\n%sstyle=%s;\n", indent+"\t", CONDITION_CLUSTER_STYLE))
		sb.WriteString(fmt.Sprintf("\n%snodesep=%d;", indent+"\t", GRAPH_NODESPEC))
		sb.WriteString(fmt.Sprintf("\n%sranksep=%d;", indent+"\t", GRAPH_RANKSPEC))
		sb.WriteString(fmt.Sprintf("\n%spad=%d;", indent+"\t", GRAPH_PAD))
		sb.WriteString(fmt.Sprintf("\n%spack=%d;", indent+"\t", GRAPH_PACK))

		previousOperation := generateDag(conditionDag, sb, indent+"\t")

		sb.WriteString(fmt.Sprintf("\n%s}", indent))

		sb.WriteString(fmt.Sprintf("\n%s\"%s\" -> \"%s\" [color=%s style=%s];",
			indent, previousOperation, conditionEndKey, EDGE_COLOR, edgeStyle))
	}

	return conditionEndKey
}

// generateForeachDag generate dag element of a foreach vertex
func generateForeachDag(node *sdk.NodeExporter, dag *sdk.DagExporter, sb *strings.Builder, indent string) string {
	subdag := node.ForeachDag

	// Create a foreach operation vertex
	foreachKey := generateOperationKey(dag.Id, node.Index, 0, nil, "foreach")
	sb.WriteString(fmt.Sprintf("\n%s\"%s\" [shape=%s style=%s color=%s label=\"foreach\"];",
		indent, foreachKey, FOREACH_SHAPE, FOREACH_STYLE, FOREACH_COLOR))

	// Create a end operation vertex
	foreachEndKey := generateOperationKey(dag.Id, node.Index, 0, nil, "end")
	sb.WriteString(fmt.Sprintf("\n%s\"%s\" [shape=%s style=%s color=%s label=\"end\"];",
		indent, foreachEndKey, DYNAMIC_END_SHAPE, DYNAMIC_END_STYLE, DYNAMIC_END_COLOR))

	// Create Foreach Graph
	{
		nextOperationDag := subdag
		startNodeId := nextOperationDag.StartNode
		nextOperationNode := nextOperationDag.Nodes[startNodeId]

		// Find out the first operation on a subdag
		for nextOperationNode.SubDag != nil && !nextOperationNode.IsDynamic {
			nextOperationDag = nextOperationNode.SubDag
			startNodeId = nextOperationDag.StartNode
			nextOperationNode = nextOperationDag.Nodes[startNodeId]
		}

		operationKey := ""
		if nextOperationNode.IsDynamic {
			if nextOperationNode.IsCondition {
				operationKey = generateOperationKey(nextOperationDag.Id, nextOperationNode.Index, 0, nil, "conditions")
			}
			if nextOperationNode.IsForeach {
				operationKey = generateOperationKey(nextOperationDag.Id, nextOperationNode.Index, 0, nil, "foreach")
			}

		} else {
			operation := nextOperationNode.Operations[0]
			operationKey = generateOperationKey(nextOperationDag.Id, nextOperationNode.Index, 1, operation, "")
		}

		edgeStyle := DATA_EDGE_STYLE
		if node.DynamicExecOnly {
			edgeStyle = EXEC_EDGE_STYLE
		}

		sb.WriteString(fmt.Sprintf("\n%s\"%s\" -> \"%s\" [color=%s style=%s];",
			indent, foreachKey, operationKey, EDGE_COLOR, edgeStyle))

		sb.WriteString(fmt.Sprintf("\n%ssubgraph cluster_%s_%d {", indent, dag.Id, node.Index))

		sb.WriteString(fmt.Sprintf("\n%slabel=\"foreach\";", indent+"\t"))
		sb.WriteString(fmt.Sprintf("\n%scolor=%s;", indent+"\t", CONDITION_CLUSTER_BORDER_COLOR))
		sb.WriteString(fmt.Sprintf("\n%sstyle=%s;\n", indent+"\t", CONDITION_CLUSTER_STYLE))
		sb.WriteString(fmt.Sprintf("\n%snodesep=%d;", indent+"\t", GRAPH_NODESPEC))
		sb.WriteString(fmt.Sprintf("\n%sranksep=%d;", indent+"\t", GRAPH_RANKSPEC))
		sb.WriteString(fmt.Sprintf("\n%spad=%d;", indent+"\t", GRAPH_PAD))
		sb.WriteString(fmt.Sprintf("\n%spack=%d;", indent+"\t", GRAPH_PACK))

		previousOperation := generateDag(subdag, sb, indent+"\t")

		sb.WriteString(fmt.Sprintf("\n%s}", indent))

		sb.WriteString(fmt.Sprintf("\n%s\"%s\" -> \"%s\" [color=%s style=%s];",
			indent, previousOperation, foreachEndKey, EDGE_COLOR, edgeStyle))
	}

	return foreachEndKey
}

// generateDag populate a string buffer for a dag and returns the last operation ID
func generateDag(dag *sdk.DagExporter, sb *strings.Builder, indent string) string {
	lastOperation := ""
	// gener/ate nodes
	for _, node := range dag.Nodes {

		previousOperation := ""

		if node.IsDynamic {
			// Handle dynamic node
			if node.IsCondition {
				previousOperation = generateConditionalDag(node, dag, sb, indent)
			}
			if node.IsForeach {
				previousOperation = generateForeachDag(node, dag, sb, indent)
			}
		} else {
			// Handle non dynamic node

			sb.WriteString(fmt.Sprintf("\n%ssubgraph cluster_%s_%d {", indent, dag.Id, node.Index))
			sb.WriteString(fmt.Sprintf("\n%snodesep=%d;", indent, GRAPH_NODESPEC))
			sb.WriteString(fmt.Sprintf("\n%sranksep=%d;", indent, GRAPH_RANKSPEC))
			sb.WriteString(fmt.Sprintf("\n%spad=%d;", indent, GRAPH_PAD))
			sb.WriteString(fmt.Sprintf("\n%spack=%d;", indent, GRAPH_PACK))

			nodeIndexStr := fmt.Sprintf("%d", node.Index-1)

			if nodeIndexStr != node.Id {
				sb.WriteString(fmt.Sprintf("\n%slabel=\"%s\";", indent+"\t", node.Id))
			} else {
				sb.WriteString(fmt.Sprintf("\n%slabel=\"%s\";", indent+"\t", nodeIndexStr))
			}

			sb.WriteString(fmt.Sprintf("\n%scolor=%s;", indent+"\t", NODE_CLUSTER_BORDER_COLOR))
			sb.WriteString(fmt.Sprintf("\n%sstyle=%s;\n", indent+"\t", NODE_CLUSTER_STYLE))
		}

		subdag := node.SubDag
		if subdag != nil {
			previousOperation = generateDag(subdag, sb, indent+"\t")
		} else {
			for opsindex, operation := range node.Operations {
				operationKey := generateOperationKey(dag.Id, node.Index, opsindex+1, operation, "")
				operationLebel := generateOperationLebel(operation)
				sb.WriteString(fmt.Sprintf("\n%s\"%s\" [shape=%s color=%s style=%s label=\"%s\"];",
					indent+"\t", operationKey, OPERATION_SHAPE, OPERATION_COLOR, OPERATION_STYLE, operationLebel))

				// Operations always forwards data
				if previousOperation != "" {
					sb.WriteString(fmt.Sprintf("\n%s\"%s\" -> \"%s\" [color=%s style=%s];",
						indent+"\t", previousOperation, operationKey, EDGE_COLOR, DATA_EDGE_STYLE))
				}
				previousOperation = operationKey
			}
		}

		// If noce is not dynamic close the subgraph cluster
		if !node.IsDynamic {
			sb.WriteString(fmt.Sprintf("\n%s}\n", indent))
		}

		// If node has children
		if node.Children != nil {
			for _, childId := range node.Children {

				var operation *sdk.OperationExporter

				// get child node
				child := dag.Nodes[childId]

				nextOperationNode := child
				nextOperationDag := dag

				// Find out the next node with operation (recursively)
				for nextOperationNode.SubDag != nil && !nextOperationNode.IsDynamic {
					nextOperationDag = nextOperationNode.SubDag
					nextOperationNodeId := nextOperationDag.StartNode
					nextOperationNode = nextOperationDag.Nodes[nextOperationNodeId]
				}

				childOperationKey := ""
				if nextOperationNode.IsDynamic {
					if nextOperationNode.IsCondition {
						childOperationKey = generateOperationKey(nextOperationDag.Id, nextOperationNode.Index, 0, nil, "conditions")
					}
					if nextOperationNode.IsForeach {
						childOperationKey = generateOperationKey(nextOperationDag.Id, nextOperationNode.Index, 0, nil, "foreach")
					}

				} else {
					operation = nextOperationNode.Operations[0]
					childOperationKey = generateOperationKey(nextOperationDag.Id, nextOperationNode.Index, 1, operation, "")
				}

				edgeStyle := DATA_EDGE_STYLE
				if node.ChildrenExecOnly[childId] {
					edgeStyle = EXEC_EDGE_STYLE
				}

				if previousOperation != "" {
					sb.WriteString(fmt.Sprintf("\n%s\"%s\" -> \"%s\" [color=%s style=%s];",
						indent, previousOperation, childOperationKey, EDGE_COLOR, edgeStyle))
				}
			}
		} else {
			lastOperation = previousOperation
		}

		sb.WriteString("\n")
	}
	return lastOperation
}

// makeDotFromDefinition make dot graph by iterating each node in greedy approach
func makeDotFromDefinition(root *sdk.DagExporter) string {
	var sb strings.Builder

	indent := "\t"
	sb.WriteString("digraph depgraph {")
	sb.WriteString(fmt.Sprintf("\n%srankdir=TD;", indent))
	sb.WriteString(fmt.Sprintf("\n%snodesep=%d;", indent, GRAPH_NODESPEC))
	sb.WriteString(fmt.Sprintf("\n%sranksep=%d;", indent, GRAPH_RANKSPEC))
	sb.WriteString(fmt.Sprintf("\n%spad=%d;", indent, GRAPH_PAD))
	sb.WriteString(fmt.Sprintf("\n%spack=%d;", indent, GRAPH_PACK))
	sb.WriteString(fmt.Sprintf("\n%ssplines=curved;", indent))
	sb.WriteString(fmt.Sprintf("\n%sfontname=\"Courier New\";", indent))
	sb.WriteString(fmt.Sprintf("\n%sfontcolor=\"#44413b\";", indent))

	sb.WriteString(fmt.Sprintf("\n%snode [style=filled fontname=\"Courier\" fontcolor=black]\n", indent))

	generateDag(root, &sb, indent)

	sb.WriteString("}\n")
	return sb.String()
}

func MakeDotFromDefinitionString(definition string) (string, error) {
	flowDefition := &sdk.DagExporter{}
	err := json.Unmarshal([]byte(definition), flowDefition)
	if err != nil {
		return "", err
	}
	return makeDotFromDefinition(flowDefition), nil
}
