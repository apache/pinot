/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
import React from "react";
import ReactFlow, { Background, Controls, MiniMap, Handle, Node, Edge } from "react-flow-renderer";
import dagre from "dagre";
import { Typography, useTheme } from "@material-ui/core";
import "react-flow-renderer/dist/style.css";
import isEmpty from "lodash/isEmpty";

/**
 * Main component to visualize query stage stats as a flowchart.
 */
export const VisualizeQueryStageStats = ({ stageStats }) => {
  const { nodes, edges } = generateFlowElements(stageStats); // Generate nodes and edges from input data
  
  if(isEmpty(stageStats)) {
    return (
      <Typography style={{height: "100px", textAlign: "center"}} variant="body1">
        No stats available
      </Typography>
    );
  }

  return (
    <div style={{ height: 500 }}>
      <ReactFlow
        nodes={nodes}
        edges={edges}
        fitView
        nodeTypes={nodeTypes} // Use custom node types
        zoomOnScroll={false}
      >
        <Background />
        <Controls showInteractive={false} />
        <MiniMap />
      </ReactFlow>
    </div>
  );
};

// ------------------------------------------------------------
// Helper functions and constants

// Constants for node styling and layout
const NODE_PADDING = 10;
const NODE_HEADER_CONTENT_MARGIN = 5;
const NODE_HEADER_HEIGHT = 20;
const NODE_CONTENT_HEIGHT = 16;
const NODE_TEXT_CHAR_WIDTH = 7;
const NODE_MIN_WIDTH = 150;

/**
 * Calculates the dimensions of a node based on its content.
 */
const calculateNodeDimensions = (data) => {
  const contentWidth = Math.max(
    Object.entries(data)
      .map(([key, value]) => key.length + String(value).length + 1) // Estimate character count in the content
      .reduce((max, length) => Math.max(max, length), 0) * NODE_TEXT_CHAR_WIDTH,
    NODE_MIN_WIDTH
  );

  const contentHeight =
    Object.keys(data).length * NODE_CONTENT_HEIGHT + // Height for each data line
    NODE_HEADER_HEIGHT + // Height for the header
    NODE_HEADER_CONTENT_MARGIN * 2 + // Margin between header and content
    NODE_PADDING * 2; // Padding around the node

  return { width: contentWidth + NODE_PADDING * 2, height: contentHeight };
};

/**
 * Applies Dagre layout to position nodes and edges.
 */
const layoutNodesAndEdges = (nodes, edges, direction = "TB") => {
  const dagreGraph = new dagre.graphlib.Graph();
  dagreGraph.setDefaultEdgeLabel(() => ({})); // Default edge properties
  dagreGraph.setGraph({ rankdir: direction }); // Layout direction

  // Add nodes to the graph
  nodes.forEach((node) => {
    dagreGraph.setNode(node.id, { width: node.width, height: node.height });
  });

  // Add edges to the graph
  edges.forEach((edge) => {
    dagreGraph.setEdge(edge.source, edge.target);
  });

  // Perform Dagre layout
  dagre.layout(dagreGraph);

  const isHorizontal = direction === "LR";
  return {
    nodes: nodes.map((node) => {
      const layoutedNode = dagreGraph.node(node.id); // Get node's position
      return {
        ...node,
        position: {
          x: layoutedNode.x - node.width / 2, // Center node horizontally
          y: layoutedNode.y - node.height / 2, // Center node vertically
        },
        targetPosition: isHorizontal ? "left" : "top",
        sourcePosition: isHorizontal ? "right" : "bottom",
      };
    }),
    edges,
  };
};

/**
 * Recursively generates nodes and edges for the flowchart from a hierarchical data structure.
 */
const generateFlowElements = (stats) => {
  const stageRoots: Map<Number, Node> = new Map();
  const nodes: Node[] = [];
  const edges: Edge[] = [];

  const createFlowNode = (data, id, parentId) => {
    const { width, height } = calculateNodeDimensions(data);

    // Add the node
    const flowNode: Node = { id, type: "customNode", data, position: { x: 0, y: 0 }, width, height };
    nodes.push(flowNode);

    // Add an edge if this node has a parent
    if (parentId) {
      edges.push({ id: `edge-${id}`, source: parentId, target: id });
    }

    return flowNode;
  }

  /**
   * Traverses the hierarchy and builds nodes and edges.
   *
   * Nodes that have been already added to the graph are not added again.
   */
  const traverseTree = (node, id, parentId) => {
    const { children, ...data } = node;

    const stageId = data["stage"];
    if (stageId) {
      const oldFlowNode = stageRoots.get(stageId);
      if (oldFlowNode) {
        // Add an edge if this node has a parent
        if (parentId) {
          const id = oldFlowNode.id;
          edges.push({ id: `edge-${parentId}-${id}`, source: parentId, target: id });
          return;
        }
      }
    }

    const newFlowNode = createFlowNode(data, id, parentId);
    if (stageId) {
      stageRoots.set(stageId, newFlowNode);
    }
    // Recursively process children
    children?.forEach((child, idx) => traverseTree(child, `${id}.${idx+1}`, newFlowNode.id));
  };

  traverseTree(stats, "0", null); // Start traversal from the root node

  return layoutNodesAndEdges(nodes, edges);
};

/**
 * Custom Node Renderer for React Flow.
 */
const CustomNode = ({ data, ...props }) => {
  const theme = useTheme();
  return (
    <div
      style={{
        border: "1px solid #ccc",
        borderRadius: "8px",
        backgroundColor: "#fff",
        padding: NODE_PADDING,
        boxShadow: "0 2px 5px rgba(0, 0, 0, 0.1)",
        minWidth: NODE_MIN_WIDTH,
      }}
    >
      <Handle type="source" position={props.sourcePosition} />
      <div
        style={{
          fontWeight: "bold",
          color: theme.palette.primary.main,
          fontSize: "18px",
          lineHeight: `${NODE_HEADER_HEIGHT}px`,
        }}
      >
        {data.type || "Unknown Type"} {/* Display node type */}
      </div>
      <hr style={{ margin: NODE_HEADER_CONTENT_MARGIN, color: "#ddd" }} />
      <div style={{ fontSize: "14px", lineHeight: `${NODE_CONTENT_HEIGHT}px` }}>
        {Object.entries(data).map(([key, value]) => (
          <div key={key}>
            <strong>{key}:</strong> {String(value)} {/* Display key-value pairs */}
          </div>
        ))}
      </div>
      <Handle type="target" position={props.targetPosition} />
    </div>
  );
};

// Define custom node types for React Flow
const nodeTypes = {
  customNode: CustomNode,
};
