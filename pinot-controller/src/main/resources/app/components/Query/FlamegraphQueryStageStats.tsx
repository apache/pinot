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
import React, {useMemo, useState} from "react";
import { Typography, useTheme } from "@material-ui/core";
import "react-flow-renderer/dist/style.css";
import isEmpty from "lodash/isEmpty";
import { FlameGraph } from 'react-flame-graph';


/**
 * Main component to visualize query stage stats as a flowchart.
 */
export const FlameGraphQueryStageStats = ({ stageStats, mode }) => {
  // highlighted stage is the stage currently highlighted by the user
  const [highlightedStage, setHighlightedStage] = useState<number | null>(null);
  const { data } = useMemo(() => generateFlameGraphData(stageStats, highlightedStage, mode), [stageStats, highlightedStage, mode]);

  if(isEmpty(stageStats)) {
    return (
      <Typography style={{height: "100px", textAlign: "center"}} variant="body1">
        No stats available
      </Typography>
    );
  }

  return (
    <div style={{ height: 1000}}>
      <FlameGraph
        data={data}
        height={1000}
        width={800}
        onMouseOver = {(event, itemData) => {
          if (itemData?.relatedStage) {
            setHighlightedStage(itemData.relatedStage);
          }
        }}
        onMouseOut = {(event, itemData) => {
          setHighlightedStage(null);
        }}
      />
    </div>
  );
};

// ------------------------------------------------------------
// Helper functions and constants

export enum FlamegraphMode
{
  /**
   * Uses clockTimeMs to size the nodes.
   */
  CLOCK_TIME,
  /**
   * Uses allocatedBytes to size the nodes.
   */
  ALLOCATION,
}

/**
 * Recursively generates data for the flame graph from the stats.
 *
 * Although the actual value used for sizing the nodes depends on the selected mode, the structure remains the same.
 *
 * The returned flame graph structure has two different nodes: Stage nodes and Operation nodes. Stage nodes are always
 * the children of the root node. Each stage node contains a single children node that corresponds to the MAILBOX_SEND
 * operation whose "stage" field matches the stage id. The rest of the operations are children of this MAILBOX_SEND
 * node. In other words, for each MAILBOX_SEND operation, we create a new stage node that is a children of the root
 * node.
 * Then the MAILBOX_SEND node contains the rest of the operations as children, pruning after the MAILBOX_RECEIVE nodes.
 */
const generateFlameGraphData = (stats, highlightedStage : Number = null, mode : FlamegraphMode = FlamegraphMode.CLOCK_TIME) => {
  let getNodeValue;
  switch (mode) {
    case FlamegraphMode.ALLOCATION:
      getNodeValue = (node) => node["allocatedMemoryBytes"] || 0;
      break;
    case FlamegraphMode.CLOCK_TIME:
    default:
      getNodeValue = (node) => node["clockTimeMs"] || 1;
      break;
  }

  const stages = [];

  const processNode = (node, currentStage, onPipelineBreaker: boolean = false) => {
    let { children, ...data } = node;

    const baseNode = {
      tooltip: JSON.stringify(data),
      value: getNodeValue(data),
      backgroundColor: highlightedStage === currentStage ? 'lightblue' : null,
    }
    let name: String;
    switch (data.type) {
      case "LEAF": {
        name = `LEAF (${data.table || ''})`;
        if (children && children.length !== 0) {
          // We don't want to include the children in this stage because its time is independent of the leaf node.
          children = [];
        }
        break;
      }
      case "MAILBOX_RECEIVE": {
        // If it's a MAILBOX_RECEIVE node, prune the tree here
        const sendOperator = children[0];
        visitStage(sendOperator, currentStage);
        const prefix = onPipelineBreaker ? "PIPELINE_BREAKER " : "MAILBOX_RECEIVE";
        return {
          name: `${prefix} from stage ${sendOperator.stage || 'unknown'}`,
          relatedStage: sendOperator.stage || null, ...baseNode,
        };
      }
      default: {
        name = data.type || "Unknown Type";
      }
    }

    // For other nodes, continue processing children
    return {
      name: name,
      ...baseNode,
      children: children
        ? children.map(node => processNode(node, currentStage, onPipelineBreaker))
          .filter(child => child !== null) : [],
    };
  }

  const getPipelineBreakerNode = (children, currentStage: number) => {
    const stack = [...children];
    while (stack.length > 0) {
      const node = stack.pop();
      if (node.type === "LEAF" && node.children && node.children.length > 0) {
        return processNode(node.children[0], currentStage, true);
      }
      if (node.type == "MAILBOX_RECEIVE") {
        // We don't want to go past MAILBOX_RECEIVE nodes, as their children belong to other stages.
        continue;
      }
      if (node.children && node.children.length > 0) {
        stack.push(...node.children);
      }
    }
    return null;
  }

  const visitStage = (node, parentStage = null) => {
    const { children, ...data } = node;
    const stage = data.stage || 0;
    const pipelineBreakerNode = getPipelineBreakerNode(children, stage);
    let value: number;
    const childrenNodes = children
        ? children.map(node => processNode(node, stage)).filter(child => child !== null)
        : [];
    if (pipelineBreakerNode) {
      value = getNodeValue(node) + pipelineBreakerNode.value || 0;
      childrenNodes.push(pipelineBreakerNode);
    } else {
      value = getNodeValue(node)
    }

    stages.push({
      name: "Stage " + stage,
      value: value,
      tooltip: JSON.stringify(data),
      backgroundColor: stage === highlightedStage ? 'lightblue' : null,
      relatedStage: parentStage,
      children: childrenNodes
    });
  }
  stats.children.forEach((node) => visitStage(node));
  stages.sort((a, b) => b.value - a.value);

  return {
    data: {
      name: 'All stages',
      value: Math.max(1, stages.reduce((sum, child) => sum + child.value, 0)),
      children: stages,
    }
  };
}