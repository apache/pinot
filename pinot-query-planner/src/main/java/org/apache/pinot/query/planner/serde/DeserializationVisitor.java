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
package org.apache.pinot.query.planner.serde;

import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.common.proto.Plan;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.plannode.AbstractPlanNode;
import org.apache.pinot.query.planner.plannode.AggregateNode;
import org.apache.pinot.query.planner.plannode.FilterNode;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.planner.plannode.MailboxSendNode;
import org.apache.pinot.query.planner.plannode.ProjectNode;
import org.apache.pinot.query.planner.plannode.SetOpNode;
import org.apache.pinot.query.planner.plannode.SortNode;
import org.apache.pinot.query.planner.plannode.TableScanNode;
import org.apache.pinot.query.planner.plannode.ValueNode;
import org.apache.pinot.query.planner.plannode.WindowNode;


public class DeserializationVisitor {
  public AbstractPlanNode process(Plan.PlanNode protoNode) {
    switch (protoNode.getNodeTypeCase()) {
      case OBJECTFIELD:
        return processUnknownNodeType(protoNode);
      case TABLESCANNODE:
        return visitTableScanNode(protoNode);
      default:
        throw new RuntimeException(String.format("Unknown Node Type %s", protoNode.getNodeTypeCase()));
    }
  }

  private AbstractPlanNode processUnknownNodeType(Plan.PlanNode protoNode) {
    AbstractPlanNode planNode = newNodeInstance(protoNode.getNodeName(), protoNode.getStageId());
    planNode.setDataSchema(extractDataSchema(protoNode));
    planNode.fromObjectField(protoNode.getObjectField());
    for (Plan.PlanNode protoChild : protoNode.getInputsList()) {
      planNode.addInput(process(protoChild));
    }
    return planNode;
  }

  private AbstractPlanNode visitTableScanNode(Plan.PlanNode protoNode) {
    Plan.TableScanNode protoTableNode = protoNode.getTableScanNode();
    List<String> list = new ArrayList<>(protoTableNode.getTableScanColumnsList());
    return new TableScanNode(protoNode.getStageId(), extractDataSchema(protoNode),
        extractNodeHint(protoTableNode.getNodeHint()), protoTableNode.getTableName(), list);
  }

  private static AbstractPlanNode.NodeHint extractNodeHint(Plan.NodeHint protoNodeHint) {
    AbstractPlanNode.NodeHint nodeHint = new AbstractPlanNode.NodeHint();
    protoNodeHint.getHintOptionsMap().forEach((key, value) -> nodeHint._hintOptions.put(key, value.getOptionsMap()));

    return nodeHint;
  }

  private static DataSchema extractDataSchema(Plan.PlanNode protoNode) {
    String[] columnDataTypesList = protoNode.getColumnDataTypesList().toArray(new String[]{});
    String[] columnNames = protoNode.getColumnNamesList().toArray(new String[]{});
    DataSchema.ColumnDataType[] columnDataTypes = new DataSchema.ColumnDataType[columnNames.length];
    for (int i = 0; i < columnNames.length; i++) {
      columnDataTypes[i] = DataSchema.ColumnDataType.valueOf(columnDataTypesList[i]);
    }
    return new DataSchema(columnNames, columnDataTypes);
  }

  private static AbstractPlanNode newNodeInstance(String nodeName, int planFragmentId) {
    switch (nodeName) {
      case "JoinNode":
        return new JoinNode(planFragmentId);
      case "ProjectNode":
        return new ProjectNode(planFragmentId);
      case "FilterNode":
        return new FilterNode(planFragmentId);
      case "AggregateNode":
        return new AggregateNode(planFragmentId);
      case "SortNode":
        return new SortNode(planFragmentId);
      case "MailboxSendNode":
        return new MailboxSendNode(planFragmentId);
      case "MailboxReceiveNode":
        return new MailboxReceiveNode(planFragmentId);
      case "ValueNode":
        return new ValueNode(planFragmentId);
      case "WindowNode":
        return new WindowNode(planFragmentId);
      case "SetOpNode":
        return new SetOpNode(planFragmentId);
      case "ExchangeNode":
        throw new IllegalArgumentException(
            "ExchangeNode should be already split into MailboxSendNode and MailboxReceiveNode");
      default:
        throw new IllegalArgumentException("Unknown node name: " + nodeName);
    }
  }
}
