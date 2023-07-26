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
package org.apache.pinot.query.planner.plannode;

import org.apache.pinot.common.proto.Plan;
import org.apache.pinot.common.utils.DataSchema;


public final class StageNodeSerDeUtils {
  private StageNodeSerDeUtils() {
    // do not instantiate.
  }

  public static AbstractPlanNode deserializeStageNode(Plan.StageNode protoNode) {
    AbstractPlanNode planNode = newNodeInstance(protoNode.getNodeName(), protoNode.getStageId());
    planNode.setDataSchema(extractDataSchema(protoNode));
    planNode.fromObjectField(protoNode.getObjectField());
    for (Plan.StageNode protoChild : protoNode.getInputsList()) {
      planNode.addInput(deserializeStageNode(protoChild));
    }
    return planNode;
  }

  public static Plan.StageNode serializeStageNode(AbstractPlanNode planNode) {
    Plan.StageNode.Builder builder = Plan.StageNode.newBuilder()
        .setStageId(planNode.getPlanFragmentId())
        .setNodeName(planNode.getClass().getSimpleName())
        .setObjectField(planNode.toObjectField());
    DataSchema dataSchema = planNode.getDataSchema();
    for (int i = 0; i < dataSchema.getColumnNames().length; i++) {
      builder.addColumnNames(dataSchema.getColumnName(i));
      builder.addColumnDataTypes(dataSchema.getColumnDataType(i).name());
    }
    for (PlanNode childNode : planNode.getInputs()) {
      builder.addInputs(serializeStageNode((AbstractPlanNode) childNode));
    }
    return builder.build();
  }

  private static DataSchema extractDataSchema(Plan.StageNode protoNode) {
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
      case "TableScanNode":
        return new TableScanNode(planFragmentId);
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
