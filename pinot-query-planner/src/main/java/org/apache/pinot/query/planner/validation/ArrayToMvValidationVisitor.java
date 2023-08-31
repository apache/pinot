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
package org.apache.pinot.query.planner.validation;

import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.plannode.AggregateNode;
import org.apache.pinot.query.planner.plannode.ExchangeNode;
import org.apache.pinot.query.planner.plannode.FilterNode;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.planner.plannode.MailboxSendNode;
import org.apache.pinot.query.planner.plannode.PlanNodeVisitor;
import org.apache.pinot.query.planner.plannode.ProjectNode;
import org.apache.pinot.query.planner.plannode.SetOpNode;
import org.apache.pinot.query.planner.plannode.SortNode;
import org.apache.pinot.query.planner.plannode.TableScanNode;
import org.apache.pinot.query.planner.plannode.ValueNode;
import org.apache.pinot.query.planner.plannode.WindowNode;


/**
 * This class is used to validate the arrayToMv usage.
 * Only leaf nodes are allowed to use arrayToMv function.
 */
public class ArrayToMvValidationVisitor implements PlanNodeVisitor<Void, Boolean> {
  public static final ArrayToMvValidationVisitor INSTANCE = new ArrayToMvValidationVisitor();

  @Override
  public Void visitFilter(FilterNode node, Boolean isIntermediateStage) {
    if (isIntermediateStage && containsArrayToMv(node.getCondition())) {
      throw new UnsupportedOperationException("Function 'ArrayToMv' is not supported in FILTER Intermediate Stage");
    }
    node.getInputs().forEach(e -> e.visit(this, isIntermediateStage));
    return null;
  }

  @Override
  public Void visitJoin(JoinNode node, Boolean isIntermediateStage) {
    if (containsArrayToMv(node.getJoinClauses())) {
      throw new UnsupportedOperationException("Function 'ArrayToMv' is not supported in JOIN Intermediate Stage");
    }
    node.getInputs().forEach(e -> e.visit(this, isIntermediateStage));
    return null;
  }

  @Override
  public Void visitMailboxReceive(MailboxReceiveNode node, Boolean isIntermediateStage) {
    node.getInputs().forEach(e -> e.visit(this, isIntermediateStage));
    return null;
  }

  @Override
  public Void visitMailboxSend(MailboxSendNode node, Boolean isIntermediateStage) {
    node.getInputs().forEach(e -> e.visit(this, isIntermediateStage));
    return null;
  }

  @Override
  public Void visitAggregate(AggregateNode node, Boolean isIntermediateStage) {
    if (isIntermediateStage && containsArrayToMv(node.getGroupSet())) {
      throw new UnsupportedOperationException(
          "Function 'ArrayToMv' is not supported in AGGREGATE Intermediate Stage");
    }
    if (isIntermediateStage && containsArrayToMv(node.getAggCalls())) {
      throw new UnsupportedOperationException(
          "Function 'ArrayToMv' is not supported in AGGREGATE Intermediate Stage");
    }
    if (isIntermediateStage) {
      node.getInputs().forEach(e -> e.visit(this, true));
    }
    // No need to traverse underlying ProjectNode in leaf stage
    return null;
  }

  @Override
  public Void visitProject(ProjectNode node, Boolean isIntermediateStage) {
    // V1 project node contains arrayToMv function is not supported as it will be transferred using toString.
    if (containsArrayToMv(node.getProjects())) {
      throw new UnsupportedOperationException(
          "Function 'ArrayToMv' is not supported in PROJECT " + (isIntermediateStage ? "Intermediate Stage"
              : "Leaf Stage"));
    }
    node.getInputs().forEach(e -> e.visit(this, isIntermediateStage));
    return null;
  }

  @Override
  public Void visitSort(SortNode node, Boolean isIntermediateStage) {
    if (isIntermediateStage && containsArrayToMv(node.getCollationKeys())) {
      throw new UnsupportedOperationException("Function 'ArrayToMv' is not supported in SORT Intermediate Stage");
    }
    node.getInputs().forEach(e -> e.visit(this, isIntermediateStage));
    return null;
  }

  @Override
  public Void visitTableScan(TableScanNode node, Boolean isIntermediateStage) {
    return null;
  }

  @Override
  public Void visitValue(ValueNode node, Boolean isIntermediateStage) {
    return null;
  }

  @Override
  public Void visitWindow(WindowNode node, Boolean isIntermediateStage) {
    if (isIntermediateStage && containsArrayToMv(node.getGroupSet())) {
      throw new UnsupportedOperationException("Function 'ArrayToMv' is not supported in WINDOW Intermediate Stage");
    }
    if (isIntermediateStage && containsArrayToMv(node.getAggCalls())) {
      throw new UnsupportedOperationException("Function 'ArrayToMv' is not supported in WINDOW Intermediate Stage");
    }
    if (isIntermediateStage && containsArrayToMv(node.getOrderSet())) {
      throw new UnsupportedOperationException("Function 'ArrayToMv' is not supported in WINDOW Intermediate Stage");
    }
    node.getInputs().forEach(e -> e.visit(this, isIntermediateStage));
    return null;
  }

  @Override
  public Void visitSetOp(SetOpNode setOpNode, Boolean isIntermediateStage) {
    setOpNode.getInputs().forEach(e -> e.visit(this, isIntermediateStage));
    return null;
  }

  @Override
  public Void visitExchange(ExchangeNode exchangeNode, Boolean isIntermediateStage) {
    exchangeNode.getInputs().forEach(input -> input.visit(this, isIntermediateStage));
    return null;
  }

  private boolean containsArrayToMv(@Nullable RexExpression expression) {
    if (expression == null) {
      return false;
    }
    if (expression instanceof RexExpression.FunctionCall) {
      RexExpression.FunctionCall functionCall = (RexExpression.FunctionCall) expression;
      String functionName = functionCall.getFunctionName();
      if (functionName.equalsIgnoreCase("ARRAY_TO_MV") || functionName
          .equalsIgnoreCase("ARRAYTOMV")) {
        return true;
      }
      // Process operands
      return containsArrayToMv(functionCall.getFunctionOperands());
    }
    return false;
  }

  private boolean containsArrayToMv(@Nullable List<RexExpression> conditions) {
    if (conditions == null) {
      return false;
    }
    for (RexExpression condition : conditions) {
      if (containsArrayToMv(condition)) {
        return true;
      }
    }
    return false;
  }
}
