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
import java.util.Set;
import org.apache.calcite.sql.SqlKind;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.plannode.AggregateNode;
import org.apache.pinot.query.planner.plannode.EnrichedJoinNode;
import org.apache.pinot.query.planner.plannode.ExchangeNode;
import org.apache.pinot.query.planner.plannode.ExplainedNode;
import org.apache.pinot.query.planner.plannode.FilterNode;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.planner.plannode.MailboxSendNode;
import org.apache.pinot.query.planner.plannode.PlanNodeVisitor;
import org.apache.pinot.query.planner.plannode.ProjectNode;
import org.apache.pinot.query.planner.plannode.SetOpNode;
import org.apache.pinot.query.planner.plannode.SortNode;
import org.apache.pinot.query.planner.plannode.TableScanNode;
import org.apache.pinot.query.planner.plannode.UnnestNode;
import org.apache.pinot.query.planner.plannode.ValueNode;
import org.apache.pinot.query.planner.plannode.WindowNode;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.QueryException;


/// This class is used to validate the arrayToMv usage.
/// Only leaf nodes are allowed to use arrayToMv function.
public class ArrayToMvValidationVisitor implements PlanNodeVisitor<Void, Boolean> {
  public static final ArrayToMvValidationVisitor INSTANCE = new ArrayToMvValidationVisitor();

  private static final Set<String> COMPARISON_FUNCTIONS =
      Set.of(SqlKind.EQUALS.name(), SqlKind.NOT_EQUALS.name(), SqlKind.IN.name(), SqlKind.NOT_IN.name());

  @Override
  public Void visitFilter(FilterNode node, Boolean isIntermediateStage) {
    if (isIntermediateStage && containsArrayToMv(node.getCondition())) {
      throw arrayToMvNotSupported("FILTER Intermediate Stage", List.of(node.getCondition()), "");
    }
    node.getInputs().forEach(e -> e.visit(this, isIntermediateStage));
    return null;
  }

  @Override
  public Void visitJoin(JoinNode node, Boolean isIntermediateStage) {
    if (containsArrayToMv(node.getNonEquiConditions())) {
      throw arrayToMvNotSupported("JOIN Intermediate Stage", node.getNonEquiConditions(), "");
    }
    node.getInputs().forEach(e -> e.visit(this, isIntermediateStage));
    return null;
  }

  @Deprecated(forRemoval = true, since = "1.6.0")
  @Override
  public Void visitEnrichedJoin(EnrichedJoinNode node, Boolean isIntermediateStage) {
    visitJoin(node, isIntermediateStage);
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
    if (!isIntermediateStage) {
      // No need to traverse underlying ProjectNode in leaf stage
      return null;
    }
    if (containsArrayToMv(node.getAggCalls())) {
      throw new QueryException(QueryErrorCode.QUERY_PLANNING,
          "Function 'ArrayToMv' is not supported in AGGREGATE Intermediate Stage");
    }
    DataSchema.ColumnDataType[] columnDataTypes = node.getDataSchema().getColumnDataTypes();
    for (Integer key : node.getGroupKeys()) {
      if (key >= 0 && key < columnDataTypes.length
          && columnDataTypes[key] != null
          && columnDataTypes[key].isArray()) {
        throw new QueryException(QueryErrorCode.QUERY_PLANNING,
            "Multi-valued columns are not supported as a grouping key in the intermediate stage. "
                + "Use ARRAY_TO_MV() to group by multi-value column");
      }
    }
    node.getInputs().forEach(e -> e.visit(this, true));
    return null;
  }

  @Override
  public Void visitProject(ProjectNode node, Boolean isIntermediateStage) {
    // V1 project node contains arrayToMv function is not supported as it will be transferred using toString.
    if (containsArrayToMv(node.getProjects())) {
      throw arrayToMvNotSupported("PROJECT " + (isIntermediateStage ? "Intermediate Stage" : "Leaf Stage"),
          node.getProjects(), ". To get one row for each value of a multi-value column, use "
              + "'CROSS JOIN UNNEST(mvCol) AS t(val)' instead");
    }
    node.getInputs().forEach(e -> e.visit(this, isIntermediateStage));
    return null;
  }

  @Override
  public Void visitSort(SortNode node, Boolean isIntermediateStage) {
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
    if (isIntermediateStage && containsArrayToMv(node.getAggCalls())) {
      throw new QueryException(QueryErrorCode.QUERY_PLANNING,
          "Function 'ArrayToMv' is not supported in WINDOW Intermediate Stage");
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

  @Override
  public Void visitExplained(ExplainedNode node, Boolean isIntermediateStage) {
    node.getInputs().forEach(input -> input.visit(this, isIntermediateStage));
    return null;
  }

  @Override
  public Void visitUnnest(UnnestNode node, Boolean isIntermediateStage) {
    node.getInputs().forEach(e -> e.visit(this, isIntermediateStage));
    return null;
  }

  /// Comparing a multi-value column to a value (e.g. `mvCol = 'a'`) is planned as a comparison on `ARRAY_TO_MV`. As
  /// the query does not contain `ARRAY_TO_MV` in this case, the error explains the comparison instead.
  private QueryException arrayToMvNotSupported(String location, List<? extends RexExpression> expressions,
      String hint) {
    if (containsMultiValueComparison(expressions)) {
      return new QueryException(QueryErrorCode.QUERY_PLANNING,
          "Comparing a multi-value column to a value (e.g. mvCol = 'a' or mvCol IN ('a', 'b')) is not supported in "
              + location + ". It is only supported in a WHERE clause filter that can be pushed down to the table "
              + "scan. Elsewhere, use the arrayContains function for the column type instead, e.g. "
              + "arrayContainsString(mvCol, 'a') or arrayContainsInt(mvCol, 1)");
    }
    return new QueryException(QueryErrorCode.QUERY_PLANNING,
        "Function 'ArrayToMv' is not supported in " + location + hint);
  }

  private boolean containsMultiValueComparison(List<? extends RexExpression> expressions) {
    for (RexExpression expression : expressions) {
      if (expression instanceof RexExpression.FunctionCall) {
        RexExpression.FunctionCall functionCall = (RexExpression.FunctionCall) expression;
        List<RexExpression> operands = functionCall.getFunctionOperands();
        if ((COMPARISON_FUNCTIONS.contains(functionCall.getFunctionName()) && operands.stream()
            .anyMatch(ArrayToMvValidationVisitor::isArrayToMv)) || containsMultiValueComparison(operands)) {
          return true;
        }
      }
    }
    return false;
  }

  private static boolean isArrayToMv(RexExpression expression) {
    if (!(expression instanceof RexExpression.FunctionCall)) {
      return false;
    }
    String functionName = ((RexExpression.FunctionCall) expression).getFunctionName();
    return functionName.equalsIgnoreCase("ARRAY_TO_MV") || functionName.equalsIgnoreCase("ARRAYTOMV");
  }

  private boolean containsArrayToMv(RexExpression expression) {
    if (expression instanceof RexExpression.FunctionCall) {
      RexExpression.FunctionCall functionCall = (RexExpression.FunctionCall) expression;
      String functionName = functionCall.getFunctionName();
      if (functionName.equalsIgnoreCase("ARRAY_TO_MV") || functionName.equalsIgnoreCase("ARRAYTOMV")) {
        return true;
      }
      // Process operands
      return containsArrayToMv(functionCall.getFunctionOperands());
    }
    return false;
  }

  private boolean containsArrayToMv(List<? extends RexExpression> expressions) {
    for (RexExpression expression : expressions) {
      if (containsArrayToMv(expression)) {
        return true;
      }
    }
    return false;
  }
}
