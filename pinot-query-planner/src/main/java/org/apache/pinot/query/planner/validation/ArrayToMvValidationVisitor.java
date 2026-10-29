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
import org.apache.pinot.calcite.sql.fun.ArrayToMvFunctionName;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.planner.PlanFragment;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.plannode.AggregateNode;
import org.apache.pinot.query.planner.plannode.EnrichedJoinNode;
import org.apache.pinot.query.planner.plannode.ExchangeNode;
import org.apache.pinot.query.planner.plannode.ExplainedNode;
import org.apache.pinot.query.planner.plannode.FilterNode;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.planner.plannode.MailboxSendNode;
import org.apache.pinot.query.planner.plannode.MvAggregateNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
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


/// Validates the uses of `ARRAY_TO_MV` that remain in a plan after the planner rules.
///
/// `GROUP BY ARRAY_TO_MV(expr)` is planned as an MV aggregate, which removes the call from the plan. Any other use is
/// either a supported leaf-stage use, where the leaf translation drops the call and the single-stage engine sees the
/// multi-value column, or an error. The check is structural: any remaining call in a node that runs in an intermediate
/// stage is an error, so that no `f(ARRAY_TO_MV(col))` shape reaches an intermediate-stage operator undetected.
///
/// | Node | Leaf stage | Intermediate stage |
/// |---|---|---|
/// | Project under an aggregate | allowed, not visited | error |
/// | Project not under an aggregate (SELECT list) | error | error |
/// | Filter | allowed | error |
/// | Aggregate call argument | allowed | error |
/// | Array-typed group key (`GROUP BY mvCol`) | error | error |
/// | Join non-equi condition | error | error |
/// | Window | not checked (a window never runs in the leaf stage) | error |
///
/// Every error message ends with [#SUPPORTED_USES] and carries [QueryErrorCode#QUERY_PLANNING].
///
/// "Leaf stage" means below the leaf boundary, the part of a leaf fragment that the single-stage engine runs. In a
/// fragment with a lookup join, the boundary is the right input of the join: the join and every node above it run
/// as multi-stage operators and are validated as intermediate-stage nodes. In a fragment with an `UNNEST`, the
/// boundary is the input of the `UNNEST` in the same way.
///
/// The visitor is stateless and thread-safe. The context is `true` when the visited node runs in an intermediate
/// stage and `false` when it runs in the leaf stage. Use [#validate(PlanNode, boolean)] to validate a fragment.
public class ArrayToMvValidationVisitor implements PlanNodeVisitor<Void, Boolean> {
  public static final ArrayToMvValidationVisitor INSTANCE = new ArrayToMvValidationVisitor();

  /// Validates the plan fragment rooted at `fragmentRoot`.
  ///
  /// @param scansTables whether the fragment scans a table (a leaf fragment)
  public static void validate(PlanNode fragmentRoot, boolean scansTables) {
    // In a leaf fragment with a lookup join or an UNNEST, the nodes from the root down to that node run above the leaf
    // boundary.
    boolean isIntermediateStage = !scansTables || containsLookupJoin(fragmentRoot) || containsUnnest(fragmentRoot);
    fragmentRoot.visit(INSTANCE, isIntermediateStage);
  }

  /// Validates every fragment of the plan rooted at `fragment`. It needs no worker assignment: a fragment is a leaf
  /// fragment when it scans a table.
  public static void validateFragments(PlanFragment fragment) {
    PlanNode fragmentRoot = fragment.getFragmentRoot();
    validate(fragmentRoot, scansTable(fragmentRoot));
    for (PlanFragment child : fragment.getChildren()) {
      validateFragments(child);
    }
  }

  private static boolean containsLookupJoin(PlanNode node) {
    if (node instanceof JoinNode && ((JoinNode) node).getJoinStrategy() == JoinNode.JoinStrategy.LOOKUP) {
      return true;
    }
    // The inputs of a MailboxReceiveNode belong to other fragments.
    if (node instanceof MailboxReceiveNode) {
      return false;
    }
    for (PlanNode input : node.getInputs()) {
      if (containsLookupJoin(input)) {
        return true;
      }
    }
    return false;
  }

  private static boolean containsUnnest(PlanNode node) {
    if (node instanceof UnnestNode) {
      return true;
    }
    if (node instanceof MailboxReceiveNode) {
      return false;
    }
    for (PlanNode input : node.getInputs()) {
      if (containsUnnest(input)) {
        return true;
      }
    }
    return false;
  }

  private static boolean scansTable(PlanNode node) {
    if (node instanceof TableScanNode) {
      return true;
    }
    if (node instanceof MailboxReceiveNode) {
      return false;
    }
    for (PlanNode input : node.getInputs()) {
      if (scansTable(input)) {
        return true;
      }
    }
    return false;
  }

  /// The tail of every error message of this visitor.
  public static final String SUPPORTED_USES = "ARRAY_TO_MV is supported only: (1) as a complete GROUP BY key, "
      + "GROUP BY ARRAY_TO_MV(expr), where expr is a multi-value column or an array expression; (2) in WHERE "
      + "predicates that run in the leaf stage (directly on a table scan). It is not supported nested inside another "
      + "expression (for example CASE WHEN ARRAY_TO_MV(col) IS NOT NULL ...), in the SELECT list, in aggregate "
      + "arguments, in filters above a join or an aggregate, in JOIN conditions, or in window functions.";

  @Override
  public Void visitFilter(FilterNode node, Boolean isIntermediateStage) {
    if (isIntermediateStage && containsArrayToMv(node.getCondition())) {
      throw error("ARRAY_TO_MV found in FILTER in the " + stageName(true) + " stage");
    }
    node.getInputs().forEach(e -> e.visit(this, isIntermediateStage));
    return null;
  }

  @Override
  public Void visitJoin(JoinNode node, Boolean isIntermediateStage) {
    if (containsArrayToMv(node.getNonEquiConditions())) {
      throw error("ARRAY_TO_MV found in JOIN condition in the " + stageName(isIntermediateStage) + " stage");
    }
    if (node.getJoinStrategy() == JoinNode.JoinStrategy.LOOKUP) {
      // The right input of a lookup join is the leaf boundary: it runs in the single-stage engine.
      List<PlanNode> inputs = node.getInputs();
      inputs.get(0).visit(this, isIntermediateStage);
      inputs.get(1).visit(this, false);
    } else {
      node.getInputs().forEach(e -> e.visit(this, isIntermediateStage));
    }
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
    // The group keys are the first columns of the output.
    checkNoArrayGroupKey(node.getDataSchema(), node.getGroupKeys().size(), List.of(), "AGGREGATE",
        isIntermediateStage);
    if (!isIntermediateStage) {
      // ARRAY_TO_MV is a no-op in the leaf stage: the leaf translation drops the call and the single-stage engine
      // evaluates the expression on the multi-value column. The Project under the aggregate is not visited.
      return null;
    }
    if (containsArrayToMv(node.getAggCalls())) {
      throw error("ARRAY_TO_MV found in an aggregate call argument in AGGREGATE in the " + stageName(true) + " stage");
    }
    node.getInputs().forEach(e -> e.visit(this, true));
    return null;
  }

  /// The MV aggregate replaced `ARRAY_TO_MV(expr)` keys with `expr`. Its MV keys must be arrays in the input and
  /// scalars in the output; anything else is a planner bug, not a user error.
  @Override
  public Void visitMvAggregate(MvAggregateNode node, Boolean isIntermediateStage) {
    DataSchema inputSchema = node.getInputs().get(0).getDataSchema();
    DataSchema outputSchema = node.getDataSchema();
    List<Integer> groupKeys = node.getGroupKeys();
    List<Integer> mvKeyIndexes = node.getMvKeyIndexes();
    for (int mvKeyIndex : mvKeyIndexes) {
      ColumnDataType inputType = inputSchema.getColumnDataType(groupKeys.get(mvKeyIndex));
      ColumnDataType outputType = outputSchema.getColumnDataType(mvKeyIndex);
      if (!inputType.isArray() || outputType.isArray()) {
        throw new IllegalStateException(
            "MV key at group position " + mvKeyIndex + " must be an array in the input and a scalar in the output, "
                + "got input: " + inputType + ", output: " + outputType);
      }
    }
    checkNoArrayGroupKey(outputSchema, groupKeys.size(), mvKeyIndexes, "MV_AGGREGATE", isIntermediateStage);
    if (!isIntermediateStage) {
      // As for a plain aggregate, the Project under a leaf-stage MV aggregate is not visited.
      return null;
    }
    if (containsArrayToMv(node.getAggCalls())) {
      throw error(
          "ARRAY_TO_MV found in an aggregate call argument in MV_AGGREGATE in the " + stageName(true) + " stage");
    }
    node.getInputs().forEach(e -> e.visit(this, true));
    return null;
  }

  @Override
  public Void visitProject(ProjectNode node, Boolean isIntermediateStage) {
    // A Project with ARRAY_TO_MV is not supported in either stage: the multi-value column would be transferred as an
    // array, not one value per row.
    if (containsArrayToMv(node.getProjects())) {
      throw error("ARRAY_TO_MV found in PROJECT in the " + stageName(isIntermediateStage) + " stage. To get one row "
          + "for each value of a multi-value column, use 'CROSS JOIN UNNEST(mvCol) AS t(val)' instead");
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
      throw error("ARRAY_TO_MV found in WINDOW in the " + stageName(true) + " stage");
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
    // In a leaf fragment, the input of the UNNEST is the leaf boundary: the single-stage engine runs it.
    node.getInputs().forEach(e -> e.visit(this, isIntermediateStage && !scansTable(e)));
    return null;
  }

  /// Fails when one of the first `numGroupKeys` columns of `schema`, except the positions in `mvKeyIndexes`, is an
  /// array: grouping by a whole array is not supported.
  private static void checkNoArrayGroupKey(DataSchema schema, int numGroupKeys, List<Integer> mvKeyIndexes,
      String nodeName, boolean isIntermediateStage) {
    ColumnDataType[] columnDataTypes = schema.getColumnDataTypes();
    for (int i = 0; i < numGroupKeys && i < columnDataTypes.length; i++) {
      if (columnDataTypes[i] != null && columnDataTypes[i].isArray() && !mvKeyIndexes.contains(i)) {
        throw error("Multi-valued column found as a grouping key in " + nodeName + " in the "
            + stageName(isIntermediateStage) + " stage. Use ARRAY_TO_MV() to group by multi-value column");
      }
    }
  }

  private static String stageName(boolean isIntermediateStage) {
    return isIntermediateStage ? "intermediate" : "leaf";
  }

  private static QueryException error(String context) {
    return new ArrayToMvValidationException(context + ". " + SUPPORTED_USES);
  }

  /// The [QueryErrorCode#QUERY_PLANNING] error of a plan that uses `ARRAY_TO_MV` where the plan cannot run it. The
  /// `auto` mode of the MV aggregate plans such a query again with the MV aggregate.
  public static class ArrayToMvValidationException extends QueryException {
    ArrayToMvValidationException(String message) {
      super(QueryErrorCode.QUERY_PLANNING, message);
    }
  }

  private boolean containsArrayToMv(RexExpression expression) {
    if (expression instanceof RexExpression.FunctionCall) {
      RexExpression.FunctionCall functionCall = (RexExpression.FunctionCall) expression;
      String functionName = functionCall.getFunctionName();
      if (ArrayToMvFunctionName.matches(functionName)) {
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
