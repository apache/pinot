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
package org.apache.pinot.calcite.rel.rules;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.rex.RexWindowBounds;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.pinot.calcite.rel.hint.PinotHintStrategyTable;


public class PinotRuleUtils {
  private PinotRuleUtils() {
  }

  public static final RelBuilder.Config PINOT_REL_CONFIG =
      RelBuilder.Config.DEFAULT.withPruneInputOfAggregate(false).withPushJoinCondition(true).withAggregateUnique(true);

  public static final RelBuilderFactory PINOT_REL_FACTORY =
      RelBuilder.proto(Contexts.of(RelFactories.DEFAULT_STRUCT, PINOT_REL_CONFIG));

  public static final SqlToRelConverter.Config PINOT_SQL_TO_REL_CONFIG = SqlToRelConverter.config()
      .withHintStrategyTable(PinotHintStrategyTable.PINOT_HINT_STRATEGY_TABLE)
      .withTrimUnusedFields(true)
      // TODO: expansion is deprecated in Calcite; we need to move to the default value of false here which will be the
      //       only supported option in Calcite going forward. This will probably require some changes in the planner
      //       rules in order to support things like scalar query filters.
      .withExpand(true)
      .withInSubQueryThreshold(Integer.MAX_VALUE)
      .withRelBuilderFactory(PINOT_REL_FACTORY);

  public static RelNode unboxRel(RelNode rel) {
    if (rel instanceof HepRelVertex) {
      return ((HepRelVertex) rel).getCurrentRel();
    } else {
      return rel;
    }
  }

  public static boolean isExchange(RelNode rel) {
    return unboxRel(rel) instanceof Exchange;
  }

  public static boolean isProject(RelNode rel) {
    return unboxRel(rel) instanceof Project;
  }

  public static boolean isJoin(RelNode rel) {
    return unboxRel(rel) instanceof Join;
  }

  public static boolean isAggregate(RelNode rel) {
    return unboxRel(rel) instanceof Aggregate;
  }

  /**
   * utility logic to determine if a JOIN can be pushed down to the leaf-stage execution and leverage the
   * segment-local info (indexing and others) to speed up the execution.
   *
   * <p>The logic here is that the "row-representation" of the relation must not have changed. E.g. </p>
   * <ul>
   *   <li>`RelNode` that are single-in, single-out are possible (Project/Filter/)</li>
   *   <li>`Join` can be stacked on top if we only consider SEMI-JOIN</li>
   *   <li>`Window` should be allowed but we dont have impl for Window on leaf, so not yet included.</li>
   *   <li>`Sort` should be allowed but we need to reorder Sort and Join first, so not yet included.</li>
   * </ul>
   */
  public static boolean canPushDynamicBroadcastToLeaf(RelNode relNode) {
    // TODO 1: optimize this part out as it is not efficient to scan the entire subtree for exchanges;
    //    we should cache the stats in the node (potentially using Trait, e.g. marking LeafTrait & IntermediateTrait)
    // TODO 2: this part is similar to how ServerPlanRequestVisitor determines leaf-stage boundary;
    //    we should refactor and merge both logic
    // TODO 3: for JoinNode, currently this only works towards left-side;
    //    we should support both left and right.
    // TODO 4: for JoinNode, currently this only works for SEMI-JOIN, INNER-JOIN can bring in rows from both sides;
    //    we should check only the non-pipeline-breaker side columns are accessed.
    relNode = PinotRuleUtils.unboxRel(relNode);

    if (relNode instanceof TableScan) {
      // reaching table means it is plan-able.
      return true;
    } else if (relNode instanceof Project || relNode instanceof Filter) {
      // reaching single-in, single-out RelNode means we can continue downward.
      return canPushDynamicBroadcastToLeaf(relNode.getInput(0));
    } else if (relNode instanceof Join) {
      // always check only the left child for dynamic broadcast
      return canPushDynamicBroadcastToLeaf(((Join) relNode).getLeft());
    } else {
      // for all others we don't allow dynamic broadcast
      return false;
    }
  }

  public static String extractFunctionName(RexCall function) {
    SqlKind funcSqlKind = function.getOperator().getKind();
    return funcSqlKind == SqlKind.OTHER_FUNCTION ? function.getOperator().getName() : funcSqlKind.name();
  }

  public static class WindowUtils {
    // Supported window functions
    // OTHER_FUNCTION supported are: BOOL_AND, BOOL_OR
    private static final EnumSet<SqlKind> SUPPORTED_WINDOW_FUNCTION_KIND =
        EnumSet.of(SqlKind.SUM, SqlKind.SUM0, SqlKind.MIN, SqlKind.MAX, SqlKind.COUNT, SqlKind.ROW_NUMBER, SqlKind.RANK,
            SqlKind.DENSE_RANK, SqlKind.NTILE, SqlKind.LAG, SqlKind.LEAD, SqlKind.FIRST_VALUE, SqlKind.LAST_VALUE,
            SqlKind.OTHER_FUNCTION);

    public static void validateWindows(Window window) {
      int numGroups = window.groups.size();
      // For Phase 1 we only handle single window groups
      Preconditions.checkState(numGroups == 1,
          String.format("Currently only 1 window group is supported, query has %d groups", numGroups));

      // Validate that only supported window aggregation functions are present
      Window.Group windowGroup = window.groups.get(0);
      validateWindowAggCallsSupported(windowGroup);

      // Validate the frame
      validateWindowFrames(windowGroup);
    }

    /**
     * Replaces the reference to literal arguments in the window group with the actual literal values.
     * NOTE: {@link Window} has a field called "constants" which contains the literal values. If the input reference is
     * beyond the window input size, it is a reference to the constants.
     */
    public static Window.Group updateLiteralArgumentsInWindowGroup(Window window) {
      Window.Group oldWindowGroup = window.groups.get(0);
      RelNode input = unboxRel(window.getInput());
      int numInputFields = input.getRowType().getFieldCount();
      List<RexNode> projects = input instanceof Project ? ((Project) input).getProjects() : null;

      List<Window.RexWinAggCall> newAggCallWindow = new ArrayList<>(oldWindowGroup.aggCalls.size());
      boolean windowChanged = false;
      for (Window.RexWinAggCall oldAggCall : oldWindowGroup.aggCalls) {
        boolean changed = false;
        List<RexNode> oldOperands = oldAggCall.getOperands();
        List<RexNode> newOperands = new ArrayList<>(oldOperands.size());
        for (RexNode oldOperand : oldOperands) {
          RexLiteral literal = getLiteral(oldOperand, numInputFields, window.constants, projects);
          if (literal != null) {
            newOperands.add(literal);
            changed = true;
            windowChanged = true;
          } else {
            newOperands.add(oldOperand);
          }
        }
        if (changed) {
          newAggCallWindow.add(
              new Window.RexWinAggCall((SqlAggFunction) oldAggCall.getOperator(), oldAggCall.type, newOperands,
                  oldAggCall.ordinal, oldAggCall.distinct, oldAggCall.ignoreNulls));
        } else {
          newAggCallWindow.add(oldAggCall);
        }
      }

      RexWindowBound lowerBound = oldWindowGroup.lowerBound;
      RexNode offset = lowerBound.getOffset();
      if (offset != null) {
        RexLiteral literal = getLiteral(offset, numInputFields, window.constants, projects);
        if (literal == null) {
          throw new IllegalStateException(
              "Could not read window lower bound literal value from window group: " + oldWindowGroup);
        }
        lowerBound = lowerBound.isPreceding() ? RexWindowBounds.preceding(literal) : RexWindowBounds.following(literal);
        windowChanged = true;
      }
      RexWindowBound upperBound = oldWindowGroup.upperBound;
      offset = upperBound.getOffset();
      if (offset != null) {
        RexLiteral literal = getLiteral(offset, numInputFields, window.constants, projects);
        if (literal == null) {
          throw new IllegalStateException(
              "Could not read window upper bound literal value from window group: " + oldWindowGroup);
        }
        upperBound = upperBound.isFollowing() ? RexWindowBounds.following(literal) : RexWindowBounds.preceding(literal);
        windowChanged = true;
      }

      return windowChanged ? new Window.Group(oldWindowGroup.keys, oldWindowGroup.isRows, lowerBound, upperBound,
          oldWindowGroup.exclude, oldWindowGroup.orderKeys, newAggCallWindow) : oldWindowGroup;
    }

    private static void validateWindowAggCallsSupported(Window.Group windowGroup) {
      for (Window.RexWinAggCall aggCall : windowGroup.aggCalls) {
        SqlKind aggKind = aggCall.getKind();
        Preconditions.checkState(SUPPORTED_WINDOW_FUNCTION_KIND.contains(aggKind),
            String.format("Unsupported Window function kind: %s. Only aggregation functions are supported!", aggKind));
      }
    }

    private static void validateWindowFrames(Window.Group windowGroup) {
      RexWindowBound lowerBound = windowGroup.lowerBound;
      RexWindowBound upperBound = windowGroup.upperBound;

      boolean hasOffset = (lowerBound.isPreceding() && !lowerBound.isUnbounded()) || (upperBound.isFollowing()
          && !upperBound.isUnbounded());

      if (!windowGroup.isRows) {
        Preconditions.checkState(!hasOffset, "RANGE window frame with offset PRECEDING / FOLLOWING is not supported");
      }
    }

    @Nullable
    private static RexLiteral getLiteral(RexNode rexNode, int numInputFields, ImmutableList<RexLiteral> constants,
        @Nullable List<RexNode> projects) {
      if (!(rexNode instanceof RexInputRef)) {
        return null;
      }
      int index = ((RexInputRef) rexNode).getIndex();
      if (index >= numInputFields) {
        return constants.get(index - numInputFields);
      }
      if (projects != null) {
        RexNode project = projects.get(index);
        if (project instanceof RexLiteral) {
          return (RexLiteral) project;
        }
      }
      return null;
    }
  }
}
