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
package org.apache.calcite.rel.rules;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.logical.LogicalExchange;
import org.apache.calcite.rel.logical.LogicalSortExchange;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilderFactory;


/**
 * Special rule for Pinot, this rule is fixed to always insert an exchange or sort exchange below the WINDOW node.
 * TODO:
 *     1. Add support for more than one window group
 *     2. Add support for functions other than aggregation functions (AVG, COUNT, MAX, MIN, SUM)
 *     3. Add support for custom frames
 */
public class PinotWindowExchangeNodeInsertRule extends RelOptRule {
  public static final PinotWindowExchangeNodeInsertRule INSTANCE =
      new PinotWindowExchangeNodeInsertRule(PinotRuleUtils.PINOT_REL_FACTORY);

  // Supported window functions
  private static final Set<SqlKind> SUPPORTED_WINDOW_FUNCTION_KIND = ImmutableSet.of(SqlKind.SUM, SqlKind.SUM0,
      SqlKind.MIN, SqlKind.MAX, SqlKind.COUNT, SqlKind.OTHER_FUNCTION);

  public PinotWindowExchangeNodeInsertRule(RelBuilderFactory factory) {
    super(operand(LogicalWindow.class, any()), factory, null);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    if (call.rels.length < 1) {
      return false;
    }
    if (call.rel(0) instanceof Window) {
      Window window = call.rel(0);
      // Only run the rule if the input isn't already an exchange node
      return !PinotRuleUtils.isExchange(window.getInput());
    }
    return false;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Window window = call.rel(0);
    RelNode windowInput = window.getInput();

    // Perform all validations
    validateWindows(window);

    Window.Group windowGroup = window.groups.get(0);
    if (windowGroup.keys.isEmpty() && windowGroup.orderKeys.getKeys().isEmpty()) {
      // Empty OVER()
      // Add a single LogicalExchange for empty OVER() since no sort is required
      LogicalExchange exchange = LogicalExchange.create(windowInput, RelDistributions.hash(Collections.emptyList()));
      call.transformTo(
          LogicalWindow.create(window.getTraitSet(), exchange, window.constants, window.getRowType(), window.groups));
    } else if (windowGroup.keys.isEmpty() && !windowGroup.orderKeys.getKeys().isEmpty()) {
      // Only ORDER BY
      // Add a LogicalSortExchange with collation on the order by key(s) and an empty hash partition key
      LogicalSortExchange sortExchange = LogicalSortExchange.create(windowInput,
          RelDistributions.hash(Collections.emptyList()), windowGroup.orderKeys);
      call.transformTo(LogicalWindow.create(window.getTraitSet(), sortExchange, window.constants, window.getRowType(),
          window.groups));
    } else {
      // All other variants
      // Assess whether this is a PARTITION BY only query or not (includes queries of the type where PARTITION BY and
      // ORDER BY key(s) are the same)
      boolean isPartitionByOnly = isPartitionByOnlyQuery(windowGroup);

      if (isPartitionByOnly) {
        // Only PARTITION BY or PARTITION BY and ORDER BY on the same key(s)
        // Add a LogicalExchange hashed on the partition by keys
        LogicalExchange exchange = LogicalExchange.create(windowInput,
            RelDistributions.hash(windowGroup.keys.toList()));
        call.transformTo(LogicalWindow.create(window.getTraitSet(), exchange, window.constants, window.getRowType(),
            window.groups));
      } else {
        // PARTITION BY and ORDER BY on different key(s)
        // Add a LogicalSortExchange hashed on the partition by keys and collation based on order by keys
        LogicalSortExchange sortExchange = LogicalSortExchange.create(windowInput,
            RelDistributions.hash(windowGroup.keys.toList()), windowGroup.orderKeys);
        call.transformTo(LogicalWindow.create(window.getTraitSet(), sortExchange, window.constants, window.getRowType(),
            window.groups));
      }
    }
  }

  private void validateWindows(Window window) {
    int numGroups = window.groups.size();
    // For Phase 1 we only handle single window groups
    Preconditions.checkState(numGroups <= 1,
        String.format("Currently only 1 window group is supported, query has %d groups", numGroups));

    // Validate that only supported window aggregation functions are present
    Window.Group windowGroup = window.groups.get(0);
    validateWindowAggCallsSupported(windowGroup);

    // Validate the frame
    validateWindowFrames(windowGroup);
  }

  private void validateWindowAggCallsSupported(Window.Group windowGroup) {
    for (int i = 0; i < windowGroup.aggCalls.size(); i++) {
      Window.RexWinAggCall aggCall = windowGroup.aggCalls.get(i);
      SqlKind aggKind = aggCall.getKind();
      Preconditions.checkState(SUPPORTED_WINDOW_FUNCTION_KIND.contains(aggKind),
          String.format("Unsupported Window function kind: %s. Only aggregation functions are supported!", aggKind));
    }
  }

  private void validateWindowFrames(Window.Group windowGroup) {
    // For Phase 1 only the default frame is supported
    Preconditions.checkState(!windowGroup.isRows, "Default frame must be of type RANGE and not ROWS");
    Preconditions.checkState(windowGroup.lowerBound.isPreceding() && windowGroup.lowerBound.isUnbounded(),
        String.format("Lower bound must be UNBOUNDED PRECEDING but it is: %s", windowGroup.lowerBound));
    if (windowGroup.orderKeys.getKeys().isEmpty()) {
      Preconditions.checkState(windowGroup.upperBound.isFollowing() && windowGroup.upperBound.isUnbounded(),
          String.format("Upper bound must be UNBOUNDED PRECEDING but it is: %s", windowGroup.upperBound));
    } else {
      Preconditions.checkState(windowGroup.upperBound.isCurrentRow(),
          String.format("Upper bound must be CURRENT ROW but it is: %s", windowGroup.upperBound));
    }
  }

  private boolean isPartitionByOnlyQuery(Window.Group windowGroup) {
    boolean isPartitionByOnly = false;
    if (windowGroup.orderKeys.getKeys().isEmpty()) {
      return true;
    }

    if (windowGroup.orderKeys.getKeys().size() == windowGroup.keys.asList().size()) {
      Set<Integer> partitionByKeyList = new HashSet<>(windowGroup.keys.toList());
      Set<Integer> orderByKeyList = new HashSet<>(windowGroup.orderKeys.getKeys());
      isPartitionByOnly = partitionByKeyList.equals(orderByKeyList);
    }
    return isPartitionByOnly;
  }
}
