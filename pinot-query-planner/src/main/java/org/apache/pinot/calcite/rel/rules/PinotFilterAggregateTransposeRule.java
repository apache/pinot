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

import java.util.List;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.rules.FilterAggregateTransposeRule;


/// Same as [FilterAggregateTransposeRule], except that a filter that reads an MV key of an MV aggregate stays above
/// the aggregate.
///
/// An MV key is a group key `ARRAY_TO_MV(expr)` (see [ArrayToMvGroupKeys#detect]). A condition on it below the
/// aggregate becomes a multi-value predicate in the leaf stage. That predicate keeps the whole row when any element
/// matches, so the other elements of the row also get groups. For example, `GROUP BY ARRAY_TO_MV(tags) HAVING
/// ARRAY_TO_MV(tags) = 'vip'` would also return the group of each other tag of a row that has `vip`. So the whole
/// filter stays above the aggregate, where it reads one element for each group.
///
/// The rule is stateless and thread-safe.
public class PinotFilterAggregateTransposeRule extends FilterAggregateTransposeRule {

  public static PinotFilterAggregateTransposeRule instanceWithDescription(String description) {
    return new PinotFilterAggregateTransposeRule((Config) Config.DEFAULT.withDescription(description));
  }

  private PinotFilterAggregateTransposeRule(Config config) {
    super(config);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    if (!readsMvKey(call.rel(0), call.rel(1))) {
      super.onMatch(call);
    }
  }

  /// Returns whether `filter` reads a group key of `aggregate` that is `ARRAY_TO_MV(expr)`. The group keys are the
  /// first output columns of the aggregate. This rule can run before the filters under the aggregate are pushed
  /// down, so the key is traced with [ArrayToMvGroupKeys#isArrayToMvColumn].
  private static boolean readsMvKey(Filter filter, Aggregate aggregate) {
    List<Integer> groupColumns = aggregate.getGroupSet().asList();
    for (int column : RelOptUtil.InputFinder.bits(filter.getCondition())) {
      if (column < groupColumns.size()
          && ArrayToMvGroupKeys.isArrayToMvColumn(aggregate.getInput(), groupColumns.get(column))) {
        return true;
      }
    }
    return false;
  }
}
