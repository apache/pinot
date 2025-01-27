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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.pinot.calcite.rel.hint.PinotHintOptions;
import org.apache.pinot.calcite.rel.hint.PinotHintStrategyTable;
import org.apache.pinot.calcite.rel.logical.PinotLogicalExchange;
import org.apache.pinot.calcite.rel.logical.PinotRelExchangeType;


/**
 * Special rule for Pinot, this rule transposing an INNER JOIN into dynamic broadcast join for the leaf stage.
 *
 * <p>Consider the following INNER JOIN plan
 *
 *                  ...                                     ...
 *                   |                                       |
 *             [ Transform ]                           [ Transform ]
 *                   |                                       |
 *             [ Inner Join ]                     [ Pass-through xChange ]
 *             /            \                          /
 *        [xChange]      [xChange]              [Inner Join] <----
 *           /                \                      /            \
 *     [ Transform ]     [ Transform ]         [ Transform ]       \
 *          |                  |                    |               \
 *     [Proj/Filter]     [Proj/Filter]         [Proj/Filter]  <------\
 *          |                  |                    |                 \
 *     [Table Scan ]     [Table Scan ]         [Table Scan ]       [xChange]
 *                                                                      \
 *                                                                 [ Transform ]
 *                                                                       |
 *                                                                 [Proj/Filter]
 *                                                                       |
 *                                                                 [Table Scan ]
 *
 * <p> This rule is one part of the overall mechanism and this rule only does the following
 *
 *                 ...                                      ...
 *                  |                                        |
 *            [ Transform ]                             [ Transform ]
 *                  |                                       /
 *            [ Inner Join ]                     [Pass-through xChange]
 *            /            \                            /
 *       [xChange]      [xChange]          |----[Dyn. Broadcast]   <-----
 *          /                \             |           |                 \
 *    [ Transform ]     [ Transform ]      |----> [ Inner Join ]      [xChange]
 *         |                  |            |           |                   \
 *    [Proj/Filter]     [Proj/Filter]      |      [ Transform ]       [ Transform ]
 *         |                  |            |          |                    |
 *    [Table Scan ]     [Table Scan ]      |----> [Proj/Filter]       [Proj/Filter]
 *                                                     |                    |
 *                                                [Table Scan ]       [Table Scan ]
 *
 *
 *
 * <p> The next part to extend the Dynamic broadcast into the Proj/Filter operator happens in the runtime.
 *
 * <p> This rewrite is only useful if we can ensure that:
 * <ul>
 *   <li>new plan can leverage the indexes and fast filtering of the leaf-stage Pinot server processing logic.</li>
 *   <li>
 *     data sending over xChange should be relatively small comparing to data would've been selected out if the dynamic
 *     broadcast is not applied.
 *   </li>
 *   <li>
 *     since leaf-stage operator can only process a typical Pinot V1 engine chain-operator chain. This means the entire
 *     right-table will be ship over and persist in memory before the dynamic broadcast can occur. memory foot print
 *     will be high so this rule should be use carefully until we have cost-based optimization.
 *   </li>
 *   <li>
 *     if the dynamic broadcast stage is broadcasting to a left-table that's pre-partitioned with the join key, then the
 *     right-table will be ship over and persist in memory using hash distribution. memory foot print will be
 *     relatively low comparing to non-partitioned.
 *   </li>
 *   <li>
 *     if the dynamic broadcast stage operating on a table with the same partition scheme as the left-table, then there
 *     is not going to be any network shuffling overhead. both memory and latency overhead will be low.
 *   </li>
 * </ul>
 *
 * TODO #1: Only support SEMI-JOIN, once JOIN operator is supported by leaf-stage we should allow it to match
 *   @see <a href="https://github.com/apache/pinot/pull/10565/>
 * TODO #2: Only convert to dynamic broadcast from right-to-left, allow option to specify dynamic broadcast direction.
 * TODO #3: Only convert to dynamic broadcast if left is leaf stage, allow the option for intermediate stage.
 * TODO #4: currently adding a pass-through after join, support leaf-stage to chain arbitrary operator(s) next.
 */
public class PinotJoinToDynamicBroadcastRule extends RelOptRule {
  public static final PinotJoinToDynamicBroadcastRule INSTANCE =
      new PinotJoinToDynamicBroadcastRule(PinotRuleUtils.PINOT_REL_FACTORY);

  public PinotJoinToDynamicBroadcastRule(RelBuilderFactory factory) {
    super(operand(Join.class, any()), factory, null);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    Join join = call.rel(0);

    // Do not apply this rule if join strategy is explicitly set to something other than dynamic broadcast
    String joinStrategy = PinotHintOptions.JoinHintOptions.getJoinStrategyHint(join);
    if (joinStrategy != null && !joinStrategy.equals(
        PinotHintOptions.JoinHintOptions.DYNAMIC_BROADCAST_JOIN_STRATEGY)) {
      return false;
    }

    // Do not apply this rule if it is not a SEMI join
    JoinInfo joinInfo = join.analyzeCondition();
    if (join.getJoinType() != JoinRelType.SEMI || !joinInfo.nonEquiConditions.isEmpty()
        || joinInfo.leftKeys.size() != 1) {
      return false;
    }

    // Apply this rule if the left side can be pushed as dynamic exchange
    RelNode left = ((HepRelVertex) join.getLeft()).getCurrentRel();
    RelNode right = ((HepRelVertex) join.getRight()).getCurrentRel();
    return left instanceof Exchange && right instanceof Exchange && PinotRuleUtils.canPushDynamicBroadcastToLeaf(
        left.getInput(0));
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Join join = call.rel(0);
    Exchange left = (Exchange) ((HepRelVertex) join.getLeft()).getCurrentRel();
    Exchange right = (Exchange) ((HepRelVertex) join.getRight()).getCurrentRel();

    // when colocated join hint is given, dynamic broadcast exchange can be hash-distributed b/c
    //    1. currently, dynamic broadcast only works against main table off leaf-stage; (e.g. receive node on leaf)
    //    2. when hash key are the same but hash functions are different, it can be done via normal hash shuffle.
    boolean isColocatedJoin =
        PinotHintStrategyTable.isHintOptionTrue(join.getHints(), PinotHintOptions.JOIN_HINT_OPTIONS,
            PinotHintOptions.JoinHintOptions.IS_COLOCATED_BY_JOIN_KEYS);
    RelDistribution relDistribution = isColocatedJoin ? RelDistributions.hash(join.analyzeCondition().rightKeys)
        : RelDistributions.BROADCAST_DISTRIBUTED;
    PinotLogicalExchange dynamicBroadcastExchange =
        PinotLogicalExchange.create(right.getInput(), relDistribution, PinotRelExchangeType.PIPELINE_BREAKER);

    call.transformTo(join.copy(join.getTraitSet(), join.getCondition(), left.getInput(), dynamicBroadcastExchange,
        join.getJoinType(), join.isSemiJoinDone()));
  }
}
