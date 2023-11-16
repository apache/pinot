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

import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.hint.PinotHintOptions;
import org.apache.calcite.rel.hint.PinotHintStrategyTable;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.PinotLogicalExchange;
import org.apache.calcite.rel.logical.PinotRelExchangeType;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.zookeeper.common.StringUtils;


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
    super(operand(LogicalJoin.class, any()), factory, null);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    if (call.rels.length < 1 || !(call.rel(0) instanceof Join)) {
      return false;
    }
    Join join = call.rel(0);
    String joinStrategyString = PinotHintStrategyTable.getHintOption(join.getHints(),
        PinotHintOptions.JOIN_HINT_OPTIONS, PinotHintOptions.JoinHintOptions.JOIN_STRATEGY);
    List<String> joinStrategies = joinStrategyString != null ? StringUtils.split(joinStrategyString, ",")
        : Collections.emptyList();
    boolean explicitOtherStrategy = joinStrategies.size() > 0
        && !joinStrategies.contains(PinotHintOptions.JoinHintOptions.DYNAMIC_BROADCAST_JOIN_STRATEGY);

    JoinInfo joinInfo = join.analyzeCondition();
    RelNode left = join.getLeft() instanceof HepRelVertex ? ((HepRelVertex) join.getLeft()).getCurrentRel()
        : join.getLeft();
    RelNode right = join.getRight() instanceof HepRelVertex ? ((HepRelVertex) join.getRight()).getCurrentRel()
        : join.getRight();
    return left instanceof Exchange && right instanceof Exchange
        && PinotRuleUtils.canPushDynamicBroadcastToLeaf(left.getInput(0))
        // default enable dynamic broadcast for SEMI join unless other join strategy were specified
        && (!explicitOtherStrategy && join.getJoinType() == JoinRelType.SEMI && joinInfo.nonEquiConditions.isEmpty());
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Join join = call.rel(0);
    PinotLogicalExchange left = (PinotLogicalExchange) (join.getLeft() instanceof HepRelVertex
        ? ((HepRelVertex) join.getLeft()).getCurrentRel() : join.getLeft());
    PinotLogicalExchange right = (PinotLogicalExchange) (join.getRight() instanceof HepRelVertex
        ? ((HepRelVertex) join.getRight()).getCurrentRel() : join.getRight());

    boolean isColocatedJoin = PinotHintStrategyTable.isHintOptionTrue(join.getHints(),
        PinotHintOptions.JOIN_HINT_OPTIONS, PinotHintOptions.JoinHintOptions.IS_COLOCATED_BY_JOIN_KEYS);
    PinotLogicalExchange dynamicBroadcastExchange = isColocatedJoin
        ? PinotLogicalExchange.create(right.getInput(), RelDistributions.hash(join.analyzeCondition().rightKeys),
        PinotRelExchangeType.PIPELINE_BREAKER)
        : PinotLogicalExchange.create(right.getInput(), RelDistributions.BROADCAST_DISTRIBUTED,
            PinotRelExchangeType.PIPELINE_BREAKER);
    Join dynamicFilterJoin =
        new LogicalJoin(join.getCluster(), join.getTraitSet(), left.getInput(), dynamicBroadcastExchange,
            join.getCondition(), join.getVariablesSet(), join.getJoinType(), join.isSemiJoinDone(),
            ImmutableList.copyOf(join.getSystemFieldList()));
    call.transformTo(dynamicFilterJoin);
  }
}
