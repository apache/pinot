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
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.hint.PinotHintOptions;
import org.apache.calcite.rel.hint.PinotHintStrategyTable;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.PinotLogicalExchange;
import org.apache.calcite.tools.RelBuilderFactory;


/**
 * Special rule for Pinot, this rule is fixed to always insert exchange after JOIN node.
 */
public class PinotJoinExchangeNodeInsertRule extends RelOptRule {
  public static final PinotJoinExchangeNodeInsertRule INSTANCE =
      new PinotJoinExchangeNodeInsertRule(PinotRuleUtils.PINOT_REL_FACTORY);

  public PinotJoinExchangeNodeInsertRule(RelBuilderFactory factory) {
    super(operand(LogicalJoin.class, any()), factory, null);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    if (call.rels.length < 1) {
      return false;
    }
    if (call.rel(0) instanceof Join) {
      Join join = call.rel(0);
      return !PinotRuleUtils.isExchange(join.getLeft()) && !PinotRuleUtils.isExchange(join.getRight());
    }
    return false;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Join join = call.rel(0);
    RelNode leftInput = join.getInput(0);
    RelNode rightInput = join.getInput(1);

    RelNode leftExchange;
    RelNode rightExchange;
    JoinInfo joinInfo = join.analyzeCondition();

    boolean isColocatedJoin =
        PinotHintStrategyTable.isHintOptionTrue(join.getHints(), PinotHintOptions.JOIN_HINT_OPTIONS,
            PinotHintOptions.JoinHintOptions.IS_COLOCATED_BY_JOIN_KEYS);
    if (isColocatedJoin) {
      // join exchange are colocated, we should directly pass through via join key
      leftExchange = PinotLogicalExchange.create(leftInput, RelDistributions.SINGLETON);
      rightExchange = PinotLogicalExchange.create(rightInput, RelDistributions.SINGLETON);
    } else if (joinInfo.leftKeys.isEmpty()) {
      // when there's no JOIN key, use broadcast.
      leftExchange = PinotLogicalExchange.create(leftInput, RelDistributions.RANDOM_DISTRIBUTED);
      rightExchange = PinotLogicalExchange.create(rightInput, RelDistributions.BROADCAST_DISTRIBUTED);
    } else {
      // when join key exists, use hash distribution.
      leftExchange = PinotLogicalExchange.create(leftInput, RelDistributions.hash(joinInfo.leftKeys));
      rightExchange = PinotLogicalExchange.create(rightInput, RelDistributions.hash(joinInfo.rightKeys));
    }

    RelNode newJoinNode =
        new LogicalJoin(join.getCluster(), join.getTraitSet(), join.getHints(), leftExchange, rightExchange,
            join.getCondition(), join.getVariablesSet(), join.getJoinType(), join.isSemiJoinDone(),
            ImmutableList.copyOf(join.getSystemFieldList()));
    call.transformTo(newJoinNode);
  }
}
