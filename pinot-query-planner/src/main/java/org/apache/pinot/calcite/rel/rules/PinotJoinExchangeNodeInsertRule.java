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
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.pinot.calcite.rel.logical.PinotLogicalExchange;
import org.apache.pinot.calcite.rel.logical.PinotLogicalJoin;
import org.apache.pinot.query.planner.plannode.JoinNode.JoinStrategy;


/**
 * Special rule for Pinot, this rule is fixed to always insert exchange after JOIN node.
 */
public class PinotJoinExchangeNodeInsertRule extends RelOptRule {
  public static final PinotJoinExchangeNodeInsertRule INSTANCE =
      new PinotJoinExchangeNodeInsertRule(PinotRuleUtils.PINOT_REL_FACTORY);

  public PinotJoinExchangeNodeInsertRule(RelBuilderFactory factory) {
    super(operand(Join.class, any()), factory, null);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    Join join = call.rel(0);
    return !PinotRuleUtils.isExchange(join.getLeft()) && !PinotRuleUtils.isExchange(join.getRight());
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    PinotLogicalJoin join = call.rel(0);
    RelNode left = PinotRuleUtils.unboxRel(join.getInput(0));
    RelNode right = PinotRuleUtils.unboxRel(join.getInput(1));
    JoinInfo joinInfo = join.analyzeCondition();
    RelNode newLeft;
    RelNode newRight;
    if (join.getJoinStrategy() == JoinStrategy.LOOKUP) {
      // Lookup join - add local exchange on the left side
      newLeft = PinotLogicalExchange.create(left, RelDistributions.SINGLETON);
      newRight = right;
    } else {
      // Regular join - add exchange on both sides
      if (joinInfo.leftKeys.isEmpty()) {
        // Broadcast the right side if there is no join key
        newLeft = PinotLogicalExchange.create(left, RelDistributions.RANDOM_DISTRIBUTED);
        newRight = PinotLogicalExchange.create(right, RelDistributions.BROADCAST_DISTRIBUTED);
      } else {
        // Use hash exchange when there are join keys
        newLeft = PinotLogicalExchange.create(left, RelDistributions.hash(joinInfo.leftKeys));
        newRight = PinotLogicalExchange.create(right, RelDistributions.hash(joinInfo.rightKeys));
      }
    }
    call.transformTo(join.copy(join.getTraitSet(), join.getCondition(), newLeft, newRight, join.getJoinType(),
        join.isSemiJoinDone()));
  }
}
