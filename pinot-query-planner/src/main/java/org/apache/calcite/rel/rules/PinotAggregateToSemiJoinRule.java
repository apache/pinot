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

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.checkerframework.checker.nullness.qual.Nullable;


/**
 * SemiJoinRule that matches an Aggregate on top of a Join with an Aggregate
 * as its right child.
 *
 * @see CoreRules#PROJECT_TO_SEMI_JOIN
 */
public class PinotAggregateToSemiJoinRule extends RelOptRule {
  public static final PinotAggregateToSemiJoinRule INSTANCE =
      new PinotAggregateToSemiJoinRule(PinotRuleUtils.PINOT_REL_FACTORY);

  public PinotAggregateToSemiJoinRule(RelBuilderFactory factory) {
    super(operand(LogicalAggregate.class, any()), factory, null);
  }

  @Override
  @SuppressWarnings("rawtypes")
  public boolean matches(RelOptRuleCall call) {
    final Aggregate topAgg = call.rel(0);
    if (!PinotRuleUtils.isJoin(topAgg.getInput())) {
      return false;
    }
    final Join join = (Join) PinotRuleUtils.unboxRel(topAgg.getInput());
    return PinotRuleUtils.isAggregate(join.getInput(1));
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final Aggregate topAgg = call.rel(0);
    final Join join = (Join) PinotRuleUtils.unboxRel(topAgg.getInput());
    final RelNode left = PinotRuleUtils.unboxRel(join.getInput(0));
    final Aggregate rightAgg = (Aggregate) PinotRuleUtils.unboxRel(join.getInput(1));
    perform(call, topAgg, join, left, rightAgg);
  }


  protected void perform(RelOptRuleCall call, @Nullable Aggregate topAgg,
      Join join, RelNode left, Aggregate rightAgg) {
    final RelOptCluster cluster = join.getCluster();
    final RexBuilder rexBuilder = cluster.getRexBuilder();
    if (topAgg != null) {
      final ImmutableBitSet aggBits = ImmutableBitSet.of(RelOptUtil.getAllFields(topAgg));
      final ImmutableBitSet rightBits =
          ImmutableBitSet.range(left.getRowType().getFieldCount(),
              join.getRowType().getFieldCount());
      if (aggBits.intersects(rightBits)) {
        return;
      }
    } else {
      if (join.getJoinType().projectsRight()
          && !isEmptyAggregate(rightAgg)) {
        return;
      }
    }
    final JoinInfo joinInfo = join.analyzeCondition();
    if (!joinInfo.rightSet().equals(
        ImmutableBitSet.range(rightAgg.getGroupCount()))) {
      // Rule requires that aggregate key to be the same as the join key.
      // By the way, neither a super-set nor a sub-set would work.
      return;
    }
    if (!joinInfo.isEqui()) {
      return;
    }
    final RelBuilder relBuilder = call.builder();
    relBuilder.push(left);
    switch (join.getJoinType()) {
      case SEMI:
      case INNER:
        final List<Integer> newRightKeyBuilder = new ArrayList<>();
        final List<Integer> aggregateKeys = rightAgg.getGroupSet().asList();
        for (int key : joinInfo.rightKeys) {
          newRightKeyBuilder.add(aggregateKeys.get(key));
        }
        final ImmutableIntList newRightKeys = ImmutableIntList.copyOf(newRightKeyBuilder);
        relBuilder.push(rightAgg.getInput());
        final RexNode newCondition =
            RelOptUtil.createEquiJoinCondition(relBuilder.peek(2, 0),
                joinInfo.leftKeys, relBuilder.peek(2, 1), newRightKeys,
                rexBuilder);
        relBuilder.semiJoin(newCondition).hints(join.getHints());
        break;

      case LEFT:
        // The right-hand side produces no more than 1 row (because of the
        // Aggregate) and no fewer than 1 row (because of LEFT), and therefore
        // we can eliminate the semi-join.
        break;

      default:
        throw new AssertionError(join.getJoinType());
    }
    if (topAgg != null) {
      relBuilder.aggregate(relBuilder.groupKey(topAgg.getGroupSet()), topAgg.getAggCallList());
    }
    final RelNode relNode = relBuilder.build();
    call.transformTo(relNode);
  }

  private static boolean isEmptyAggregate(Aggregate aggregate) {
    return aggregate.getRowType().getFieldCount() == 0;
  }
}
