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
package org.apache.pinot.query.rules;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalExchange;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.pinot.query.planner.PlannerUtils;
import org.apache.pinot.query.planner.hints.PinotRelationalHints;


/**
 * Special rule for Pinot, this rule is fixed to always insert exchange after JOIN node.
 */
public class PinotJoinExchangeNodeInsertRule extends RelOptRule {
  public static final PinotJoinExchangeNodeInsertRule INSTANCE =
      new PinotJoinExchangeNodeInsertRule(RelFactories.LOGICAL_BUILDER);

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
      return !isExchange(join.getLeft()) && !isExchange(join.getRight());
    }
    return false;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    // TODO: this only works for single equality JOIN. add generic condition parser
    Join join = call.rel(0);
    RelNode leftInput = join.getInput(0);
    RelNode rightInput = join.getInput(1);

    RelNode leftExchange;
    RelNode rightExchange;
    List<RelHint> hints = join.getHints();
    if (hints.contains(PinotRelationalHints.USE_BROADCAST_DISTRIBUTE)) {
      // TODO: determine which side should be the broadcast table based on table metadata
      // TODO: support SINGLETON exchange if the non-broadcast table size is small enough to stay local.
      leftExchange = LogicalExchange.create(leftInput, RelDistributions.RANDOM_DISTRIBUTED);
      rightExchange = LogicalExchange.create(rightInput, RelDistributions.BROADCAST_DISTRIBUTED);
    } else { // if (hints.contains(PinotRelationalHints.USE_HASH_DISTRIBUTE)) {
      RexCall joinCondition = (RexCall) join.getCondition();
      int leftNodeOffset = join.getLeft().getRowType().getFieldNames().size();
      List<List<Integer>> conditions = PlannerUtils.parseJoinConditions(joinCondition, leftNodeOffset);
      leftExchange = LogicalExchange.create(leftInput,
          RelDistributions.hash(conditions.get(0)));
      rightExchange = LogicalExchange.create(rightInput,
          RelDistributions.hash(conditions.get(1)));
    }

    RelNode newJoinNode =
        new LogicalJoin(join.getCluster(), join.getTraitSet(), leftExchange, rightExchange, join.getCondition(),
            join.getVariablesSet(), join.getJoinType(), join.isSemiJoinDone(),
            ImmutableList.copyOf(join.getSystemFieldList()));

    call.transformTo(newJoinNode);
  }

  private static boolean isExchange(RelNode rel) {
    RelNode reference = rel;
    if (reference instanceof HepRelVertex) {
      reference = ((HepRelVertex) reference).getCurrentRel();
    }
    return reference instanceof Exchange;
  }
}
