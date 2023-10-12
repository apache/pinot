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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.hint.PinotHintOptions;
import org.apache.calcite.rel.hint.PinotHintStrategyTable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.PinotLogicalExchange;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.pinot.query.planner.plannode.AggregateNode;


public class PinotAggregateExchangeNodeRemoveRule extends RelOptRule {
  public static final PinotAggregateExchangeNodeRemoveRule INSTANCE =
      new PinotAggregateExchangeNodeRemoveRule(PinotRuleUtils.PINOT_REL_FACTORY);

  public PinotAggregateExchangeNodeRemoveRule(RelBuilderFactory factory) {
    super(operand(PinotLogicalExchange.class,
        some(operand(LogicalAggregate.class, some(operand(PinotLogicalExchange.class, any()))))), factory, null);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    if (call.rels.length < 1) {
      return false;
    }
    if (call.rel(1) instanceof Aggregate) {
      Aggregate agg = call.rel(1);
      ImmutableList<RelHint> hints = agg.getHints();
      return AggregateNode.AggType.LEAF.name().equals(
          PinotHintStrategyTable.getHintOption(hints, PinotHintOptions.INTERNAL_AGG_OPTIONS,
              PinotHintOptions.InternalAggregateOptions.AGG_TYPE));
    }
    return false;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final PinotLogicalExchange topExchange = call.rel(0);
    final Aggregate agg = (Aggregate) PinotRuleUtils.unboxRel(topExchange.getInput());
    final PinotLogicalExchange bottomExchange = (PinotLogicalExchange) PinotRuleUtils.unboxRel(agg.getInput());
    final RelNode input = PinotRuleUtils.unboxRel(bottomExchange.getInput());
    Aggregate newAgg =
        new LogicalAggregate(agg.getCluster(), agg.getTraitSet(), agg.getHints(), input, agg.getGroupSet(),
            agg.getGroupSets(), agg.getAggCallList());
    PinotLogicalExchange newExchange = PinotLogicalExchange.create(newAgg, topExchange.getDistribution());
    call.transformTo(newExchange);
  }
}
