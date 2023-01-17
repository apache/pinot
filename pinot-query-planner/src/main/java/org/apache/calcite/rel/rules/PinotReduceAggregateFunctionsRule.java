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

import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.PinotFourthMomentAggregateFunction;
import org.apache.calcite.sql.fun.PinotKurtosisAggregateFunction;
import org.apache.calcite.sql.fun.PinotOperatorTable;
import org.apache.calcite.sql.fun.PinotSkewnessAggregateFunction;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.CompositeList;


/**
 * This rule rewrites aggregate functions when necessary for Pinot's
 * multistage engine. For example, SKEWNESS must be rewritten into two
 * parts: a multi-stage FOURTH_MOMENT calculation and then a scalar function
 * that reduces the moment into the skewness at the end. This is to ensure
 * that the aggregation computation can merge partial results from different
 * intermediate nodes before reducing it into the final result.
 *
 * <p>This implementation follows closely with Calcite's
 * {@link AggregateReduceFunctionsRule}.
 */
public class PinotReduceAggregateFunctionsRule extends RelOptRule {

  public static final PinotReduceAggregateFunctionsRule INSTANCE =
      new PinotReduceAggregateFunctionsRule(PinotRuleUtils.PINOT_REL_FACTORY);

  private static final Set<String> FUNCTIONS = ImmutableSet.of(
      PinotSkewnessAggregateFunction.SKEWNESS,
      PinotKurtosisAggregateFunction.KURTOSIS
  );

  protected PinotReduceAggregateFunctionsRule(RelBuilderFactory factory) {
    super(operand(Aggregate.class, any()), factory, null);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    if (call.rels.length < 1) {
      return false;
    }

    if (call.rel(0) instanceof Aggregate) {
      Aggregate agg = call.rel(0);
      for (AggregateCall aggCall : agg.getAggCallList()) {
        if (canReduce(aggCall)) {
          return true;
        }
      }
    }

    return false;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Aggregate oldAggRel = call.rel(0);
    reduceAggs(call, oldAggRel);
  }

  private void reduceAggs(RelOptRuleCall ruleCall, Aggregate oldAggRel) {
    RexBuilder rexBuilder = oldAggRel.getCluster().getRexBuilder();

    List<AggregateCall> oldCalls = oldAggRel.getAggCallList();
    final int groupCount = oldAggRel.getGroupCount();

    final List<AggregateCall> newCalls = new ArrayList<>();
    final Map<AggregateCall, RexNode> aggCallMapping = new HashMap<>();

    final List<RexNode> projList = new ArrayList<>();

    // pass through group key
    for (int i = 0; i < groupCount; i++) {
      projList.add(rexBuilder.makeInputRef(oldAggRel, i));
    }

    // List of input expressions. If a particular aggregate needs more, it
    // will add an expression to the end, and we will create an extra project
    final RelBuilder relBuilder = ruleCall.builder();
    relBuilder.push(oldAggRel.getInput());
    final List<RexNode> inputExprs = new ArrayList<>(relBuilder.fields());

    // create new aggregate function calls and rest of project list together
    for (AggregateCall oldCall : oldCalls) {
      projList.add(
          reduceAgg(oldAggRel, oldCall, newCalls, aggCallMapping, inputExprs));
    }

    final int extraArgCount = inputExprs.size() - relBuilder.peek().getRowType().getFieldCount();
    if (extraArgCount > 0) {
      relBuilder.project(inputExprs,
          CompositeList.of(
              relBuilder.peek().getRowType().getFieldNames(),
              Collections.nCopies(extraArgCount, null)));
    }
    newAggregateRel(relBuilder, oldAggRel, newCalls);
    newCalcRel(relBuilder, oldAggRel.getRowType(), projList);
    ruleCall.transformTo(relBuilder.build());
  }

  private RexNode reduceAgg(Aggregate oldAggRel, AggregateCall oldCall, List<AggregateCall> newCalls,
      Map<AggregateCall, RexNode> aggCallMapping, List<RexNode> inputExprs) {
    if (canReduce(oldCall)) {
      switch (oldCall.getAggregation().getName()) {
        case PinotSkewnessAggregateFunction.SKEWNESS:
          return reduceFourthMoment(oldAggRel, oldCall, newCalls, aggCallMapping, false);
        case PinotKurtosisAggregateFunction.KURTOSIS:
          return reduceFourthMoment(oldAggRel, oldCall, newCalls, aggCallMapping, true);
        default:
          throw new IllegalStateException("Unexpected aggregation name " + oldCall.getAggregation().getName());
      }
    } else {
      // anything else:  preserve original call
      RexBuilder rexBuilder = oldAggRel.getCluster().getRexBuilder();
      final int nGroups = oldAggRel.getGroupCount();
      return rexBuilder.addAggCall(oldCall,
          nGroups,
          newCalls,
          aggCallMapping,
          oldAggRel.getInput()::fieldIsNullable);
    }
  }

  private RexNode reduceFourthMoment(Aggregate oldAggRel, AggregateCall oldCall, List<AggregateCall> newCalls,
      Map<AggregateCall, RexNode> aggCallMapping, boolean isKurtosis) {
    final int nGroups = oldAggRel.getGroupCount();
    final RexBuilder rexBuilder = oldAggRel.getCluster().getRexBuilder();
    final AggregateCall fourthMomentCall =
        AggregateCall.create(PinotFourthMomentAggregateFunction.INSTANCE,
            oldCall.isDistinct(),
            oldCall.isApproximate(),
            oldCall.ignoreNulls(),
            oldCall.getArgList(),
            oldCall.filterArg,
            oldCall.distinctKeys,
            oldCall.collation,
            oldAggRel.getGroupCount(),
            oldAggRel.getInput(),
            null,
            null);

    RexNode fmRef = rexBuilder.addAggCall(fourthMomentCall, nGroups, newCalls,
        aggCallMapping, oldAggRel.getInput()::fieldIsNullable);

    final RexNode skewRef = rexBuilder.makeCall(
        isKurtosis ? PinotOperatorTable.KURTOSIS_REDUCE : PinotOperatorTable.SKEWNESS_REDUCE,
        fmRef);
    return rexBuilder.makeCast(oldCall.getType(), skewRef);
  }

  private boolean canReduce(AggregateCall call) {
    return FUNCTIONS.contains(call.getAggregation().getName());
  }

  protected void newAggregateRel(RelBuilder relBuilder,
      Aggregate oldAggregate,
      List<AggregateCall> newCalls) {
    relBuilder.aggregate(
        relBuilder.groupKey(oldAggregate.getGroupSet(), oldAggregate.getGroupSets()),
        newCalls);
  }

  protected void newCalcRel(RelBuilder relBuilder,
      RelDataType rowType,
      List<RexNode> exprs) {
    relBuilder.project(exprs, rowType.getFieldNames());
  }
}
