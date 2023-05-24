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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.hint.PinotHintStrategyTable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.PinotSqlAggFunction;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.PinotOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.CompositeList;
import org.apache.pinot.segment.spi.AggregationFunctionType;


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

  public static final RelHint REDUCER_ADDED_HINT = RelHint.builder(
      PinotHintStrategyTable.INTERNAL_AGG_REDUCE_PRESENT).build();

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
        if (shouldReduce(aggCall) && !PinotHintStrategyTable.containsHint(
            agg.getHints(), PinotHintStrategyTable.INTERNAL_AGG_REDUCE_PRESENT)) {
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
    if (shouldReduce(oldCall)) {
      return reduceAggregation(oldAggRel, oldCall, newCalls, aggCallMapping);
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

  private RexNode reduceAggregation(Aggregate oldAggRel, AggregateCall oldCall, List<AggregateCall> newCalls,
      Map<AggregateCall, RexNode> aggCallMapping) {
    final int nGroups = oldAggRel.getGroupCount();
    final RexBuilder rexBuilder = oldAggRel.getCluster().getRexBuilder();
    String functionName = oldCall.getAggregation().getName();
    if (oldCall.getAggregation().getName().equalsIgnoreCase("COUNT") && oldCall.isDistinct()) {
      functionName = "distinctCount";
    }
    AggregationFunctionType type = AggregationFunctionType.getAggregationFunctionType(functionName);
    SqlAggFunction sqlAggFunction =
        new PinotSqlAggFunction(type.getIntermediateFunctionName().toUpperCase(Locale.ROOT),
            type.getSqlIdentifier(), type.getSqlKind(), type.getSqlIntermediateReturnTypeInference(),
            type.getSqlOperandTypeInference(), type.getSqlOperandTypeChecker(), type.getSqlFunctionCategory());
    final AggregateCall aggregateCall =
        AggregateCall.create(sqlAggFunction,
            functionName.equals("distinctCount") || oldCall.isDistinct(),
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

    // TODO: This only works for a single argument (i.e. for general expressions, literals args are to be added later)
    RexNode ref = rexBuilder.addAggCall(aggregateCall, nGroups, newCalls, aggCallMapping,
        oldAggRel.getInput()::fieldIsNullable);
    List<RexNode> functionArgs = new ArrayList<>();
    functionArgs.add(ref);

    SqlFunction function = new SqlFunction(type.getReduceFunctionName(), SqlKind.OTHER_FUNCTION,
        type.getSqlReduceReturnTypeInference(), null, type.getSqlReduceOperandTypeChecker(),
        SqlFunctionCategory.USER_DEFINED_FUNCTION);

    final RexNode reduceRef = rexBuilder.makeCall(
        function,
        functionArgs);
    return rexBuilder.makeCast(oldCall.getType(), reduceRef);
  }

  private boolean shouldReduce(AggregateCall call) {
    String name = call.getAggregation().getName();
    SqlKind sqlKind = call.getAggregation().getKind();
    // Check kind instead of function name if the name contains '$'. Enums cannot start with '$' so this catches
    // aggregation functions such as $SUM0
    return PinotOperatorTable.isAggregationReduceSupported(name)
        || (name.contains("$") && PinotOperatorTable.isAggregationKindSupported(sqlKind));
  }

  protected void newAggregateRel(RelBuilder relBuilder,
      Aggregate oldAggregate,
      List<AggregateCall> newCalls) {
    // Add a hint to ensure that once the reduce is done this rule won't be called again
    ImmutableList<RelHint> orgHints = oldAggregate.getHints();
    ImmutableList<RelHint> newAggHints =
        new ImmutableList.Builder<RelHint>().addAll(orgHints).add(REDUCER_ADDED_HINT).build();
    relBuilder.aggregate(
        relBuilder.groupKey(oldAggregate.getGroupSet(), oldAggregate.getGroupSets()),
        newCalls);
    relBuilder.hints(newAggHints);
  }

  protected void newCalcRel(RelBuilder relBuilder,
      RelDataType rowType,
      List<RexNode> exprs) {
    relBuilder.project(exprs, rowType.getFieldNames());
  }

  protected void newCalcRel(RelBuilder relBuilder,
      List<String> rowTypeNames,
      List<RexNode> exprs) {
    relBuilder.project(exprs, rowTypeNames);
  }
}
