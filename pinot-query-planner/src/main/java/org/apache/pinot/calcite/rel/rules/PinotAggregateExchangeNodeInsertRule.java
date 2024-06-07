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

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.rules.AggregateExtractProjectRule;
import org.apache.calcite.rel.rules.AggregateReduceFunctionsRule;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.pinot.calcite.rel.hint.PinotHintOptions;
import org.apache.pinot.calcite.rel.hint.PinotHintStrategyTable;
import org.apache.pinot.calcite.rel.logical.PinotLogicalAggregate;
import org.apache.pinot.calcite.rel.logical.PinotLogicalExchange;
import org.apache.pinot.calcite.rel.logical.PinotLogicalSortExchange;
import org.apache.pinot.calcite.sql.PinotSqlAggFunction;
import org.apache.pinot.query.planner.plannode.AggregateNode.AggType;
import org.apache.pinot.segment.spi.AggregationFunctionType;


/**
 * Special rule for Pinot, this rule is fixed to generate a 2-stage aggregation split between the
 * (1) non-data-locale Pinot server agg stage, and (2) the data-locale Pinot intermediate agg stage.
 *
 * Pinot uses special intermediate data representation for partially aggregated results, thus we can't use
 * {@link AggregateReduceFunctionsRule} to reduce complex aggregation.
 *
 * This rule is here to introduces Pinot-special aggregation splits. In-general there are several options:
 * <ul>
 *   <li>`aggName`__DIRECT</li>
 *   <li>`aggName`__LEAF + `aggName`__FINAL</li>
 *   <li>`aggName`__LEAF [+ `aggName`__INTERMEDIATE] + `aggName`__FINAL</li>
 * </ul>
 *
 * for example:
 * - COUNT(*) with a GROUP_BY_KEY transforms into: COUNT(*)__LEAF --> COUNT(*)__FINAL, where
 *   - COUNT(*)__LEAF produces TUPLE[ SUM(1), GROUP_BY_KEY ]
 *   - COUNT(*)__FINAL produces TUPLE[ SUM(COUNT(*)__LEAF), GROUP_BY_KEY ]
 */
public class PinotAggregateExchangeNodeInsertRule extends RelOptRule {
  public static final PinotAggregateExchangeNodeInsertRule INSTANCE =
      new PinotAggregateExchangeNodeInsertRule(PinotRuleUtils.PINOT_REL_FACTORY);

  public PinotAggregateExchangeNodeInsertRule(RelBuilderFactory factory) {
    super(operand(LogicalAggregate.class, any()), factory, null);
  }

  /**
   * Split the AGG into 3 plan fragments, all with the same AGG type (in some cases the final agg name may be different)
   * Pinot internal plan fragment optimization can use the info of the input data type to infer whether it should
   * generate the "final-stage AGG operator" or "intermediate-stage AGG operator" or "leaf-stage AGG operator"
   *
   * @param call the {@link RelOptRuleCall} on match.
   * @see org.apache.pinot.core.query.aggregation.function.AggregationFunction
   */
  @Override
  public void onMatch(RelOptRuleCall call) {
    Aggregate aggRel = call.rel(0);
    ImmutableList<RelHint> hints = aggRel.getHints();
    // Collation is not supported in leaf stage aggregation.
    RelCollation collation = extractWithInGroupCollation(aggRel);
    boolean hasGroupBy = !aggRel.getGroupSet().isEmpty();
    if (collation != null || (hasGroupBy && PinotHintStrategyTable.isHintOptionTrue(hints,
        PinotHintOptions.AGGREGATE_HINT_OPTIONS,
        PinotHintOptions.AggregateOptions.SKIP_LEAF_STAGE_GROUP_BY_AGGREGATION))) {
      call.transformTo(createPlanWithExchangeDirectAggregation(call, collation));
    } else if (hasGroupBy && PinotHintStrategyTable.isHintOptionTrue(hints, PinotHintOptions.AGGREGATE_HINT_OPTIONS,
        PinotHintOptions.AggregateOptions.IS_PARTITIONED_BY_GROUP_BY_KEYS)) {
      call.transformTo(new PinotLogicalAggregate(aggRel, buildAggCalls(aggRel, AggType.DIRECT), AggType.DIRECT));
    } else {
      call.transformTo(createPlanWithLeafExchangeFinalAggregate(call));
    }
  }

  // TODO: Currently it only handles one WITHIN GROUP collation across all AggregateCalls.
  @Nullable
  private static RelCollation extractWithInGroupCollation(Aggregate aggRel) {
    for (AggregateCall aggCall : aggRel.getAggCallList()) {
      List<RelFieldCollation> fieldCollations = aggCall.getCollation().getFieldCollations();
      if (!fieldCollations.isEmpty()) {
        return RelCollations.of(fieldCollations);
      }
    }
    return null;
  }

  /**
   * Use this group by optimization to skip leaf stage aggregation when aggregating at leaf level is not desired. Many
   * situation could be wasted effort to do group-by on leaf, eg: when cardinality of group by column is very high.
   */
  private static PinotLogicalAggregate createPlanWithExchangeDirectAggregation(RelOptRuleCall call,
      @Nullable RelCollation collation) {
    Aggregate aggRel = call.rel(0);
    RelNode input = aggRel.getInput();
    // Create Project when there's none below the aggregate.
    if (!(PinotRuleUtils.unboxRel(input) instanceof Project)) {
      aggRel = (Aggregate) generateProjectUnderAggregate(call);
      input = aggRel.getInput();
    }

    ImmutableBitSet groupSet = aggRel.getGroupSet();
    RelDistribution distribution = RelDistributions.hash(groupSet.asList());
    RelNode exchange;
    if (collation != null) {
      // Insert a LogicalSort node between exchange and aggregate whe collation exists.
      exchange = PinotLogicalSortExchange.create(input, distribution, collation, false, true);
    } else {
      exchange = PinotLogicalExchange.create(input, distribution);
    }

    return new PinotLogicalAggregate(aggRel, exchange, buildAggCalls(aggRel, AggType.DIRECT), AggType.DIRECT);
  }

  /**
   * Aggregate node will be split into LEAF + EXCHANGE + FINAL.
   * TODO: Add optional INTERMEDIATE stage to reduce hotspot.
   */
  private static PinotLogicalAggregate createPlanWithLeafExchangeFinalAggregate(RelOptRuleCall call) {
    Aggregate aggRel = call.rel(0);
    // Create a LEAF aggregate.
    PinotLogicalAggregate leafAggRel =
        new PinotLogicalAggregate(aggRel, buildAggCalls(aggRel, AggType.LEAF), AggType.LEAF);
    // Create an EXCHANGE node over the LEAF aggregate.
    PinotLogicalExchange exchange = PinotLogicalExchange.create(leafAggRel,
        RelDistributions.hash(ImmutableIntList.range(0, aggRel.getGroupCount())));
    // Create a FINAL aggregate over the EXCHANGE.
    return convertAggFromIntermediateInput(call, exchange, AggType.FINAL);
  }

  /**
   * The following is copied from {@link AggregateExtractProjectRule#onMatch(RelOptRuleCall)} with modification to take
   * aggregate input as input.
   */
  private static RelNode generateProjectUnderAggregate(RelOptRuleCall call) {
    final Aggregate aggregate = call.rel(0);
    // --------------- MODIFIED ---------------
    final RelNode input = aggregate.getInput();
    // final RelNode input = call.rel(1);
    // ------------- END MODIFIED -------------

    // Compute which input fields are used.
    // 1. group fields are always used
    final ImmutableBitSet.Builder inputFieldsUsed = aggregate.getGroupSet().rebuild();
    // 2. agg functions
    for (AggregateCall aggCall : aggregate.getAggCallList()) {
      for (int i : aggCall.getArgList()) {
        inputFieldsUsed.set(i);
      }
      if (aggCall.filterArg >= 0) {
        inputFieldsUsed.set(aggCall.filterArg);
      }
    }
    final RelBuilder relBuilder = call.builder().push(input);
    final List<RexNode> projects = new ArrayList<>();
    final Mapping mapping =
        Mappings.create(MappingType.INVERSE_SURJECTION, aggregate.getInput().getRowType().getFieldCount(),
            inputFieldsUsed.cardinality());
    int j = 0;
    for (int i : inputFieldsUsed.build()) {
      projects.add(relBuilder.field(i));
      mapping.set(i, j++);
    }

    relBuilder.project(projects);

    final ImmutableBitSet newGroupSet = Mappings.apply(mapping, aggregate.getGroupSet());
    final List<ImmutableBitSet> newGroupSets =
        aggregate.getGroupSets().stream().map(bitSet -> Mappings.apply(mapping, bitSet))
            .collect(ImmutableList.toImmutableList());
    final List<RelBuilder.AggCall> newAggCallList =
        aggregate.getAggCallList().stream().map(aggCall -> relBuilder.aggregateCall(aggCall, mapping))
            .collect(ImmutableList.toImmutableList());

    final RelBuilder.GroupKey groupKey = relBuilder.groupKey(newGroupSet, newGroupSets);
    relBuilder.aggregate(groupKey, newAggCallList);
    return relBuilder.build();
  }

  private static PinotLogicalAggregate convertAggFromIntermediateInput(RelOptRuleCall call,
      PinotLogicalExchange exchange, AggType aggType) {
    Aggregate aggRel = call.rel(0);
    RelNode input = PinotRuleUtils.unboxRel(aggRel.getInput());
    List<RexNode> projects = (input instanceof Project) ? ((Project) input).getProjects() : null;

    // Create new AggregateCalls from exchange input. Exchange produces results with group keys followed by intermediate
    // aggregate results.
    int groupCount = aggRel.getGroupCount();
    List<AggregateCall> orgAggCalls = aggRel.getAggCallList();
    int numAggCalls = orgAggCalls.size();
    List<AggregateCall> aggCalls = new ArrayList<>(numAggCalls);
    for (int i = 0; i < numAggCalls; i++) {
      AggregateCall orgAggCall = orgAggCalls.get(i);
      List<Integer> argList = orgAggCall.getArgList();
      int index = groupCount + i;
      RexInputRef inputRef = RexInputRef.of(index, aggRel.getRowType());
      // Generate rexList from argList and replace literal reference with literal. Keep the first argument as is.
      int numArguments = argList.size();
      List<RexNode> rexList;
      if (numArguments <= 1) {
        rexList = ImmutableList.of(inputRef);
      } else {
        rexList = new ArrayList<>(numArguments);
        rexList.add(inputRef);
        for (int j = 1; j < numArguments; j++) {
          int argument = argList.get(j);
          if (projects != null && projects.get(argument) instanceof RexLiteral) {
            rexList.add(projects.get(argument));
          } else {
            // Replace all the input reference in the rexList to the new input reference.
            rexList.add(inputRef);
          }
        }
      }
      aggCalls.add(buildAggCall(exchange, orgAggCall, rexList, groupCount, aggType));
    }

    return new PinotLogicalAggregate(aggRel, exchange, ImmutableBitSet.range(groupCount), aggCalls, aggType);
  }

  private static List<AggregateCall> buildAggCalls(Aggregate aggRel, AggType aggType) {
    RelNode input = PinotRuleUtils.unboxRel(aggRel.getInput());
    List<RexNode> projects = (input instanceof Project) ? ((Project) input).getProjects() : null;
    List<AggregateCall> orgAggCalls = aggRel.getAggCallList();
    List<AggregateCall> aggCalls = new ArrayList<>(orgAggCalls.size());
    for (AggregateCall orgAggCall : orgAggCalls) {
      // Generate rexList from argList and replace literal reference with literal. Keep the first argument as is.
      List<Integer> argList = orgAggCall.getArgList();
      int numArguments = argList.size();
      List<RexNode> rexList;
      if (numArguments == 0) {
        rexList = ImmutableList.of();
      } else if (numArguments == 1) {
        rexList = ImmutableList.of(RexInputRef.of(argList.get(0), input.getRowType()));
      } else {
        rexList = new ArrayList<>(numArguments);
        rexList.add(RexInputRef.of(argList.get(0), input.getRowType()));
        for (int i = 1; i < numArguments; i++) {
          int argument = argList.get(i);
          if (projects != null && projects.get(argument) instanceof RexLiteral) {
            rexList.add(projects.get(argument));
          } else {
            rexList.add(RexInputRef.of(argument, input.getRowType()));
          }
        }
      }
      aggCalls.add(buildAggCall(input, orgAggCall, rexList, aggRel.getGroupCount(), aggType));
    }
    return aggCalls;
  }

  // TODO: Revisit the following logic:
  //   - DISTINCT is resolved here
  //   - argList is replaced with rexList
  private static AggregateCall buildAggCall(RelNode input, AggregateCall orgAggCall, List<RexNode> rexList,
      int numGroups, AggType aggType) {
    String functionName = orgAggCall.getAggregation().getName();
    if (orgAggCall.isDistinct()) {
      if (functionName.equals("COUNT")) {
        functionName = "DISTINCTCOUNT";
      } else if (functionName.equals("LISTAGG")) {
        rexList.add(input.getCluster().getRexBuilder().makeLiteral(true));
      }
    }
    AggregationFunctionType functionType = AggregationFunctionType.getAggregationFunctionType(functionName);
    SqlAggFunction sqlAggFunction;
    switch (aggType) {
      case DIRECT:
        sqlAggFunction = new PinotSqlAggFunction(functionName, null, functionType.getSqlKind(),
            ReturnTypes.explicit(orgAggCall.getType()), null, functionType.getOperandTypeChecker(),
            functionType.getSqlFunctionCategory());
        break;
      case LEAF:
        sqlAggFunction = new PinotSqlAggFunction(functionName, null, functionType.getSqlKind(),
            functionType.getIntermediateReturnTypeInference(), null, functionType.getOperandTypeChecker(),
            functionType.getSqlFunctionCategory());
        break;
      case INTERMEDIATE:
        sqlAggFunction = new PinotSqlAggFunction(functionName, null, functionType.getSqlKind(),
            functionType.getIntermediateReturnTypeInference(), null, OperandTypes.ANY,
            functionType.getSqlFunctionCategory());
        break;
      case FINAL:
        sqlAggFunction = new PinotSqlAggFunction(functionName, null, functionType.getSqlKind(),
            ReturnTypes.explicit(orgAggCall.getType()), null, OperandTypes.ANY, functionType.getSqlFunctionCategory());
        break;
      default:
        throw new IllegalStateException("Unsupported AggType: " + aggType);
    }
    return AggregateCall.create(sqlAggFunction, false, orgAggCall.isApproximate(), orgAggCall.ignoreNulls(), rexList,
        ImmutableList.of(), aggType.isInputIntermediateFormat() ? -1 : orgAggCall.filterArg, orgAggCall.distinctKeys,
        orgAggCall.collation, numGroups, input, null, null);
  }
}
