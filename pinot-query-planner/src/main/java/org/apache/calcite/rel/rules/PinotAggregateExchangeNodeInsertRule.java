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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.hint.PinotHintOptions;
import org.apache.calcite.rel.hint.PinotHintStrategyTable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.PinotLogicalExchange;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.PinotSqlAggFunction;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.PinotOperatorTable;
import org.apache.calcite.sql.fun.SqlCountAggFunction;
import org.apache.calcite.sql.fun.SqlSumEmptyIsZeroAggFunction;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.pinot.query.planner.plannode.AggregateNode.AggType;
import org.apache.pinot.segment.spi.AggregationFunctionType;


/**
 * Special rule for Pinot, this rule is fixed to generate a 2-stage aggregation split between the
 * (1) non-data-locale Pinot server agg stage, and (2) the data-locale Pinot intermediate agg stage.
 *
 * Pinot uses special intermediate data representation for partially aggregated results, thus we can't use
 * {@link org.apache.calcite.rel.rules.AggregateReduceFunctionsRule} to reduce complex aggregation.
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

  @Override
  public boolean matches(RelOptRuleCall call) {
    if (call.rels.length < 1) {
      return false;
    }
    if (call.rel(0) instanceof Aggregate) {
      Aggregate agg = call.rel(0);
      ImmutableList<RelHint> hints = agg.getHints();
      return !PinotHintStrategyTable.containsHint(hints, PinotHintOptions.INTERNAL_AGG_OPTIONS);
    }
    return false;
  }

  /**
   * Split the AGG into 3 plan fragments, all with the same AGG type (in some cases the final agg name may be different)
   * Pinot internal plan fragment optimization can use the info of the input data type to infer whether it should
   * generate the "final-stage AGG operator" or "intermediate-stage AGG operator" or "leaf-stage AGG operator"
   * @see org.apache.pinot.core.query.aggregation.function.AggregationFunction
   *
   * @param call the {@link RelOptRuleCall} on match.
   */
  @Override
  public void onMatch(RelOptRuleCall call) {
    Aggregate oldAggRel = call.rel(0);
    ImmutableList<RelHint> oldHints = oldAggRel.getHints();

    Aggregate newAgg;
    if (!oldAggRel.getGroupSet().isEmpty() && PinotHintStrategyTable.containsHintOption(oldHints,
        PinotHintOptions.AGGREGATE_HINT_OPTIONS, PinotHintOptions.AggregateOptions.IS_PARTITIONED_BY_GROUP_BY_KEYS)) {
      // ------------------------------------------------------------------------
      // If the "is_partitioned_by_group_by_keys" aggregate hint option is set, just add additional hints indicating
      // this is a single stage aggregation.
      ImmutableList<RelHint> newLeafAggHints =
          new ImmutableList.Builder<RelHint>().addAll(oldHints).add(createAggHint(AggType.DIRECT)).build();
      newAgg =
          new LogicalAggregate(oldAggRel.getCluster(), oldAggRel.getTraitSet(), newLeafAggHints, oldAggRel.getInput(),
              oldAggRel.getGroupSet(), oldAggRel.getGroupSets(), oldAggRel.getAggCallList());
    } else if (!oldAggRel.getGroupSet().isEmpty() && PinotHintStrategyTable.containsHintOption(oldHints,
        PinotHintOptions.AGGREGATE_HINT_OPTIONS,
        PinotHintOptions.AggregateOptions.SKIP_LEAF_STAGE_GROUP_BY_AGGREGATION)) {
      // ------------------------------------------------------------------------
      // If "is_skip_leaf_stage_group_by" SQLHint option is passed, the leaf stage aggregation is skipped.
      newAgg = (Aggregate) createPlanWithExchangeDirectAggregation(call);
    } else {
      // ------------------------------------------------------------------------
      newAgg = (Aggregate) createPlanWithLeafExchangeFinalAggregate(call);
    }
    call.transformTo(newAgg);
  }

  /**
   * Aggregate node will be split into LEAF + exchange + FINAL.
   * optionally we can insert INTERMEDIATE to reduce hotspot in the future.
   */
  private RelNode createPlanWithLeafExchangeFinalAggregate(RelOptRuleCall call) {
    // TODO: add optional intermediate stage here when hinted.
    Aggregate oldAggRel = call.rel(0);
    // 1. attach leaf agg RelHint to original agg. Perform any aggregation call conversions necessary
    Aggregate leafAgg = convertAggForLeafInput(oldAggRel);
    // 2. attach exchange.
    List<Integer> groupSetIndices = ImmutableIntList.range(0, oldAggRel.getGroupCount());
    PinotLogicalExchange exchange;
    if (groupSetIndices.size() == 0) {
      exchange = PinotLogicalExchange.create(leafAgg, RelDistributions.hash(Collections.emptyList()));
    } else {
      exchange = PinotLogicalExchange.create(leafAgg, RelDistributions.hash(groupSetIndices));
    }
    // 3. attach final agg stage.
    return convertAggFromIntermediateInput(call, oldAggRel, exchange, AggType.FINAL);
  }

  /**
   * Use this group by optimization to skip leaf stage aggregation when aggregating at leaf level is not desired.
   * Many situation could be wasted effort to do group-by on leaf, eg: when cardinality of group by column is very high.
   */
  private RelNode createPlanWithExchangeDirectAggregation(RelOptRuleCall call) {
    Aggregate oldAggRel = call.rel(0);
    ImmutableList<RelHint> oldHints = oldAggRel.getHints();
    ImmutableList<RelHint> newHints =
        new ImmutableList.Builder<RelHint>().addAll(oldHints).add(createAggHint(AggType.DIRECT)).build();

    // create project when there's none below the aggregate to reduce exchange overhead
    RelNode childRel = ((HepRelVertex) oldAggRel.getInput()).getCurrentRel();
    if (!(childRel instanceof Project)) {
      return convertAggForExchangeDirectAggregate(call, newHints);
    } else {
      // create normal exchange
      List<Integer> groupSetIndices = new ArrayList<>();
      oldAggRel.getGroupSet().forEach(groupSetIndices::add);
      PinotLogicalExchange exchange = PinotLogicalExchange.create(childRel, RelDistributions.hash(groupSetIndices));
      return new LogicalAggregate(oldAggRel.getCluster(), oldAggRel.getTraitSet(), newHints, exchange,
          oldAggRel.getGroupSet(), oldAggRel.getGroupSets(), oldAggRel.getAggCallList());
    }
  }

  /**
   * The following is copied from {@link AggregateExtractProjectRule#onMatch(RelOptRuleCall)}
   * with modification to insert an exchange in between the Aggregate and Project
   */
  private RelNode convertAggForExchangeDirectAggregate(RelOptRuleCall call, ImmutableList<RelHint> newHints) {
    final Aggregate aggregate = call.rel(0);
    final RelNode input = aggregate.getInput();
    // Compute which input fields are used.
    // 1. group fields are always used
    final ImmutableBitSet.Builder inputFieldsUsed =
        aggregate.getGroupSet().rebuild();
    // 2. agg functions
    for (AggregateCall aggCall : aggregate.getAggCallList()) {
      for (int i : aggCall.getArgList()) {
        inputFieldsUsed.set(i);
      }
      if (aggCall.filterArg >= 0) {
        inputFieldsUsed.set(aggCall.filterArg);
      }
    }
    final RelBuilder relBuilder1 = call.builder().push(input);
    final List<RexNode> projects = new ArrayList<>();
    final Mapping mapping =
        Mappings.create(MappingType.INVERSE_SURJECTION,
            aggregate.getInput().getRowType().getFieldCount(),
            inputFieldsUsed.cardinality());
    int j = 0;
    for (int i : inputFieldsUsed.build()) {
      projects.add(relBuilder1.field(i));
      mapping.set(i, j++);
    }
    relBuilder1.project(projects);
    final ImmutableBitSet newGroupSet =
        Mappings.apply(mapping, aggregate.getGroupSet());
    Project project = (Project) relBuilder1.build();

    // ------------------------------------------------------------------------
    PinotLogicalExchange exchange = PinotLogicalExchange.create(project, RelDistributions.hash(newGroupSet.asList()));
    // ------------------------------------------------------------------------

    final RelBuilder relBuilder2 = call.builder().push(exchange);
    final List<ImmutableBitSet> newGroupSets =
        aggregate.getGroupSets().stream()
            .map(bitSet -> Mappings.apply(mapping, bitSet))
            .collect(Util.toImmutableList());
    final List<RelBuilder.AggCall> newAggCallList =
        aggregate.getAggCallList().stream()
            .map(aggCall -> relBuilder2.aggregateCall(aggCall, mapping))
            .collect(Util.toImmutableList());
    final RelBuilder.GroupKey groupKey =
        relBuilder2.groupKey(newGroupSet, newGroupSets);
    relBuilder2.aggregate(groupKey, newAggCallList).hints(newHints);
    return relBuilder2.build();
  }

  private Aggregate convertAggForLeafInput(Aggregate oldAggRel) {
    List<AggregateCall> oldCalls = oldAggRel.getAggCallList();
    List<AggregateCall> newCalls = new ArrayList<>();
    for (AggregateCall oldCall : oldCalls) {
      newCalls.add(buildAggregateCall(oldAggRel.getInput(), oldCall, oldCall.getArgList(), oldAggRel.getGroupCount(),
          AggType.LEAF));
    }
    ImmutableList<RelHint> oldHints = oldAggRel.getHints();
    ImmutableList<RelHint> newHints =
        new ImmutableList.Builder<RelHint>().addAll(oldHints).add(createAggHint(AggType.LEAF)).build();
    return new LogicalAggregate(oldAggRel.getCluster(), oldAggRel.getTraitSet(), newHints, oldAggRel.getInput(),
        oldAggRel.getGroupSet(), oldAggRel.getGroupSets(), newCalls);
  }

  private RelNode convertAggFromIntermediateInput(RelOptRuleCall ruleCall, Aggregate oldAggRel,
      PinotLogicalExchange exchange, AggType aggType) {
    // add the exchange as the input node to the relation builder.
    RelBuilder relBuilder = ruleCall.builder();
    relBuilder.push(exchange);

    // make input ref to the exchange after the leaf aggregate, all groups should be at the front
    RexBuilder rexBuilder = exchange.getCluster().getRexBuilder();
    final int nGroups = oldAggRel.getGroupCount();
    for (int i = 0; i < nGroups; i++) {
      rexBuilder.makeInputRef(oldAggRel, i);
    }

    List<AggregateCall> newCalls = new ArrayList<>();
    Map<AggregateCall, RexNode> aggCallMapping = new HashMap<>();

    // create new aggregate function calls from exchange input, all aggCalls are followed one by one from exchange
    // b/c the exchange produces intermediate results, thus the input to the newCall will be indexed at
    // [nGroup + oldCallIndex]
    List<AggregateCall> oldCalls = oldAggRel.getAggCallList();
    for (int oldCallIndex = 0; oldCallIndex < oldCalls.size(); oldCallIndex++) {
      AggregateCall oldCall = oldCalls.get(oldCallIndex);
      // intermediate stage input only supports single argument inputs.
      List<Integer> argList = Collections.singletonList(nGroups + oldCallIndex);
      AggregateCall newCall = buildAggregateCall(exchange, oldCall, argList, nGroups, aggType);
      rexBuilder.addAggCall(newCall, nGroups, newCalls, aggCallMapping, oldAggRel.getInput()::fieldIsNullable);
    }

    // create new aggregate relation.
    ImmutableList<RelHint> orgHints = oldAggRel.getHints();
    ImmutableList<RelHint> newAggHint =
        new ImmutableList.Builder<RelHint>().addAll(orgHints).add(createAggHint(aggType)).build();
    ImmutableBitSet groupSet = ImmutableBitSet.range(nGroups);
    relBuilder.aggregate(relBuilder.groupKey(groupSet, ImmutableList.of(groupSet)), newCalls);
    relBuilder.hints(newAggHint);
    return relBuilder.build();
  }

  private static AggregateCall buildAggregateCall(RelNode inputNode, AggregateCall orgAggCall, List<Integer> argList,
      int numberGroups, AggType aggType) {
    final SqlAggFunction oldAggFunction = orgAggCall.getAggregation();
    final SqlKind aggKind = oldAggFunction.getKind();
    String functionName = getFunctionNameFromAggregateCall(orgAggCall);
    AggregationFunctionType functionType = AggregationFunctionType.getAggregationFunctionType(functionName);
    // Check only the supported AGG functions are provided.
    validateAggregationFunctionIsSupported(functionType.getName(), aggKind);
    // create the aggFunction
    SqlAggFunction sqlAggFunction;
    if (functionType.getReturnTypeInference() != null) {
      switch (aggType) {
        case LEAF:
          sqlAggFunction = new PinotSqlAggFunction(functionName.toUpperCase(Locale.ROOT), null,
              functionType.getSqlKind(), functionType.getIntermediateReturnTypeInference(), null,
              functionType.getOperandTypeChecker(), functionType.getSqlFunctionCategory());
          break;
        case INTERMEDIATE:
          sqlAggFunction = new PinotSqlAggFunction(functionName.toUpperCase(Locale.ROOT), null,
              functionType.getSqlKind(), functionType.getIntermediateReturnTypeInference(), null,
              OperandTypes.ANY, functionType.getSqlFunctionCategory());
          break;
        case FINAL:
          sqlAggFunction = new PinotSqlAggFunction(functionName.toUpperCase(Locale.ROOT), null,
              functionType.getSqlKind(), functionType.getReturnTypeInference(), null,
              OperandTypes.ANY, functionType.getSqlFunctionCategory());
          break;
        default:
          throw new UnsupportedOperationException("Unsuppoted aggType: " + aggType + " for " + functionName);
      }
    } else if (oldAggFunction instanceof SqlCountAggFunction && aggType.isInputIntermediateFormat()) {
      sqlAggFunction = new SqlSumEmptyIsZeroAggFunction();
    } else {
      sqlAggFunction = oldAggFunction;
    }

    return AggregateCall.create(sqlAggFunction,
        functionName.equals("distinctCount") || orgAggCall.isDistinct(),
        orgAggCall.isApproximate(),
        orgAggCall.ignoreNulls(),
        argList,
        orgAggCall.filterArg,
        orgAggCall.distinctKeys,
        orgAggCall.collation,
        numberGroups,
        inputNode,
        null,
        null);
  }

  private static String getFunctionNameFromAggregateCall(AggregateCall aggregateCall) {
    return aggregateCall.getAggregation().getName().equalsIgnoreCase("COUNT") && aggregateCall.isDistinct()
        ? "distinctCount" : aggregateCall.getAggregation().getName();
  }

  private static void validateAggregationFunctionIsSupported(String functionName, SqlKind aggKind) {
    Preconditions.checkState(PinotOperatorTable.isAggregationFunctionRegisteredWithOperatorTable(functionName),
        String.format("Failed to create aggregation. Unsupported SQL aggregation kind: %s or function name: %s. "
                + "Only splittable aggregation functions are supported!", aggKind, functionName));
  }

  private static RelHint createAggHint(AggType aggType) {
    return RelHint.builder(PinotHintOptions.INTERNAL_AGG_OPTIONS)
        .hintOption(PinotHintOptions.InternalAggregateOptions.AGG_TYPE, aggType.name())
        .build();
  }
}
