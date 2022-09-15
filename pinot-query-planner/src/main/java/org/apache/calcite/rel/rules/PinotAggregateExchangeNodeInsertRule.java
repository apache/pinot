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
import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalExchange;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlCountAggFunction;
import org.apache.calcite.sql.fun.SqlSumEmptyIsZeroAggFunction;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.pinot.query.planner.hints.PinotRelationalHints;


/**
 * Special rule for Pinot, this rule is fixed to generate a 2-stage aggregation split between the
 * (1) non-data-locale Pinot server agg stage, and (2) the data-locale Pinot intermediate agg stage.
 *
 * Pinot uses special intermediate data representation for partially aggregated results, thus we can't use
 * {@link org.apache.calcite.rel.rules.AggregateReduceFunctionsRule} to reduce complex aggregation.
 *
 * This rule is here to introduces Pinot-special aggregation splits. In-general, all aggregations are split into
 * intermediate-stage AGG; and server-stage AGG with the same naming. E.g.
 *
 * COUNT(*) transforms into: COUNT(*)_SERVER --> COUNT(*)_INTERMEDIATE, where
 *   COUNT(*)_SERVER produces TUPLE[ SUM(1), GROUP_BY_KEY ]
 *   COUNT(*)_INTERMEDIATE produces TUPLE[ SUM(COUNT(*)_SERVER), GROUP_BY_KEY ]
 *
 * However, the suffix _SERVER/_INTERMEDIATE is merely a SQL hint to the Aggregate operator and will be translated
 * into correct, actual operator chain during Physical plan.
 */
public class PinotAggregateExchangeNodeInsertRule extends RelOptRule {
  public static final PinotAggregateExchangeNodeInsertRule INSTANCE =
      new PinotAggregateExchangeNodeInsertRule(PinotRuleUtils.PINOT_REL_FACTORY);
  private static final Set<SqlKind> SUPPORTED_AGG_KIND = ImmutableSet.of(
      SqlKind.SUM, SqlKind.SUM0, SqlKind.MIN, SqlKind.MAX, SqlKind.COUNT);

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
      return !agg.getHints().contains(PinotRelationalHints.AGG_LEAF_STAGE)
          && !agg.getHints().contains(PinotRelationalHints.AGG_INTERMEDIATE_STAGE);
    }
    return false;
  }

  /**
   * Split the AGG into 2 stages, both with the same AGG type,
   * Pinot internal stage optimization can use the info of the input data type to infer whether it should generate
   * the "intermediate-stage AGG operator" or a "leaf-stage AGG operator"
   * @see org.apache.pinot.core.query.aggregation.function.AggregationFunction
   *
   * @param call the {@link RelOptRuleCall} on match.
   */
  @Override
  public void onMatch(RelOptRuleCall call) {
    Aggregate oldAggRel = call.rel(0);
    ImmutableList<RelHint> orgHints = oldAggRel.getHints();

    // 1. attach leaf agg RelHint to original agg.
    ImmutableList<RelHint> newLeafAggHints =
        new ImmutableList.Builder<RelHint>().addAll(orgHints).add(PinotRelationalHints.AGG_LEAF_STAGE).build();
    Aggregate newLeafAgg =
        new LogicalAggregate(oldAggRel.getCluster(), oldAggRel.getTraitSet(), newLeafAggHints, oldAggRel.getInput(),
            oldAggRel.getGroupSet(), oldAggRel.getGroupSets(), oldAggRel.getAggCallList());

    // 2. attach exchange.
    List<Integer> groupSetIndices = ImmutableIntList.range(0, oldAggRel.getGroupCount());
    LogicalExchange exchange = null;
    if (groupSetIndices.size() == 0) {
      exchange = LogicalExchange.create(newLeafAgg, RelDistributions.hash(Collections.emptyList()));
    } else {
      exchange = LogicalExchange.create(newLeafAgg, RelDistributions.hash(groupSetIndices));
    }

    // 3. attach intermediate agg stage.
    RelNode newAggNode = makeNewIntermediateAgg(call, oldAggRel, exchange);
    call.transformTo(newAggNode);
  }

  private RelNode makeNewIntermediateAgg(RelOptRuleCall ruleCall, Aggregate oldAggRel, LogicalExchange exchange) {

    // add the exchange as the input node to the relation builder.
    RelBuilder relBuilder = ruleCall.builder();
    relBuilder.push(exchange);
    List<RexNode> inputExprs = new ArrayList<>(relBuilder.fields());

    // make input ref to the exchange after the leaf aggregate.
    RexBuilder rexBuilder = exchange.getCluster().getRexBuilder();
    final int nGroups = oldAggRel.getGroupCount();
    for (int i = 0; i < nGroups; i++) {
      rexBuilder.makeInputRef(oldAggRel, i);
    }

    // create new aggregate function calls from exchange input.
    List<AggregateCall> oldCalls = oldAggRel.getAggCallList();
    List<AggregateCall> newCalls = new ArrayList<>();
    Map<AggregateCall, RexNode> aggCallMapping = new HashMap<>();

    for (int oldCallIndex = 0; oldCallIndex < oldCalls.size(); oldCallIndex++) {
      AggregateCall oldCall = oldCalls.get(oldCallIndex);
      convertAggCall(rexBuilder, oldAggRel, oldCallIndex, oldCall, newCalls, aggCallMapping, inputExprs);
    }

    // create new aggregate relation.
    ImmutableList<RelHint> orgHints = oldAggRel.getHints();
    ImmutableList<RelHint> newIntermediateAggHints =
        new ImmutableList.Builder<RelHint>().addAll(orgHints).add(PinotRelationalHints.AGG_INTERMEDIATE_STAGE).build();
    ImmutableBitSet groupSet = ImmutableBitSet.range(nGroups);
    relBuilder.aggregate(
        relBuilder.groupKey(groupSet, ImmutableList.of(groupSet)),
        newCalls);
    relBuilder.hints(newIntermediateAggHints);
    return relBuilder.build();
  }

  /**
   * convert aggregate call based on the intermediate stage input.
   *
   * <p>Note that the intermediate stage input only supports splittable aggregators such as SUM/MIN/MAX.
   * All non-splittable aggregator must be converted into splittable aggregator first.
   *
   * <p>For COUNT operations, the intermediate stage will be converted to SUM.
   */
  private static void convertAggCall(RexBuilder rexBuilder, Aggregate oldAggRel, int oldCallIndex,
      AggregateCall oldCall, List<AggregateCall> newCalls, Map<AggregateCall, RexNode> aggCallMapping,
      List<RexNode> inputExprs) {
    final int nGroups = oldAggRel.getGroupCount();
    final SqlAggFunction oldAggregation = oldCall.getAggregation();
    final SqlKind aggKind = oldAggregation.getKind();
    // Check only the supported AGG functions are provided.
    Preconditions.checkState(SUPPORTED_AGG_KIND.contains(aggKind), "Unsupported SQL aggregation "
        + "kind: {}. Only splittable aggregation functions are supported!", aggKind);

    // Special treatment on COUNT
    AggregateCall newCall;
    if (oldAggregation instanceof SqlCountAggFunction) {
      newCall = AggregateCall.create(new SqlSumEmptyIsZeroAggFunction(), oldCall.isDistinct(), oldCall.isApproximate(),
          oldCall.ignoreNulls(), convertArgList(nGroups + oldCallIndex, Collections.singletonList(oldCallIndex)),
          oldCall.filterArg, oldCall.distinctKeys, oldCall.collation, oldCall.type, oldCall.getName());
    } else {
      newCall = AggregateCall.create(
          oldCall.getAggregation(), oldCall.isDistinct(), oldCall.isApproximate(), oldCall.ignoreNulls(),
          convertArgList(nGroups + oldCallIndex, oldCall.getArgList()), oldCall.filterArg, oldCall.distinctKeys,
          oldCall.collation, oldCall.type, oldCall.getName());
    }
    rexBuilder.addAggCall(newCall,
        nGroups,
        newCalls,
        aggCallMapping,
        oldAggRel.getInput()::fieldIsNullable);
  }

  private static List<Integer> convertArgList(int oldCallIndexWithShift, List<Integer> argList) {
    Preconditions.checkArgument(argList.size() <= 1,
        "Unable to convert call as the argList contains more than 1 argument");
    return argList.size() == 1 ? Collections.singletonList(oldCallIndexWithShift) : Collections.emptyList();
  }
}
