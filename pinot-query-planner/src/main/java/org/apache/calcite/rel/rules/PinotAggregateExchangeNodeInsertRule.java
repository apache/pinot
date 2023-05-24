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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
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
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.PinotLogicalExchange;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.PinotOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;


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

  private static final RelHint FINAL_STAGE_HINT = RelHint.builder(
      PinotHintStrategyTable.INTERNAL_AGG_FINAL_STAGE).build();
  private static final RelHint INTERMEDIATE_STAGE_HINT = RelHint.builder(
      PinotHintStrategyTable.INTERNAL_AGG_INTERMEDIATE_STAGE).build();
  private static final RelHint SINGLE_STAGE_AGG_HINT = RelHint.builder(
      PinotHintStrategyTable.INTERNAL_IS_SINGLE_STAGE_AGG).build();

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
      return !PinotHintStrategyTable.containsHint(hints, PinotHintStrategyTable.INTERNAL_AGG_INTERMEDIATE_STAGE)
          && !PinotHintStrategyTable.containsHint(hints, PinotHintStrategyTable.INTERNAL_AGG_FINAL_STAGE)
          && !PinotHintStrategyTable.containsHint(hints, PinotHintStrategyTable.INTERNAL_IS_SINGLE_STAGE_AGG);
    }
    return false;
  }

  /**
   * Split the AGG into 2 plan fragments, both with the same AGG type,
   * Pinot internal plan fragment optimization can use the info of the input data type to infer whether it should
   * generate
   * the "intermediate-stage AGG operator" or a "leaf-stage AGG operator"
   * @see org.apache.pinot.core.query.aggregation.function.AggregationFunction
   *
   * @param call the {@link RelOptRuleCall} on match.
   */
  @Override
  public void onMatch(RelOptRuleCall call) {
    Aggregate oldAggRel = call.rel(0);
    ImmutableList<RelHint> oldHints = oldAggRel.getHints();

    // If the "is_partitioned_by_group_by_keys" aggregate hint option is set, just add an additional hint indicating
    // this is a single stage aggregation. This only applies to GROUP BY aggregations.
    if (!oldAggRel.getGroupSet().isEmpty() && PinotHintStrategyTable.containsHintOption(oldHints,
        PinotHintOptions.AGGREGATE_HINT_OPTIONS, PinotHintOptions.AggregateOptions.IS_PARTITIONED_BY_GROUP_BY_KEYS)) {
      // 1. attach leaf agg RelHint to original agg.
      ImmutableList<RelHint> newLeafAggHints =
          new ImmutableList.Builder<RelHint>().addAll(oldHints).add(SINGLE_STAGE_AGG_HINT).build();
      Aggregate newLeafAgg =
          new LogicalAggregate(oldAggRel.getCluster(), oldAggRel.getTraitSet(), newLeafAggHints, oldAggRel.getInput(),
              oldAggRel.getGroupSet(), oldAggRel.getGroupSets(), oldAggRel.getAggCallList());
      call.transformTo(newLeafAgg);
      return;
    }

    // If "skipLeafStageGroupByAggregation" SQLHint is passed, the leaf stage aggregation is skipped. This only
    // applies for Group By Aggregations.
    if (!oldAggRel.getGroupSet().isEmpty() && PinotHintStrategyTable.containsHint(oldHints,
        PinotHintStrategyTable.SKIP_LEAF_STAGE_GROUP_BY_AGGREGATION)) {
      // This is not the default path. Use this group by optimization to skip leaf stage aggregation when aggregating
      // at leaf level could be wasted effort. eg: when cardinality of group by column is very high.
      createPlanWithoutLeafAggregation(call);
      return;
    }

    // 1. attach leaf agg RelHint to original agg.
    ImmutableList<RelHint> newLeafAggHints =
        new ImmutableList.Builder<RelHint>().addAll(oldHints).add(INTERMEDIATE_STAGE_HINT).build();
    Aggregate newLeafAgg =
        new LogicalAggregate(oldAggRel.getCluster(), oldAggRel.getTraitSet(), newLeafAggHints, oldAggRel.getInput(),
            oldAggRel.getGroupSet(), oldAggRel.getGroupSets(), oldAggRel.getAggCallList());

    // 2. attach exchange.
    List<Integer> groupSetIndices = ImmutableIntList.range(0, oldAggRel.getGroupCount());
    PinotLogicalExchange exchange = null;
    if (groupSetIndices.size() == 0) {
      exchange = PinotLogicalExchange.create(newLeafAgg, RelDistributions.hash(Collections.emptyList()));
    } else {
      exchange = PinotLogicalExchange.create(newLeafAgg, RelDistributions.hash(groupSetIndices));
    }

    // 3. attach intermediate agg stage.
    RelNode newAggNode = makeNewIntermediateAgg(call, oldAggRel, exchange, true, null, null);
    call.transformTo(newAggNode);
  }

  private RelNode makeNewIntermediateAgg(RelOptRuleCall ruleCall, Aggregate oldAggRel, PinotLogicalExchange exchange,
      boolean isLeafStageAggregationPresent, List<Integer> argList, List<Integer> groupByList) {

    // add the exchange as the input node to the relation builder.
    RelBuilder relBuilder = ruleCall.builder();
    relBuilder.push(exchange);

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
      convertAggCall(rexBuilder, oldAggRel, oldCallIndex, oldCall, newCalls, aggCallMapping,
          isLeafStageAggregationPresent, argList);
    }

    // create new aggregate relation.
    ImmutableList<RelHint> orgHints = oldAggRel.getHints();
    // if the aggregation isn't split between intermediate and final stages, indicate that this is a single stage
    // aggregation so that the execution engine knows whether to aggregate or merge
    List<RelHint> additionalHints = isLeafStageAggregationPresent ? Collections.singletonList(FINAL_STAGE_HINT)
        : Arrays.asList(FINAL_STAGE_HINT, SINGLE_STAGE_AGG_HINT);
    ImmutableList<RelHint> newIntermediateAggHints =
        new ImmutableList.Builder<RelHint>().addAll(orgHints).addAll(additionalHints).build();
    ImmutableBitSet groupSet = groupByList == null ? ImmutableBitSet.range(nGroups) : ImmutableBitSet.of(groupByList);
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
      boolean isLeafStageAggregationPresent, List<Integer> argList) {
    final int nGroups = oldAggRel.getGroupCount();
    final SqlAggFunction oldAggregation = oldCall.getAggregation();
    final SqlKind aggKind = oldAggregation.getKind();
    final String aggName = oldAggregation.getName();
    // Check only the supported AGG functions are provided.
    Preconditions.checkState(PinotOperatorTable.isAggregationFunctionRegisteredWithOperatorTable(aggName)
        || PinotOperatorTable.isAggregationKindSupported(aggKind),
        String.format("Unsupported SQL aggregation kind: %s or function name: %s. Only splittable aggregation "
            + "functions are supported!", aggKind, aggName));

    AggregateCall newCall;
    if (isLeafStageAggregationPresent) {
      // Make sure COUNT in the final stage takes an argument
      List<Integer> oldArgList = (oldAggregation.getKind() == SqlKind.COUNT && !oldCall.isDistinct())
          ? Collections.singletonList(oldCallIndex)
          : oldCall.getArgList();
      newCall = AggregateCall.create(oldCall.getAggregation(), oldCall.isDistinct(), oldCall.isApproximate(),
          oldCall.ignoreNulls(), convertArgList(nGroups + oldCallIndex, oldArgList), oldCall.filterArg,
          oldCall.distinctKeys, oldCall.collation, oldCall.type, oldCall.getName());
    } else {
      List<Integer> newArgList = oldCall.getArgList().size() == 0 ? Collections.emptyList()
          : Collections.singletonList(argList.get(oldCallIndex));

      newCall = AggregateCall.create(oldCall.getAggregation(), oldCall.isDistinct(), oldCall.isApproximate(),
          oldCall.ignoreNulls(), newArgList, oldCall.filterArg, oldCall.distinctKeys, oldCall.collation, oldCall.type,
          oldCall.getName());
    }

    rexBuilder.addAggCall(newCall, nGroups, newCalls, aggCallMapping, oldAggRel.getInput()::fieldIsNullable);
  }

  private static List<Integer> convertArgList(int oldCallIndexWithShift, List<Integer> argList) {
    Preconditions.checkArgument(argList.size() <= 1,
        "Unable to convert call as the argList contains more than 1 argument");
    return argList.size() == 1 ? Collections.singletonList(oldCallIndexWithShift) : Collections.emptyList();
  }

  private void createPlanWithoutLeafAggregation(RelOptRuleCall call) {
    Aggregate oldAggRel = call.rel(0);
    RelNode childRel = ((HepRelVertex) oldAggRel.getInput()).getCurrentRel();
    LogicalProject project;

    List<Integer> newAggArgColumns = new ArrayList<>();
    List<Integer> newAggGroupByColumns = new ArrayList<>();

    // 1. Create the LogicalProject node if it does not exist. This is to send only the relevant columns over
    //    the wire for intermediate aggregation.
    if (childRel instanceof Project) {
      // Avoid creating a new LogicalProject if the child node of aggregation is already a project node.
      project = (LogicalProject) childRel;
      newAggArgColumns = fetchNewAggArgCols(oldAggRel.getAggCallList());
      newAggGroupByColumns = oldAggRel.getGroupSet().asList();
    } else {
      // Create a leaf stage project. This is done so that only the required columns are sent over the wire for
      // intermediate aggregation. If there are multiple aggregations on the same column, the column is projected
      // only once.
      project = createLogicalProjectForAggregate(oldAggRel, newAggArgColumns, newAggGroupByColumns);
    }

    // 2. Create an exchange on top of the LogicalProject.
    PinotLogicalExchange exchange = PinotLogicalExchange.create(project, RelDistributions.hash(newAggGroupByColumns));

    // 3. Create an intermediate stage aggregation.
    RelNode newAggNode =
        makeNewIntermediateAgg(call, oldAggRel, exchange, false, newAggArgColumns, newAggGroupByColumns);

    call.transformTo(newAggNode);
  }

  private LogicalProject createLogicalProjectForAggregate(Aggregate oldAggRel, List<Integer> newAggArgColumns,
      List<Integer> newAggGroupByCols) {
    RelNode childRel = ((HepRelVertex) oldAggRel.getInput()).getCurrentRel();
    RexBuilder childRexBuilder = childRel.getCluster().getRexBuilder();
    List<RelDataTypeField> fieldList = childRel.getRowType().getFieldList();

    List<RexNode> projectColRexNodes = new ArrayList<>();
    List<String> projectColNames = new ArrayList<>();
    // Maintains a mapping from the column to the corresponding index in projectColRexNodes.
    Map<Integer, Integer> projectSet = new HashMap<>();

    int projectIndex = 0;
    for (int group : oldAggRel.getGroupSet().asSet()) {
      projectColNames.add(fieldList.get(group).getName());
      projectColRexNodes.add(childRexBuilder.makeInputRef(childRel, group));
      projectSet.put(group, projectColRexNodes.size() - 1);
      newAggGroupByCols.add(projectIndex++);
    }

    List<AggregateCall> oldAggCallList = oldAggRel.getAggCallList();
    for (int i = 0; i < oldAggCallList.size(); i++) {
      List<Integer> argList = oldAggCallList.get(i).getArgList();
      if (argList.size() == 0) {
        newAggArgColumns.add(-1);
        continue;
      }
      for (int j = 0; j < argList.size(); j++) {
        Integer col = argList.get(j);
        if (!projectSet.containsKey(col)) {
          projectColRexNodes.add(childRexBuilder.makeInputRef(childRel, col));
          projectColNames.add(fieldList.get(col).getName());
          projectSet.put(col, projectColRexNodes.size() - 1);
          newAggArgColumns.add(projectColRexNodes.size() - 1);
        } else {
          newAggArgColumns.add(projectSet.get(col));
        }
      }
    }

    return LogicalProject.create(childRel, Collections.emptyList(), projectColRexNodes, projectColNames);
  }

  private List<Integer> fetchNewAggArgCols(List<AggregateCall> oldAggCallList) {
    List<Integer> newAggArgColumns = new ArrayList<>();

    for (int i = 0; i < oldAggCallList.size(); i++) {
      if (oldAggCallList.get(i).getArgList().size() == 0) {
        // This can be true for COUNT. Add a placeholder value which will be ignored.
        newAggArgColumns.add(-1);
        continue;
      }
      for (int j = 0; j < oldAggCallList.get(i).getArgList().size(); j++) {
        Integer col = oldAggCallList.get(i).getArgList().get(j);
        newAggArgColumns.add(col);
      }
    }

    return newAggArgColumns;
  }
}
