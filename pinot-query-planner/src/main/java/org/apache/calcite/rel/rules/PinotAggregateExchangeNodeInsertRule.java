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
import javax.annotation.Nullable;
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
import org.apache.calcite.sql.PinotSqlAggFunction;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.PinotOperatorTable;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.pinot.query.planner.plannode.AggregateNode.AggType;
import org.apache.pinot.segment.spi.AggregationFunctionType;


/**
 * Special rule for Pinot, this rule is fixed to generate a 3-stage aggregation split between the
 * (1) non-data-locale Pinot server agg stage, (2) the data-locale Pinot intermediate agg stage, and
 * (3) final result Pinot final agg stage.
 *
 * Pinot uses special intermediate data representation for partially aggregated results, thus we can't use
 * {@link org.apache.calcite.rel.rules.AggregateReduceFunctionsRule} to reduce complex aggregation.
 *
 * This rule is here to introduces Pinot-special aggregation splits. In-general, all aggregations are split into
 * final-stage AGG, intermediate-stage AGG, and server-stage AGG with the same naming. E.g.
 *
 * COUNT(*) transforms into: COUNT(*)_SERVER --> COUNT(*)_INTERMEDIATE --> COUNT(*)_FINAL, where
 *   COUNT(*)_SERVER produces TUPLE[ COUNT(data), GROUP_BY_KEY ]
 *   COUNT(*)_INTERMEDIATE produces TUPLE[ SUM(COUNT(*)_SERVER), GROUP_BY_KEY ] (intermediate result here is the count)
 *   COUNT(*)_FINAL produces the final TUPLE[ FINAL_COUNT, GROUP_BY_KEY ]
 *
 * Taking an example of a function which has a different intermediate object representation than the final result:
 * KURTOSIS(*) transforms into: 4THMOMENT(*)_SERVER --> 4THMOMENT(*)_INTERMEDIATE --> KURTOSIS(*)_FINAL, where
 *   FOURTHMOMENT(*)_SERVER produces TUPLE[ 4THMOMENT(data) object, GROUP_BY_KEY ] (input: rowType, output: object)
 *   FOURTHMOMENT(*)_INTERMEDIATE produces TUPLE[ 4THMOMENT(4THMOMENT(*)_SERVER), GROUP_BY_KEY ] (input, output: object)
 *   KURTOSIS(*)_FINAL produces the final TUPLE[ KURTOSIS(4THMOMENT(*)_INTERMEDIATE), GROUP_BY_KEY ]
 *     (input: object, output: double)
 *
 * However, the suffix _SERVER/_INTERMEDIATE/_FINAL is merely a SQL hint to the Aggregate operator and will be
 * translated into correct, actual operator chain during Physical plan.
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

    // If the "is_partitioned_by_group_by_keys" aggregate hint option is set, just add additional hints indicating
    // this is a single stage aggregation and intermediate stage. This only applies to GROUP BY aggregations.
    if (!oldAggRel.getGroupSet().isEmpty() && PinotHintStrategyTable.containsHintOption(oldHints,
        PinotHintOptions.AGGREGATE_HINT_OPTIONS, PinotHintOptions.AggregateOptions.IS_PARTITIONED_BY_GROUP_BY_KEYS)) {
      // 1. attach intermediate agg and skip leaf stage RelHints to original agg.
      ImmutableList<RelHint> newLeafAggHints =
          new ImmutableList.Builder<RelHint>().addAll(oldHints).add(createAggHint(AggType.DIRECT)).build();
      Aggregate newAgg =
          new LogicalAggregate(oldAggRel.getCluster(), oldAggRel.getTraitSet(), newLeafAggHints, oldAggRel.getInput(),
              oldAggRel.getGroupSet(), oldAggRel.getGroupSets(), oldAggRel.getAggCallList());
      call.transformTo(newAgg);
      return;
    }

    // If "is_skip_leaf_stage_group_by" SQLHint option is passed, the leaf stage aggregation is skipped.
    if (!oldAggRel.getGroupSet().isEmpty() && PinotHintStrategyTable.containsHintOption(oldHints,
        PinotHintOptions.AGGREGATE_HINT_OPTIONS,
        PinotHintOptions.AggregateOptions.SKIP_LEAF_STAGE_GROUP_BY_AGGREGATION)) {
      // This is not the default path. Use this group by optimization to skip leaf stage aggregation when aggregating
      // at leaf level could be wasted effort. eg: when cardinality of group by column is very high.
      Aggregate newAgg = (Aggregate) createPlanWithoutLeafAggregation(call);
      call.transformTo(newAgg);
      return;
    }

    // 1. attach leaf agg RelHint to original agg. Perform any aggregation call conversions necessary
    ImmutableList<RelHint> newLeafAggHints =
        new ImmutableList.Builder<RelHint>().addAll(oldHints).add(createAggHint(AggType.LEAF)).build();
    Aggregate newLeafAgg =
        new LogicalAggregate(oldAggRel.getCluster(), oldAggRel.getTraitSet(), newLeafAggHints, oldAggRel.getInput(),
            oldAggRel.getGroupSet(), oldAggRel.getGroupSets(), convertLeafAggCalls(oldAggRel));

    // 2. attach exchange.
    List<Integer> groupSetIndices = ImmutableIntList.range(0, oldAggRel.getGroupCount());
    PinotLogicalExchange exchange;
    if (groupSetIndices.size() == 0) {
      exchange = PinotLogicalExchange.create(newLeafAgg, RelDistributions.hash(Collections.emptyList()));
    } else {
      exchange = PinotLogicalExchange.create(newLeafAgg, RelDistributions.hash(groupSetIndices));
    }

    // 3. attach intermediate agg stage.
    RelNode newAggNode = makeNewIntermediateAgg(call, oldAggRel, exchange, AggType.INTERMEDIATE, null, null);

    // 4. attach final agg stage if aggregations are present.
    RelNode transformToAgg = newAggNode;
    if (oldAggRel.getAggCallList() != null && oldAggRel.getAggCallList().size() > 0) {
      transformToAgg = makeNewFinalAgg(call, oldAggRel, newAggNode);
    }

    call.transformTo(transformToAgg);
  }

  private List<AggregateCall> convertLeafAggCalls(Aggregate oldAggRel) {
    List<AggregateCall> oldCalls = oldAggRel.getAggCallList();
    List<AggregateCall> newCalls = new ArrayList<>();
    for (AggregateCall oldCall : oldCalls) {
      newCalls.add(buildAggregateCall(oldAggRel.getInput(), oldCall.getArgList(), oldAggRel.getGroupCount(), oldCall,
          oldCall, false));
    }
    return newCalls;
  }

  private RelNode makeNewIntermediateAgg(RelOptRuleCall ruleCall, Aggregate oldAggRel, PinotLogicalExchange exchange,
      AggType aggType, @Nullable List<Integer> argList, @Nullable List<Integer> groupByList) {

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
      convertIntermediateAggCall(rexBuilder, oldAggRel, oldCallIndex, oldCall, newCalls, aggCallMapping,
          aggType, argList, exchange);
    }

    // create new aggregate relation.
    ImmutableList<RelHint> orgHints = oldAggRel.getHints();
    // if the aggregation isn't split between intermediate and final stages, indicate that this is a single stage
    // aggregation so that the execution engine knows whether to aggregate or merge
    ImmutableList<RelHint> newIntermediateAggHints =
        new ImmutableList.Builder<RelHint>().addAll(orgHints).add(createAggHint(aggType)).build();
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
   */
  private static void convertIntermediateAggCall(RexBuilder rexBuilder, Aggregate oldAggRel, int oldCallIndex,
      AggregateCall oldCall, List<AggregateCall> newCalls, Map<AggregateCall, RexNode> aggCallMapping,
      AggType aggType, List<Integer> argList, PinotLogicalExchange exchange) {
    final int nGroups = oldAggRel.getGroupCount();
    final SqlAggFunction oldAggregation = oldCall.getAggregation();

    List<Integer> newArgList;
    if (aggType.isInputIntermediateFormat()) {
      // Make sure COUNT in the intermediate stage takes an argument
      List<Integer> oldArgList = (oldAggregation.getKind() == SqlKind.COUNT && !oldCall.isDistinct())
          ? Collections.singletonList(oldCallIndex)
          : oldCall.getArgList();
      newArgList = convertArgList(nGroups + oldCallIndex, oldArgList);
    } else {
      newArgList = oldCall.getArgList().size() == 0 ? Collections.emptyList()
          : Collections.singletonList(argList.get(oldCallIndex));
    }
    AggregateCall newCall = buildAggregateCall(exchange, newArgList, nGroups, oldCall, oldCall, false);
    rexBuilder.addAggCall(newCall, nGroups, newCalls, aggCallMapping, oldAggRel.getInput()::fieldIsNullable);
  }

  private RelNode makeNewFinalAgg(RelOptRuleCall ruleCall, Aggregate oldAggRel, RelNode newIntAggNode) {
    // add the intermediate agg node as the input node to the relation builder.
    RelBuilder relBuilder = ruleCall.builder();
    relBuilder.push(newIntAggNode);

    Aggregate aggIntNode = (Aggregate) newIntAggNode;

    // make input ref to the intermediate agg node after the leaf aggregate.
    RexBuilder rexBuilder = newIntAggNode.getCluster().getRexBuilder();
    final int nGroups = aggIntNode.getGroupCount();
    for (int i = 0; i < nGroups; i++) {
      rexBuilder.makeInputRef(aggIntNode, i);
    }

    // create new aggregate function calls from intermediate agg input.
    List<AggregateCall> oldCalls = oldAggRel.getAggCallList();
    List<AggregateCall> oldCallsIntAgg = aggIntNode.getAggCallList();
    List<AggregateCall> newCalls = new ArrayList<>();
    Map<AggregateCall, RexNode> aggCallMapping = new HashMap<>();

    for (int oldCallIndex = 0; oldCallIndex < oldCalls.size(); oldCallIndex++) {
      AggregateCall oldCall = oldCalls.get(oldCallIndex);
      AggregateCall oldCallIntAgg = oldCallsIntAgg.get(oldCallIndex);
      convertFinalAggCall(rexBuilder, aggIntNode, oldCallIndex, oldCall, oldCallIntAgg, newCalls, aggCallMapping);
    }

    // create new aggregate relation.
    ImmutableList<RelHint> orgHints = oldAggRel.getHints();
    ImmutableList<RelHint> newIntermediateAggHints =
        new ImmutableList.Builder<RelHint>().addAll(orgHints).add(createAggHint(AggType.FINAL)).build();
    ImmutableBitSet groupSet = ImmutableBitSet.range(nGroups);
    relBuilder.aggregate(
        relBuilder.groupKey(groupSet, ImmutableList.of(groupSet)),
        newCalls);
    relBuilder.hints(newIntermediateAggHints);
    return relBuilder.build();
  }

  /**
   * convert aggregate call based on the final stage input.
   */
  private static void convertFinalAggCall(RexBuilder rexBuilder, Aggregate inputAggRel, int oldCallIndex,
      AggregateCall oldCall, AggregateCall oldCallIntAgg, List<AggregateCall> newCalls, Map<AggregateCall,
      RexNode> aggCallMapping) {
    final int nGroups = inputAggRel.getGroupCount();
    final SqlAggFunction oldAggregation = oldCallIntAgg.getAggregation();
    // Make sure COUNT in the final stage takes an argument
    List<Integer> oldArgList = (oldAggregation.getKind() == SqlKind.COUNT && !oldCallIntAgg.isDistinct())
        ? Collections.singletonList(oldCallIndex)
        : oldCallIntAgg.getArgList();
    List<Integer> newArgList = convertArgList(nGroups + oldCallIndex, oldArgList);
    AggregateCall newCall = buildAggregateCall(inputAggRel, newArgList, nGroups, oldCallIntAgg, oldCall, true);
    rexBuilder.addAggCall(newCall, nGroups, newCalls, aggCallMapping, inputAggRel.getInput()::fieldIsNullable);
  }

  private static AggregateCall buildAggregateCall(RelNode input, List<Integer> newArgList, int numberGroups,
      AggregateCall inputCall, AggregateCall functionNameCall, boolean isFinalStage) {
    final SqlAggFunction oldAggregation = functionNameCall.getAggregation();
    final SqlKind aggKind = oldAggregation.getKind();
    String functionName = getFunctionNameFromAggregateCall(functionNameCall);
    // Check only the supported AGG functions are provided.
    validateAggregationFunctionIsSupported(functionName, aggKind);

    AggregationFunctionType type = AggregationFunctionType.getAggregationFunctionType(functionName);
    // Use the actual function name and return type for final stage to ensure that for aggregation functions that share
    // leaf and intermediate functions, we can correctly extract the correct final result. e.g. KURTOSIS and SKEWNESS
    // both use FOURTHMOMENT
    String aggregationFunctionName = isFinalStage ? type.getName().toUpperCase(Locale.ROOT)
        : type.getIntermediateFunctionName().toUpperCase(Locale.ROOT);
    SqlReturnTypeInference returnTypeInference = isFinalStage ? type.getSqlReturnTypeInference()
        : type.getSqlIntermediateReturnTypeInference();
    SqlAggFunction sqlAggFunction =
        new PinotSqlAggFunction(aggregationFunctionName, type.getSqlIdentifier(), type.getSqlKind(),
            returnTypeInference, type.getSqlOperandTypeInference(), type.getSqlOperandTypeChecker(),
            type.getSqlFunctionCategory());

    return AggregateCall.create(sqlAggFunction,
        functionName.equals("distinctCount") || inputCall.isDistinct(),
        inputCall.isApproximate(),
        inputCall.ignoreNulls(),
        newArgList,
        inputCall.filterArg,
        inputCall.distinctKeys,
        inputCall.collation,
        numberGroups,
        input,
        null,
        null);
  }

  private static List<Integer> convertArgList(int oldCallIndexWithShift, List<Integer> argList) {
    Preconditions.checkArgument(argList.size() <= 1,
        "Unable to convert call as the argList contains more than 1 argument");
    return argList.size() == 1 ? Collections.singletonList(oldCallIndexWithShift) : Collections.emptyList();
  }

  private static String getFunctionNameFromAggregateCall(AggregateCall aggregateCall) {
    return aggregateCall.getAggregation().getName().equalsIgnoreCase("COUNT") && aggregateCall.isDistinct()
        ? "distinctCount" : aggregateCall.getAggregation().getName();
  }

  private static void validateAggregationFunctionIsSupported(String functionName, SqlKind aggKind) {
    Preconditions.checkState(PinotOperatorTable.isAggregationFunctionRegisteredWithOperatorTable(functionName)
            || PinotOperatorTable.isAggregationKindSupported(aggKind),
        String.format("Failed to create aggregation. Unsupported SQL aggregation kind: %s or function name: %s. "
                + "Only splittable aggregation functions are supported!", aggKind, functionName));
  }

  private RelNode createPlanWithoutLeafAggregation(RelOptRuleCall call) {
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
        makeNewIntermediateAgg(call, oldAggRel, exchange, AggType.LEAF, newAggArgColumns, newAggGroupByColumns);

    // 4. Create the final agg stage node on top of the intermediate agg if aggregations are present.
    RelNode transformToAgg = newAggNode;
    if (oldAggRel.getAggCallList() != null && oldAggRel.getAggCallList().size() > 0) {
      transformToAgg = makeNewFinalAgg(call, oldAggRel, newAggNode);
    }
    return transformToAgg;
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
    for (AggregateCall aggregateCall : oldAggCallList) {
      List<Integer> argList = aggregateCall.getArgList();
      if (argList.size() == 0) {
        newAggArgColumns.add(-1);
        continue;
      }
      for (Integer col : argList) {
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

    for (AggregateCall aggregateCall : oldAggCallList) {
      if (aggregateCall.getArgList().size() == 0) {
        // This can be true for COUNT. Add a placeholder value which will be ignored.
        newAggArgColumns.add(-1);
        continue;
      }
      newAggArgColumns.addAll(aggregateCall.getArgList());
    }

    return newAggArgColumns;
  }

  private static RelHint createAggHint(AggType aggType) {
    return RelHint.builder(PinotHintOptions.INTERNAL_AGG_OPTIONS)
        .hintOption(PinotHintOptions.InternalAggregateOptions.AGG_TYPE, aggType.name())
        .build();
  }
}
