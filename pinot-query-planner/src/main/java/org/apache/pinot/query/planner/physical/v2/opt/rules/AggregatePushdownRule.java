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
package org.apache.pinot.query.planner.physical.v2.opt.rules;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.pinot.calcite.rel.hint.PinotHintOptions;
import org.apache.pinot.calcite.rel.hint.PinotHintStrategyTable;
import org.apache.pinot.calcite.rel.rules.GroupingSetsPlanUtils;
import org.apache.pinot.calcite.rel.rules.PinotRuleUtils;
import org.apache.pinot.calcite.rel.traits.PinotExecStrategyTrait;
import org.apache.pinot.common.function.sql.PinotSqlAggFunction;
import org.apache.pinot.query.context.PhysicalPlannerContext;
import org.apache.pinot.query.planner.physical.v2.PRelNode;
import org.apache.pinot.query.planner.physical.v2.PinotDataDistribution;
import org.apache.pinot.query.planner.physical.v2.mapping.DistMappingGenerator;
import org.apache.pinot.query.planner.physical.v2.mapping.PinotDistMapping;
import org.apache.pinot.query.planner.physical.v2.nodes.PhysicalAggregate;
import org.apache.pinot.query.planner.physical.v2.nodes.PhysicalExchange;
import org.apache.pinot.query.planner.physical.v2.nodes.PhysicalProject;
import org.apache.pinot.query.planner.physical.v2.opt.PRelOptRule;
import org.apache.pinot.query.planner.physical.v2.opt.PRelOptRuleCall;
import org.apache.pinot.query.planner.plannode.AggregateNode.AggType;
import org.apache.pinot.segment.spi.AggregationFunctionType;


/**
 * Does the following:
 * 1. Converts agg calls to their proper forms.
 * 2. Adds aggregate under Exchange, if exchange is an input.
 * 3. Handles leafReturnFinalResult thing.
 */
public class AggregatePushdownRule extends PRelOptRule {
  private final PhysicalPlannerContext _context;

  public AggregatePushdownRule(PhysicalPlannerContext context) {
    _context = context;
  }

  @Override
  public boolean matches(PRelOptRuleCall call) {
    return call._currentNode instanceof Aggregate;
  }

  @Override
  public PRelNode onMatch(PRelOptRuleCall call) {
    Aggregate aggRel = (Aggregate) call._currentNode;
    Preconditions.checkState(aggRel instanceof PhysicalAggregate, "Expected PhysicalAggregate, got %s", aggRel);
    boolean hasGroupBy = !aggRel.getGroupSet().isEmpty();
    RelCollation withinGroupCollation = extractWithinGroupCollation(aggRel);
    Map<String, String> hintOptions =
        PinotHintStrategyTable.getHintOptions(aggRel.getHints(), PinotHintOptions.AGGREGATE_HINT_OPTIONS);
    if (hintOptions == null) {
      hintOptions = Map.of();
    }
    boolean isInputExchange = call._currentNode.unwrap().getInput(0) instanceof Exchange;
    if (aggRel.getGroupType() != Aggregate.Group.SIMPLE) {
      /// GROUP BY GROUPING SETS / ROLLUP / CUBE: split with the synthetic $groupingId discriminator carried through the
      /// exchange (the runtime RepeatOperator does the per-set row expansion). Requires an input exchange to
      /// repartition by union keys + $groupingId.
      /// A WITHIN GROUP ordered aggregate (e.g. LISTAGG ... WITHIN GROUP) under a grouping set cannot be leaf/final
      /// split without losing the ORDER BY, and the DIRECT fallback does not preserve the ordering across the per-set
      /// expansion either. Reject explicitly (run on the single-stage engine) rather than silently returning a wrong
      /// result; the default planner rejects the same combination.
      if (withinGroupCollation != null) {
        throw new UnsupportedOperationException(
            "WITHIN GROUP ordered aggregates with GROUP BY GROUPING SETS / ROLLUP / CUBE are not supported in the "
                + "multi-stage v2 physical planner. Run the query on the single-stage query engine instead.");
      }
      if (!isInputExchange) {
        throw new UnsupportedOperationException(
            "GROUP BY GROUPING SETS / ROLLUP / CUBE without a repartitioning exchange is not yet supported in the "
                + "multi-stage v2 physical planner. Run the query on the single-stage query engine instead.");
      }
      /// Aggregate hints (e.g. is_leaf_return_final_result) are not applied to grouping-set splits.
      return addPartialAggregateForGroupingSets((PhysicalAggregate) call._currentNode, _context.getNodeIdGenerator());
    }
    /// GROUPING() / GROUPING_ID() with a plain GROUP BY is constant 0 per the SQL standard; it is not wired into
    /// the multi-stage runtime (it is not a real aggregation function), so reject it explicitly rather than failing
    /// with an obscure error at execution. TODO: fold it to the literal 0 for single-stage parity.
    rejectGroupingFunctionWithPlainGroupBy(aggRel);
    if (!isInputExchange || withinGroupCollation != null || (hasGroupBy && Boolean.parseBoolean(
        hintOptions.get(PinotHintOptions.AggregateOptions.IS_SKIP_LEAF_STAGE_GROUP_BY)))) {
      return skipPartialAggregate(call._currentNode);
    }
    return addPartialAggregate((PhysicalAggregate) call._currentNode, hintOptions, _context.getNodeIdGenerator());
  }

  /// Rejects GROUPING() / GROUPING_ID() calls on a plain (single-grouping-set) aggregate.
  private static void rejectGroupingFunctionWithPlainGroupBy(Aggregate aggRel) {
    for (AggregateCall aggCall : aggRel.getAggCallList()) {
      SqlKind kind = aggCall.getAggregation().getKind();
      if (kind == SqlKind.GROUPING || kind == SqlKind.GROUPING_ID) {
        throw new UnsupportedOperationException(
            "GROUPING() / GROUPING_ID() requires GROUP BY GROUPING SETS / ROLLUP / CUBE in the multi-stage query "
                + "engine. Run the query on the single-stage query engine instead.");
      }
    }
  }

  private static PRelNode skipPartialAggregate(PRelNode aggPRelNode) {
    PhysicalAggregate aggRel = (PhysicalAggregate) aggPRelNode.unwrap();
    List<AggregateCall> newAggCalls = buildAggCalls(aggRel, AggType.DIRECT, false);
    return new PhysicalAggregate(aggRel.getCluster(), aggRel.getTraitSet(), aggRel.getHints(), aggRel.getGroupSet(),
        aggRel.groupSets, newAggCalls, aggRel.getNodeId(), aggRel.getPRelInput(0),
        aggRel.getPinotDataDistributionOrThrow(), aggRel.isLeafStage(), AggType.DIRECT,
        false /* leaf return final agg */, aggRel.getCollations(), aggRel.getLimit());
  }

  /// Splits a GROUP BY GROUPING SETS / ROLLUP / CUBE aggregate (v2 physical planner). Mirrors the SIMPLE split
  /// {@link #addPartialAggregate} but threads the synthetic {@code $groupingId} discriminator column, exactly like the
  /// single-stage rule PinotAggregateExchangeNodeInsertRule:
  ///   - LEAF carries the grouping sets and only the real aggregations; {@link PhysicalAggregate#deriveRowType} appends
  ///     {@code $groupingId} after the union group keys. The per-set row expansion happens at runtime in the
  ///     RepeatOperator.
  ///   - EXCHANGE repartitions by the union keys AND {@code $groupingId} (so all rows of a (set, key) co-locate).
  ///   - FINAL is a SIMPLE aggregate grouping by {@code [union keys..., $groupingId]}.
  ///   - PROJECT restores the original aggregate row type: the group keys, then for each original aggregate call either
  ///     the real aggregate result or, for GROUPING() / GROUPING_ID(), the value computed from {@code $groupingId};
  ///     {@code $groupingId} itself is dropped.
  private static PRelNode addPartialAggregateForGroupingSets(PhysicalAggregate aggPRelNode,
      Supplier<Integer> idGenerator) {
    PhysicalAggregate o0 = aggPRelNode;
    PhysicalExchange o1 = (PhysicalExchange) o0.getPRelInput(0);
    PRelNode o2 = o1.getPRelInput(0);
    int groupCount = o0.getGroupCount();
    /// GROUPING() / GROUPING_ID() are not real aggregations: they are computed from $groupingId in the final
    /// projection, so they are split out of the LEAF/FINAL aggregate calls here (shared with the default planner).
    List<AggregateCall> orgAggCalls = o0.getAggCallList();
    List<AggregateCall> realAggCalls = new ArrayList<>(orgAggCalls.size());
    int[] realAggIndex = GroupingSetsPlanUtils.splitOutGroupingCalls(orgAggCalls, realAggCalls);
    /// Without a real aggregation the LEAF would push an aggregation-free query down to the single-stage engine,
    /// which would execute it as a plain selection and ignore the grouping sets. Mirrors the single-stage parser
    /// rejection (GROUPING()/GROUPING_ID() are not aggregations).
    if (realAggCalls.isEmpty()) {
      throw new UnsupportedOperationException(
          "GROUP BY GROUPING SETS / ROLLUP / CUBE requires at least one aggregation function in the query");
    }
    /// LEAF: carries the grouping sets and only the real aggregations; deriveRowType() appends $groupingId at position
    /// groupCount, so the leaf output is [union keys..., $groupingId, real aggregates...].
    /// Group trim (collations/limit) is not applied to the grouping-set LEAF: the collation indexes were
    /// extracted against the original [union keys..., aggs...] layout and would be off by one past the inserted
    /// $groupingId column (the default planner disables trim for grouping sets for the same reason).
    PhysicalAggregate n2 = new PhysicalAggregate(o0.getCluster(), RelTraitSet.createEmpty(), List.of() /* hints */,
        o0.getGroupSet(), o0.groupSets, buildAggCalls(o0, realAggCalls, AggType.LEAF, false), idGenerator.get(), o2,
        null /* data dist */, o2.isLeafStage(), AggType.LEAF, false, List.of() /* collations */, 0 /* limit */);
    PinotDistMapping mapFromInputToPartialAgg = DistMappingGenerator.compute(o2.unwrap(), n2, null);
    PinotDataDistribution leafAggDataDistribution =
        o2.getPinotDataDistributionOrThrow().apply(mapFromInputToPartialAgg);
    n2 = (PhysicalAggregate) n2.with(n2.getPRelInputs(), leafAggDataDistribution);
    /// EXCHANGE: hash by the union keys plus $groupingId (at position groupCount in the leaf output).
    List<Integer> mappedUnionKeys = mapFromInputToPartialAgg.getMappedKeys(o1.getDistributionKeys()).get(0);
    List<Integer> newDistKeys = new ArrayList<>(mappedUnionKeys);
    newDistKeys.add(groupCount);
    PhysicalExchange n1 = new PhysicalExchange(o1.getNodeId(), n2,
        o1.getPinotDataDistributionOrThrow().apply(mapFromInputToPartialAgg), newDistKeys, o1.getExchangeStrategy(),
        null /* collation */, PinotExecStrategyTrait.getDefaultExecStrategy(), o1.getHashFunction());
    /// FINAL: SIMPLE aggregate grouping by [union keys..., $groupingId]; the real aggregate input refs start at
    /// groupCount + 1 in the exchange (intermediate) output.
    int finalGroupCount = groupCount + 1;
    PhysicalAggregate n0 = convertAggForGroupingSets(o0, n1, realAggCalls, finalGroupCount, idGenerator);
    /// PROJECT: restore the original aggregate row type via the shared projection builder (also used by the
    /// default planner), dropping $groupingId.
    List<RexNode> projects =
        GroupingSetsPlanUtils.buildGroupingSetsProjects(o0.getCluster().getRexBuilder(), n0, o0, realAggIndex);
    return new PhysicalProject(o0.getCluster(), o0.getTraitSet(), List.of() /* hints */, projects, o0.getRowType(),
        Set.of(), idGenerator.get(), n0, n0.getPinotDataDistributionOrThrow(), false);
  }

  /// FINAL aggregate for a grouping-set split: like {@link #convertAggFromIntermediateInput} but groups by
  /// {@code [union keys..., $groupingId]} ({@code finalGroupCount = groupCount + 1}), aggregates only the real
  /// (non-GROUPING) calls, and reads the intermediate aggregate columns starting at {@code finalGroupCount}.
  private static PhysicalAggregate convertAggForGroupingSets(PhysicalAggregate physicalAggregate,
      PhysicalExchange exchange, List<AggregateCall> realAggCalls, int finalGroupCount, Supplier<Integer> nodeId) {
    Aggregate aggRel = (Aggregate) physicalAggregate.unwrap();
    List<RexNode> projects = findImmediateProjects(aggRel.getInput());
    List<AggregateCall> aggCalls = new ArrayList<>(realAggCalls.size());
    for (int i = 0; i < realAggCalls.size(); i++) {
      AggregateCall orgAggCall = realAggCalls.get(i);
      List<Integer> argList = orgAggCall.getArgList();
      RexInputRef inputRef = RexInputRef.of(finalGroupCount + i, exchange.getRowType());
      int numArguments = argList.size();
      List<RexNode> rexList;
      if (numArguments <= 1) {
        rexList = List.of(inputRef);
      } else {
        rexList = new ArrayList<>(numArguments);
        rexList.add(inputRef);
        for (int j = 1; j < numArguments; j++) {
          int argument = argList.get(j);
          if (projects != null && projects.get(argument) instanceof RexLiteral) {
            rexList.add(projects.get(argument));
          } else {
            rexList.add(inputRef);
          }
        }
      }
      aggCalls.add(buildAggCall(exchange, orgAggCall, rexList, finalGroupCount, AggType.FINAL, false));
    }
    ImmutableBitSet groupSet = ImmutableBitSet.range(finalGroupCount);
    return new PhysicalAggregate(aggRel.getCluster(), aggRel.getTraitSet(), aggRel.getHints(), groupSet,
        List.of(groupSet), aggCalls, nodeId.get(), exchange, physicalAggregate.getPinotDataDistributionOrThrow(),
        false, AggType.FINAL, false, List.of(), 0);
  }

  private static PRelNode addPartialAggregate(PhysicalAggregate aggPRelNode, Map<String, String> hintOptions,
      Supplier<Integer> idGenerator) {
    // Old: Aggregate (o0) > Exchange (o1) > Input (o2)
    // New: Aggregate (n0) > Exchange (n1) > Aggregate (n2) > Input (o2)
    boolean leafReturnFinalResult =
        Boolean.parseBoolean(hintOptions.get(PinotHintOptions.AggregateOptions.IS_LEAF_RETURN_FINAL_RESULT));
    // init old PRelNodes
    PhysicalAggregate o0 = aggPRelNode;
    PhysicalExchange o1 = (PhysicalExchange) o0.getPRelInput(0);
    PRelNode o2 = o1.getPRelInput(0);
    // Create n2
    PhysicalAggregate n2 = new PhysicalAggregate(o0.getCluster(), RelTraitSet.createEmpty(), List.of() /* hints */,
        o0.getGroupSet(), o0.groupSets, buildAggCalls(o0, AggType.LEAF, leafReturnFinalResult), idGenerator.get(),
        o2, null /* data dist */, o2.isLeafStage(), AggType.LEAF, leafReturnFinalResult, aggPRelNode.getCollations(),
        aggPRelNode.getLimit());
    PinotDistMapping mapFromInputToPartialAgg = DistMappingGenerator.compute(o2.unwrap(), n2, null);
    PinotDataDistribution leafAggDataDistribution = o2.getPinotDataDistributionOrThrow().apply(
        mapFromInputToPartialAgg);
    n2 = (PhysicalAggregate) n2.with(n2.getPRelInputs(), leafAggDataDistribution);
    // Create n1.
    List<Integer> newDistKeys = mapFromInputToPartialAgg.getMappedKeys(o1.getDistributionKeys()).get(0);
    RelCollation newCollation = o1.getRelCollation() == null ? null
        : PinotDistMapping.apply(o1.getRelCollation(), mapFromInputToPartialAgg);
    PhysicalExchange n1 = new PhysicalExchange(o1.getNodeId(), n2,
        o1.getPinotDataDistributionOrThrow().apply(mapFromInputToPartialAgg), newDistKeys, o1.getExchangeStrategy(),
        newCollation, PinotExecStrategyTrait.getDefaultExecStrategy(), o1.getHashFunction());
    return convertAggFromIntermediateInput(aggPRelNode, n1, AggType.FINAL, leafReturnFinalResult,
        PinotDistMapping.apply(RelCollations.of(o0.getCollations()), mapFromInputToPartialAgg).getFieldCollations(),
        aggPRelNode.getLimit(), idGenerator);
  }

  // TODO: Currently it only handles one WITHIN GROUP collation across all AggregateCalls.
  @Nullable
  private static RelCollation extractWithinGroupCollation(Aggregate aggRel) {
    for (AggregateCall aggCall : aggRel.getAggCallList()) {
      RelCollation collation = aggCall.getCollation();
      if (!collation.getFieldCollations().isEmpty()) {
        return collation;
      }
    }
    return null;
  }

  private static PhysicalAggregate convertAggFromIntermediateInput(PhysicalAggregate physicalAggregate,
      PhysicalExchange exchange, AggType aggType, boolean leafReturnFinalResult,
      @Nullable List<RelFieldCollation> collations, int limit, Supplier<Integer> nodeId) {
    Aggregate aggRel = (Aggregate) physicalAggregate.unwrap();
    RelNode input = aggRel.getInput();
    List<RexNode> projects = findImmediateProjects(input);
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
        rexList = List.of(inputRef);
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
      aggCalls.add(buildAggCall(exchange, orgAggCall, rexList, groupCount, aggType, leafReturnFinalResult));
    }
    ImmutableBitSet.Builder groupSetBuilder = ImmutableBitSet.builder();
    for (int i = 0; i < groupCount; i++) {
      groupSetBuilder.set(i);
    }
    ImmutableBitSet groupSet = groupSetBuilder.build();
    List<ImmutableBitSet> groupSets = null;
    if (!groupSet.isEmpty()) {
      groupSets = List.of(groupSet);
    }
    return new PhysicalAggregate(aggRel.getCluster(), aggRel.getTraitSet(), aggRel.getHints(), groupSet,
        groupSets, aggCalls, nodeId.get(), exchange, physicalAggregate.getPinotDataDistributionOrThrow(),
        false, aggType, leafReturnFinalResult, collations, limit);
  }

  public static List<AggregateCall> buildAggCalls(Aggregate aggRel, AggType aggType, boolean leafReturnFinalResult) {
    return buildAggCalls(aggRel, aggRel.getAggCallList(), aggType, leafReturnFinalResult);
  }

  /// As {@link #buildAggCalls(Aggregate, AggType, boolean)} but over an explicit list of aggregate calls (rather than
  /// {@code aggRel.getAggCallList()}). The grouping-set split passes only the real (non-GROUPING) aggregates.
  public static List<AggregateCall> buildAggCalls(Aggregate aggRel, List<AggregateCall> orgAggCalls, AggType aggType,
      boolean leafReturnFinalResult) {
    RelNode input = aggRel.getInput();
    List<RexNode> projects = findImmediateProjects(input);
    List<AggregateCall> aggCalls = new ArrayList<>(orgAggCalls.size());
    for (AggregateCall orgAggCall : orgAggCalls) {
      // Generate rexList from argList and replace literal reference with literal. Keep the first argument as is.
      List<Integer> argList = orgAggCall.getArgList();
      int numArguments = argList.size();
      List<RexNode> rexList;
      if (numArguments == 0) {
        rexList = List.of();
      } else if (numArguments == 1) {
        rexList = List.of(RexInputRef.of(argList.get(0), input.getRowType()));
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
      aggCalls.add(buildAggCall(input, orgAggCall, rexList, aggRel.getGroupCount(), aggType, leafReturnFinalResult));
    }
    return aggCalls;
  }

  // TODO: Revisit the following logic:
  //   - DISTINCT is resolved here
  //   - argList is replaced with rexList
  private static AggregateCall buildAggCall(RelNode input, AggregateCall orgAggCall, List<RexNode> rexList,
      int numGroups, AggType aggType, boolean leafReturnFinalResult) {
    SqlAggFunction orgAggFunction = orgAggCall.getAggregation();
    String functionName = orgAggFunction.getName();
    SqlKind kind = orgAggFunction.getKind();
    SqlFunctionCategory functionCategory = orgAggFunction.getFunctionType();
    if (orgAggCall.isDistinct()) {
      if (kind == SqlKind.COUNT) {
        functionName = "DISTINCTCOUNT";
        kind = SqlKind.OTHER_FUNCTION;
        functionCategory = SqlFunctionCategory.USER_DEFINED_FUNCTION;
      } else if (kind == SqlKind.LISTAGG) {
        rexList.add(input.getCluster().getRexBuilder().makeLiteral(true));
      }
    }
    SqlReturnTypeInference returnTypeInference = null;
    RelDataType returnType = null;
    // Override the intermediate result type inference if it is provided
    if (aggType.isOutputIntermediateFormat()) {
      AggregationFunctionType functionType = AggregationFunctionType.getAggregationFunctionType(functionName);
      returnTypeInference = leafReturnFinalResult ? functionType.getFinalReturnTypeInference()
          : functionType.getIntermediateReturnTypeInference();
    }
    // When the output is not intermediate format, or intermediate result type inference is not provided (intermediate
    // result type the same as final result type), use the explicit return type
    if (returnTypeInference == null) {
      returnType = orgAggCall.getType();
      returnTypeInference = ReturnTypes.explicit(returnType);
    }
    SqlOperandTypeChecker operandTypeChecker =
        aggType.isInputIntermediateFormat() ? OperandTypes.ANY : orgAggFunction.getOperandTypeChecker();
    SqlAggFunction sqlAggFunction =
        new PinotSqlAggFunction(functionName, kind, returnTypeInference, operandTypeChecker, functionCategory);
    return AggregateCall.create(sqlAggFunction, false, orgAggCall.isApproximate(), orgAggCall.ignoreNulls(), rexList,
        List.of(), aggType.isInputIntermediateFormat() ? -1 : orgAggCall.filterArg, orgAggCall.distinctKeys,
        orgAggCall.collation, numGroups, input, returnType, null);
  }

  @Nullable
  private static List<RexNode> findImmediateProjects(RelNode relNode) {
    relNode = PinotRuleUtils.unboxRel(relNode);
    if (relNode instanceof Project) {
      return ((Project) relNode).getProjects();
    } else if (relNode instanceof Union || relNode instanceof Exchange) {
      return findImmediateProjects(relNode.getInput(0));
    }
    return null;
  }
}
