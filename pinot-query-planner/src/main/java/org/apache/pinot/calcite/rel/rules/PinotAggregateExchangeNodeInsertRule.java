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

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.rules.AggregateExtractProjectRule;
import org.apache.calcite.rel.rules.AggregateReduceFunctionsRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
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
import org.apache.calcite.sql.type.SqlTypeName;
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
import org.apache.pinot.common.function.sql.PinotSqlAggFunction;
import org.apache.pinot.common.request.context.GroupingSets;
import org.apache.pinot.query.QueryEnvironment;
import org.apache.pinot.query.planner.plannode.AggregateNode.AggType;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.utils.CommonConstants;


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
 *
 * There are 3 sub-rules:
 * 1. {@link SortProjectAggregate}:
 *   Matches the case when there's a Sort on top of Project on top of Aggregate, and enable group trim hint is present.
 *   E.g.
 *     SELECT /*+ aggOptions(is_enable_group_trim='true') * /
 *     COUNT(*) AS cnt, col1 FROM myTable GROUP BY col1 ORDER BY cnt DESC LIMIT 10
 *   It will extract the collations and limit from the Sort node, and set them into the Aggregate node. It works only
 *   when the sort key is a direct reference to the input, i.e. no transform on the input columns.
 * 2. {@link SortAggregate}:
 *   Matches the case when there's a Sort on top of Aggregate, and enable group trim hint is present.
 *   E.g.
 *     SELECT /*+ aggOptions(is_enable_group_trim='true') * /
 *     col1, COUNT(*) AS cnt FROM myTable GROUP BY col1 ORDER BY cnt DESC LIMIT 10
 *   It will extract the collations and limit from the Sort node, and set them into the Aggregate node.
 * 3. {@link WithoutSort}:
 *   Matches Aggregate node if there is no match of {@link SortProjectAggregate} or {@link SortAggregate}.
 *
 * TODO:
 *   1. Always enable group trim when the result is guaranteed to be accurate
 *   2. Add intermediate stage group trim
 *   3. Allow tuning group trim parameters with query hint
 */
public class PinotAggregateExchangeNodeInsertRule {

  public static class SortProjectAggregate extends RelOptRule {
    public static final SortProjectAggregate INSTANCE = new SortProjectAggregate(PinotRuleUtils.PINOT_REL_FACTORY);

    private SortProjectAggregate(RelBuilderFactory factory) {
      // NOTE: Explicitly match for LogicalAggregate because after applying the rule, LogicalAggregate is replaced with
      //       PinotLogicalAggregate, and the rule won't be applied again.
      super(operand(Sort.class, operand(Project.class, operand(LogicalAggregate.class, any()))), factory, null);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      LogicalAggregate aggRel = call.rel(2);
      if (aggRel.getGroupSet().isEmpty()) {
        return;
      }
      Map<String, String> hintOptions =
          PinotHintStrategyTable.getHintOptions(aggRel.getHints(), PinotHintOptions.AGGREGATE_HINT_OPTIONS);

      if (!isGroupTrimmingEnabled(call, hintOptions, aggRel)) {
        return;
      } else if (hintOptions == null) {
        hintOptions = Map.of();
      }

      Sort sortRel = call.rel(0);
      Project projectRel = call.rel(1);
      List<RexNode> projects = projectRel.getProjects();
      List<RelFieldCollation> collations = sortRel.getCollation().getFieldCollations();
      List<RelFieldCollation> newCollations = new ArrayList<>(collations.size());
      for (RelFieldCollation fieldCollation : collations) {
        RexNode project = projects.get(fieldCollation.getFieldIndex());
        if (project instanceof RexInputRef) {
          newCollations.add(fieldCollation.withFieldIndex(((RexInputRef) project).getIndex()));
        } else {
          // Cannot enable group trim when the sort key is not a direct reference to the input.
          return;
        }
      }
      int limit = 0;
      if (sortRel.fetch != null) {
        limit = RexLiteral.intValue(sortRel.fetch);
      }
      if (limit <= 0) {
        // Cannot enable group trim when there is no limit.
        return;
      }

      RelNode newAggRel = createPlan(call, aggRel, true, hintOptions, newCollations, limit);
      RelNode newProjectRel = projectRel.copy(projectRel.getTraitSet(), List.of(newAggRel));
      call.transformTo(sortRel.copy(sortRel.getTraitSet(), List.of(newProjectRel)));
    }
  }

  public static class SortAggregate extends RelOptRule {
    public static final SortAggregate INSTANCE = new SortAggregate(PinotRuleUtils.PINOT_REL_FACTORY);

    private SortAggregate(RelBuilderFactory factory) {
      // NOTE: Explicitly match for LogicalAggregate because after applying the rule, LogicalAggregate is replaced with
      //       PinotLogicalAggregate, and the rule won't be applied again.
      super(operand(Sort.class, operand(LogicalAggregate.class, any())), factory, null);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      LogicalAggregate aggRel = call.rel(1);
      if (aggRel.getGroupSet().isEmpty()) {
        return;
      }

      Map<String, String> hintOptions =
          PinotHintStrategyTable.getHintOptions(aggRel.getHints(), PinotHintOptions.AGGREGATE_HINT_OPTIONS);

      if (!isGroupTrimmingEnabled(call, hintOptions, aggRel)) {
        return;
      } else if (hintOptions == null) {
        hintOptions = Map.of();
      }

      Sort sortRel = call.rel(0);
      List<RelFieldCollation> collations = sortRel.getCollation().getFieldCollations();
      int limit = 0;
      if (sortRel.fetch != null) {
        limit = RexLiteral.intValue(sortRel.fetch);
      }
      if (limit <= 0) {
        // Cannot enable group trim when there is no limit.
        return;
      }

      RelNode newAggRel = createPlan(call, aggRel, true, hintOptions, collations, limit);
      call.transformTo(sortRel.copy(sortRel.getTraitSet(), List.of(newAggRel)));
    }
  }

  public static class WithoutSort extends RelOptRule {
    public static final WithoutSort INSTANCE = new WithoutSort(PinotRuleUtils.PINOT_REL_FACTORY);

    private WithoutSort(RelBuilderFactory factory) {
      // NOTE: Explicitly match for LogicalAggregate because after applying the rule, LogicalAggregate is replaced with
      //       PinotLogicalAggregate, and the rule won't be applied again.
      super(operand(LogicalAggregate.class, any()), factory, null);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      Aggregate aggRel = call.rel(0);
      Map<String, String> hintOptions =
          PinotHintStrategyTable.getHintOptions(aggRel.getHints(), PinotHintOptions.AGGREGATE_HINT_OPTIONS);
      call.transformTo(
          createPlan(call, aggRel, !aggRel.getGroupSet().isEmpty(), hintOptions != null ? hintOptions : Map.of(), null,
              0));
    }
  }

  private static RelNode createPlan(RelOptRuleCall call, Aggregate aggRel, boolean hasGroupBy,
      Map<String, String> hintOptions, @Nullable List<RelFieldCollation> collations, int limit) {
    // WITHIN GROUP collation is not supported in leaf stage aggregation.
    RelCollation withinGroupCollation = extractWithinGroupCollation(aggRel);
    if (withinGroupCollation != null || (hasGroupBy && Boolean.parseBoolean(
        hintOptions.get(PinotHintOptions.AggregateOptions.IS_SKIP_LEAF_STAGE_GROUP_BY)))) {
      return createPlanWithExchangeDirectAggregation(call, aggRel, withinGroupCollation, collations, limit);
    } else if (hasGroupBy && Boolean.parseBoolean(
        hintOptions.get(PinotHintOptions.AggregateOptions.IS_PARTITIONED_BY_GROUP_BY_KEYS))) {
      return new PinotLogicalAggregate(aggRel, aggRel.getInput(), buildAggCalls(aggRel, AggType.DIRECT, false),
          AggType.DIRECT, false, collations, limit);
    } else {
      boolean leafReturnFinalResult =
          Boolean.parseBoolean(hintOptions.get(PinotHintOptions.AggregateOptions.IS_LEAF_RETURN_FINAL_RESULT));
      return createPlanWithLeafExchangeFinalAggregate(aggRel, leafReturnFinalResult, collations, limit);
    }
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

  /**
   * Use this group by optimization to skip leaf stage aggregation when aggregating at leaf level is not desired. Many
   * situation could be wasted effort to do group-by on leaf, eg: when cardinality of group by column is very high.
   */
  private static RelNode createPlanWithExchangeDirectAggregation(RelOptRuleCall call, Aggregate aggRel,
      @Nullable RelCollation withinGroupCollation, @Nullable List<RelFieldCollation> collations, int limit) {
    RelNode input = aggRel.getInput();
    // A layout-restoring Project that RelBuilder may add over the rebuilt aggregate (see below); when
    // present it is re-applied over the direct aggregate so the aggregate's output columns are
    // unchanged for the parent.
    Project topProject = null;
    // Create Project when there's none below the aggregate.
    if (!(PinotRuleUtils.unboxRel(input) instanceof Project)) {
      RelNode rewritten = PinotRuleUtils.unboxRel(generateProjectUnderAggregate(call, aggRel));
      if (rewritten instanceof Project) {
        // generateProjectUnderAggregate rebuilds the aggregate with RelBuilder, which deduplicates
        // identical aggregate calls and, when it does, adds a top Project to restore the original
        // output layout (a column referenced more than once — e.g. a SUM reused by an AVG reduction —
        // is projected twice). Keep that Project; the aggregate is its input, and we re-apply it over
        // the direct aggregate below. Without this, casting the Project to Aggregate threw
        // "LogicalProject cannot be cast to Aggregate".
        topProject = (Project) rewritten;
        aggRel = (Aggregate) PinotRuleUtils.unboxRel(topProject.getInput());
      } else {
        aggRel = (Aggregate) rewritten;
      }
      input = aggRel.getInput();
    }

    ImmutableBitSet groupSet = aggRel.getGroupSet();
    RelDistribution distribution = RelDistributions.hash(groupSet.asList());
    RelNode exchange;
    if (withinGroupCollation != null) {
      // Insert a LogicalSort node between exchange and aggregate whe collation exists.
      exchange = PinotLogicalSortExchange.create(input, distribution, withinGroupCollation, false, true);
    } else {
      exchange = PinotLogicalExchange.create(input, distribution);
    }

    PinotLogicalAggregate directAggregate = new PinotLogicalAggregate(aggRel, exchange,
        buildAggCalls(aggRel, AggType.DIRECT, false), AggType.DIRECT, false, collations, limit);
    if (topProject == null) {
      return directAggregate;
    }
    // Re-apply the layout-restoring Project over the direct aggregate (same projects and row type,
    // new input). The direct aggregate keeps the rebuilt aggregate's output columns, so the Project's
    // references remain valid.
    return topProject.copy(topProject.getTraitSet(), directAggregate, topProject.getProjects(),
        topProject.getRowType());
  }

  /**
   * Aggregate node will be split into LEAF + EXCHANGE + FINAL.
   * TODO: Add optional INTERMEDIATE stage to reduce hotspot.
   */
  private static RelNode createPlanWithLeafExchangeFinalAggregate(Aggregate aggRel, boolean leafReturnFinalResult,
      @Nullable List<RelFieldCollation> collations, int limit) {
    if (aggRel.getGroupType() != Aggregate.Group.SIMPLE) {
      return createGroupingSetsPlanWithLeafExchangeFinalAggregate(aggRel);
    }
    // Create a LEAF aggregate.
    PinotLogicalAggregate leafAggRel =
        new PinotLogicalAggregate(aggRel, aggRel.getInput(), buildAggCalls(aggRel, AggType.LEAF, leafReturnFinalResult),
            AggType.LEAF, leafReturnFinalResult, collations, limit);
    // Create an EXCHANGE node over the LEAF aggregate.
    PinotLogicalExchange exchange = PinotLogicalExchange.create(leafAggRel,
        RelDistributions.hash(ImmutableIntList.range(0, aggRel.getGroupCount())));
    // Create a FINAL aggregate over the EXCHANGE.
    return convertAggFromIntermediateInput(aggRel, exchange, AggType.FINAL, leafReturnFinalResult, collations, limit);
  }

  /// Splits a GROUP BY GROUPING SETS / ROLLUP / CUBE aggregate into LEAF + EXCHANGE + FINAL, pushing the per-set
  /// row expansion down to the single-stage (leaf) engine, which expands each row across the sets at the backend so
  /// the rest of the plan is an ordinary aggregate. Layout:
  ///   - LEAF (carries the grouping sets): the single-stage engine expands each row across the sets and appends the
  ///     synthetic {@code $groupingId} discriminator, so the leaf output is {@code [union keys..., $groupingId,
  ///     leaf aggregates...]} (see {@link PinotLogicalAggregate#deriveRowType}).
  ///   - EXCHANGE: hash-partition by the union keys AND {@code $groupingId} so all rows of a (set, key) co-locate.
  ///   - FINAL: a plain (SIMPLE) aggregate grouping by {@code [union keys..., $groupingId]}, so rows from different
  ///     grouping sets stay distinct with no grouping-set-specific merge logic.
  ///   - PROJECT: drop {@code $groupingId} so the output row type matches the original aggregate.
  /// TODO: group trim (collations/limit) is not pushed for grouping sets yet — the single-stage engine disables
  /// combine/reduce trim for them anyway; add it when leaf per-set trim is wired through.
  private static RelNode createGroupingSetsPlanWithLeafExchangeFinalAggregate(Aggregate aggRel) {
    int groupCount = aggRel.getGroupCount();
    /// The single-stage leaf encodes the grouping set as a 32-bit $groupingId bitmask over the union columns.
    if (groupCount > GroupingSets.MAX_GROUPING_SET_COLUMNS) {
      throw new UnsupportedOperationException(
          "GROUP BY GROUPING SETS / ROLLUP / CUBE supports at most " + GroupingSets.MAX_GROUPING_SET_COLUMNS
              + " distinct grouping columns in the multi-stage query engine, got " + groupCount);
    }
    /// GROUPING() / GROUPING_ID() are not real aggregations: they are functions of which grouping set a row belongs
    /// to. Like the single-stage post-aggregation handler, they are computed from $groupingId in the final projection,
    /// so they are split out of the LEAF/FINAL aggregate calls here.
    List<AggregateCall> orgAggCalls = aggRel.getAggCallList();
    List<AggregateCall> realAggCalls = new ArrayList<>(orgAggCalls.size());
    /// For each original aggregate call: its index among the real (non-GROUPING) aggregates, or -1 if it is a
    /// GROUPING / GROUPING_ID call (computed in the projection instead).
    int[] realAggIndex = new int[orgAggCalls.size()];
    for (int i = 0; i < orgAggCalls.size(); i++) {
      SqlKind kind = orgAggCalls.get(i).getAggregation().getKind();
      if (kind == SqlKind.GROUPING || kind == SqlKind.GROUPING_ID) {
        realAggIndex[i] = -1;
      } else {
        realAggIndex[i] = realAggCalls.size();
        realAggCalls.add(orgAggCalls.get(i));
      }
    }
    /// LEAF: carries the original groupSets; deriveRowType() appends $groupingId after the union group keys. Only the
    /// real aggregations are pushed down, so the leaf output is [union keys..., $groupingId, real aggregates...].
    PinotLogicalAggregate leafAggRel = new PinotLogicalAggregate(aggRel, aggRel.getInput(),
        buildAggCalls(aggRel, realAggCalls, AggType.LEAF, false), AggType.LEAF, false, null, 0);
    /// The leaf output adds $groupingId after the union keys, so the final group key count is groupCount + 1.
    int finalGroupCount = groupCount + 1;
    /// EXCHANGE: hash by union keys + $groupingId.
    PinotLogicalExchange exchange =
        PinotLogicalExchange.create(leafAggRel, RelDistributions.hash(ImmutableIntList.range(0, finalGroupCount)));
    /// FINAL: SIMPLE aggregate over [union keys..., $groupingId]; the real aggregate input refs are shifted by
    /// finalGroupCount and typed against the exchange (intermediate) row type, which carries the extra $groupingId.
    PinotLogicalAggregate finalAggRel =
        convertAggFromIntermediateInput(aggRel, exchange, realAggCalls, ImmutableBitSet.range(finalGroupCount),
            finalGroupCount, exchange.getRowType(), AggType.FINAL, false, null, 0);
    /// PROJECT: emit the original row type — group keys, then per original aggregate call either a real aggregate
    /// result reference or a GROUPING/GROUPING_ID value computed from $groupingId — dropping $groupingId itself.
    return buildGroupingSetsProject(aggRel, finalAggRel, groupCount, realAggIndex);
  }

  /// Builds the Project on top of the FINAL grouping-set aggregate. The FINAL output is
  /// {@code [union keys..., $groupingId, real aggregate results...]}; the project restores the original aggregate row
  /// type by emitting, in order: the group keys, then for each original aggregate call either the real aggregate
  /// result (referenced from the FINAL) or, for GROUPING() / GROUPING_ID(), the value computed from the {@code
  /// $groupingId} discriminator column (mirroring the single-stage post-aggregation handler). {@code $groupingId}
  /// itself is dropped.
  private static RelNode buildGroupingSetsProject(Aggregate aggRel, PinotLogicalAggregate finalAggRel, int groupCount,
      int[] realAggIndex) {
    RexBuilder rexBuilder = finalAggRel.getCluster().getRexBuilder();
    RelDataType intType = finalAggRel.getCluster().getTypeFactory().createSqlType(SqlTypeName.INTEGER);
    int finalGroupCount = groupCount + 1;
    /// $groupingId is the INT discriminator column immediately after the union group keys.
    RexNode groupingIdRef = rexBuilder.makeInputRef(finalAggRel, groupCount);
    List<Integer> union = aggRel.getGroupSet().asList();
    List<AggregateCall> orgAggCalls = aggRel.getAggCallList();
    List<RexNode> projects = new ArrayList<>(groupCount + orgAggCalls.size());
    for (int i = 0; i < groupCount; i++) {
      projects.add(rexBuilder.makeInputRef(finalAggRel, i));
    }
    for (int i = 0; i < orgAggCalls.size(); i++) {
      if (realAggIndex[i] >= 0) {
        projects.add(rexBuilder.makeInputRef(finalAggRel, finalGroupCount + realAggIndex[i]));
      } else {
        AggregateCall groupingCall = orgAggCalls.get(i);
        /// Map each GROUPING argument (an input column index) to its position in the union group key list, which is
        /// the bit position in $groupingId (bit set iff the column is rolled up in the row's grouping set).
        List<Integer> unionIndexes = new ArrayList<>(groupingCall.getArgList().size());
        for (int arg : groupingCall.getArgList()) {
          int unionIndex = union.indexOf(arg);
          Preconditions.checkState(unionIndex >= 0, "GROUPING argument must be a grouping column");
          unionIndexes.add(unionIndex);
        }
        RexNode value = GroupingSetsRexUtils.buildGroupingValue(rexBuilder, intType, groupingIdRef, unionIndexes);
        projects.add(rexBuilder.makeCast(groupingCall.getType(), value));
      }
    }
    return LogicalProject.create(finalAggRel, List.of(), projects, aggRel.getRowType().getFieldNames());
  }

  /**
   * The following is copied from {@link AggregateExtractProjectRule#onMatch(RelOptRuleCall)} with modification to take
   * aggregate input as input.
   */
  private static RelNode generateProjectUnderAggregate(RelOptRuleCall call, Aggregate aggregate) {
    // --------------- MODIFIED ---------------
    final RelNode input = aggregate.getInput();
    // final Aggregate aggregate = call.rel(0);
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
    final List<ImmutableBitSet> newGroupSets = aggregate.getGroupSets()
        .stream()
        .map(bitSet -> Mappings.apply(mapping, bitSet))
        .collect(Collectors.toUnmodifiableList());
    final List<RelBuilder.AggCall> newAggCallList = aggregate.getAggCallList()
        .stream()
        .map(aggCall -> relBuilder.aggregateCall(aggCall, mapping))
        .collect(Collectors.toUnmodifiableList());

    final RelBuilder.GroupKey groupKey = relBuilder.groupKey(newGroupSet, newGroupSets);
    relBuilder.aggregate(groupKey, newAggCallList);
    return relBuilder.build();
  }

  private static PinotLogicalAggregate convertAggFromIntermediateInput(Aggregate aggRel, PinotLogicalExchange exchange,
      AggType aggType, boolean leafReturnFinalResult, @Nullable List<RelFieldCollation> collations, int limit) {
    /// Plain GROUP BY: the FINAL groups by the union keys (range(groupCount)) and the intermediate aggregate columns
    /// start at groupCount; the input ref types are taken from the original aggregate row type (same field count).
    int groupCount = aggRel.getGroupCount();
    return convertAggFromIntermediateInput(aggRel, exchange, ImmutableBitSet.range(groupCount), groupCount,
        aggRel.getRowType(), aggType, leafReturnFinalResult, collations, limit);
  }

  /// As above, but with an explicit FINAL group set, the column offset where intermediate aggregate results begin,
  /// and the row type to type the aggregate input refs against. The grouping-set split passes
  /// {@code finalGroupSet = range(groupCount + 1)} and {@code aggColumnOffset = groupCount + 1} (the union keys plus
  /// the synthetic {@code $groupingId}), and types the refs against the exchange (intermediate) row type.
  private static PinotLogicalAggregate convertAggFromIntermediateInput(Aggregate aggRel, PinotLogicalExchange exchange,
      ImmutableBitSet finalGroupSet, int aggColumnOffset, RelDataType refRowType, AggType aggType,
      boolean leafReturnFinalResult, @Nullable List<RelFieldCollation> collations, int limit) {
    return convertAggFromIntermediateInput(aggRel, exchange, aggRel.getAggCallList(), finalGroupSet, aggColumnOffset,
        refRowType, aggType, leafReturnFinalResult, collations, limit);
  }

  /// As above, but over an explicit list of aggregate calls (rather than {@code aggRel.getAggCallList()}). The
  /// grouping-set split passes only the real (non-GROUPING) aggregates, which the leaf emits in this order starting
  /// at {@code aggColumnOffset}.
  private static PinotLogicalAggregate convertAggFromIntermediateInput(Aggregate aggRel, PinotLogicalExchange exchange,
      List<AggregateCall> orgAggCalls, ImmutableBitSet finalGroupSet, int aggColumnOffset, RelDataType refRowType,
      AggType aggType, boolean leafReturnFinalResult, @Nullable List<RelFieldCollation> collations, int limit) {
    RelNode input = aggRel.getInput();
    List<RexNode> projects = findImmediateProjects(input);

    // Create new AggregateCalls from exchange input. Exchange produces results with group keys followed by intermediate
    // aggregate results.
    int numAggCalls = orgAggCalls.size();
    List<AggregateCall> aggCalls = new ArrayList<>(numAggCalls);
    for (int i = 0; i < numAggCalls; i++) {
      AggregateCall orgAggCall = orgAggCalls.get(i);
      List<Integer> argList = orgAggCall.getArgList();
      int index = aggColumnOffset + i;
      RexInputRef inputRef = RexInputRef.of(index, refRowType);
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
      aggCalls.add(buildAggCall(exchange, orgAggCall, rexList, aggColumnOffset, aggType, leafReturnFinalResult));
    }

    return new PinotLogicalAggregate(aggRel, exchange, finalGroupSet, aggCalls, aggType, leafReturnFinalResult,
        collations, limit);
  }

  private static List<AggregateCall> buildAggCalls(Aggregate aggRel, AggType aggType, boolean leafReturnFinalResult) {
    return buildAggCalls(aggRel, aggRel.getAggCallList(), aggType, leafReturnFinalResult);
  }

  /// As above, but over an explicit list of aggregate calls (rather than {@code aggRel.getAggCallList()}). The
  /// grouping-set split passes only the real (non-GROUPING) aggregates to push down to the leaf.
  private static List<AggregateCall> buildAggCalls(Aggregate aggRel, List<AggregateCall> orgAggCalls, AggType aggType,
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
    } else if (relNode instanceof Union) {
      return findImmediateProjects(relNode.getInput(0));
    }
    return null;
  }

  private static boolean isGroupTrimmingEnabled(RelOptRuleCall call, Map<String, String> hintOptions,
      Aggregate aggRel) {
    if (hintOptions != null) {
      String option = hintOptions.get(PinotHintOptions.AggregateOptions.IS_ENABLE_GROUP_TRIM);
      if (option != null) {
        // Explicit hint always wins (true or false), for aggregates with AND without aggregate functions.
        return Boolean.parseBoolean(option);
      }
    }

    // Group-by WITHOUT aggregate functions (DISTINCT or `GROUP BY col` with no agg calls) can always push the
    // limit/collations down to the leaf stage by default: ORDER BY can only reference group keys, which are fully
    // materialized at the leaf, so leaf-level trim is exact (and a plain LIMIT without ORDER BY returns a valid
    // subset). This mirrors PinotLogicalAggregateRule (the physical-optimizer path).
    // TODO: Consider also enabling this by default for aggregation queries whose ORDER BY references only group keys
    //       (not aggregate results). The same argument holds there - a group's rank by its key is identical at every
    //       leaf, so keeping the per-leaf top-K never drops a group that belongs in the global top-N, and the kept
    //       groups still get their aggregates fully merged at the final stage. It is NOT safe when ORDER BY is over an
    //       aggregate (partial values rank differently per leaf) or for an unordered limit with aggregates (an
    //       arbitrarily dropped group would be under-counted).
    if (aggRel.getAggCallList().isEmpty()) {
      return true;
    }

    Context genericContext = call.getPlanner().getContext();
    if (genericContext != null) {
      QueryEnvironment.Config context = genericContext.unwrap(QueryEnvironment.Config.class);
      if (context != null) {
        return context.defaultEnableGroupTrim();
      }
    }

    return CommonConstants.Broker.DEFAULT_MSE_ENABLE_GROUP_TRIM;
  }
}
