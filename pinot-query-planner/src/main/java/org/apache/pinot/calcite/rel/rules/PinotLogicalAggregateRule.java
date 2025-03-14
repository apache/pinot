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
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.rules.AggregateExtractProjectRule;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.pinot.calcite.rel.hint.PinotHintOptions;
import org.apache.pinot.calcite.rel.hint.PinotHintStrategyTable;
import org.apache.pinot.calcite.rel.logical.PinotLogicalAggregate;
import org.apache.pinot.query.planner.plannode.AggregateNode.AggType;


/**
 * Does the following:
 * 1. Creates PinotLogicalAggregate from Aggregate.
 * 2. Propagates group trim properties from sort or sort-project.
 * 3. Inserts project under aggregate if one doesn't exist already.
 */
public class PinotLogicalAggregateRule {
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
      if (hintOptions == null || !Boolean.parseBoolean(
          hintOptions.get(PinotHintOptions.AggregateOptions.IS_ENABLE_GROUP_TRIM))) {
        return;
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

      PinotLogicalAggregate newAggRel = createPlan(call, aggRel, true, hintOptions, newCollations, limit);
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
      if (hintOptions == null || !Boolean.parseBoolean(
          hintOptions.get(PinotHintOptions.AggregateOptions.IS_ENABLE_GROUP_TRIM))) {
        return;
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

      PinotLogicalAggregate newAggRel = createPlan(call, aggRel, true, hintOptions, collations, limit);
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

  private static PinotLogicalAggregate createPlan(RelOptRuleCall call, Aggregate aggRel, boolean hasGroupBy,
      Map<String, String> hintOptions, @Nullable List<RelFieldCollation> collations, int limit) {
    boolean leafReturnFinalResult =
        Boolean.parseBoolean(hintOptions.get(PinotHintOptions.AggregateOptions.IS_LEAF_RETURN_FINAL_RESULT));
    RelNode input = aggRel.getInput();
    // Create Project when there's none below the aggregate.
    if (!(PinotRuleUtils.unboxRel(input) instanceof Project)) {
      aggRel = (Aggregate) generateProjectUnderAggregate(call, aggRel);
      input = aggRel.getInput();
    }
    return new PinotLogicalAggregate(aggRel, input, aggRel.getAggCallList(), AggType.DIRECT,
        leafReturnFinalResult, collations, limit);
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
        .collect(ImmutableList.toImmutableList());
    final List<RelBuilder.AggCall> newAggCallList = aggregate.getAggCallList()
        .stream()
        .map(aggCall -> relBuilder.aggregateCall(aggCall, mapping))
        .collect(ImmutableList.toImmutableList());

    final RelBuilder.GroupKey groupKey = relBuilder.groupKey(newGroupSet, newGroupSets);
    relBuilder.aggregate(groupKey, newAggCallList);
    return relBuilder.build();
  }
}
