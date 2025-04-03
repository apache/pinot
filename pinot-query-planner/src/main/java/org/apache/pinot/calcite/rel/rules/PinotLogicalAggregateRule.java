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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.pinot.calcite.rel.hint.PinotHintOptions;
import org.apache.pinot.calcite.rel.hint.PinotHintStrategyTable;
import org.apache.pinot.calcite.rel.logical.PinotLogicalAggregate;
import org.apache.pinot.query.QueryEnvironment;
import org.apache.pinot.query.planner.plannode.AggregateNode.AggType;
import org.apache.pinot.spi.utils.CommonConstants;


/**
 * Same as {@link PinotAggregateExchangeNodeInsertRule}, with the following differences:
 * <ol>
 *   <li>We don't generate project under the aggregate.</li>
 *   <li>We don't generate exchange and merely generate a PinotLogicalAggregate.</li>
 *   <li>We don't convert Agg Calls.</li>
 * </ol>
 * All of these will be done in the Physical Planning phase instead, since that is when we will know whether the
 * aggregate has been split or not. (e.g. project under aggregate is required when you skip partial aggregate).
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
      if (!isGroupTrimmingEnabled(call, hintOptions)) {
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
      PinotLogicalAggregate newAggRel = createPlan(aggRel, newCollations, limit);
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
      if (!isGroupTrimmingEnabled(call, hintOptions)) {
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

      PinotLogicalAggregate newAggRel = createPlan(aggRel, collations, limit);
      call.transformTo(sortRel.copy(sortRel.getTraitSet(), List.of(newAggRel)));
    }
  }

  /**
   * Convert any remaining LogicalAggregate to PinotLogicalAggregate. Some nodes may already be converted as part of
   * the aggregate group-trim rules above.
   */
  public static class PinotLogicalAggregateConverter extends RelOptRule {
    public static final PinotLogicalAggregateConverter INSTANCE = new PinotLogicalAggregateConverter(
        PinotRuleUtils.PINOT_REL_FACTORY);

    private PinotLogicalAggregateConverter(RelBuilderFactory factory) {
      super(operand(LogicalAggregate.class, any()), factory, null);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      Aggregate aggRel = call.rel(0);
      call.transformTo(createWithNoGroupTrim(aggRel));
    }
  }

  private static PinotLogicalAggregate createWithNoGroupTrim(Aggregate aggRel) {
    return createPlan(aggRel, null, 0);
  }

  private static PinotLogicalAggregate createPlan(Aggregate aggRel, @Nullable List<RelFieldCollation> collations,
      int limit) {
    Map<String, String> hintOptions =
        PinotHintStrategyTable.getHintOptions(aggRel.getHints(), PinotHintOptions.AGGREGATE_HINT_OPTIONS);
    if (hintOptions == null) {
      hintOptions = Map.of();
    }
    boolean leafReturnFinalResult =
        Boolean.parseBoolean(hintOptions.get(PinotHintOptions.AggregateOptions.IS_LEAF_RETURN_FINAL_RESULT));
    RelNode input = aggRel.getInput();
    // TODO(mse-physical): Remove AggType from logical aggregate. For now use DIRECT.
    return new PinotLogicalAggregate(aggRel, input, aggRel.getAggCallList(), AggType.DIRECT,
        leafReturnFinalResult, collations, limit);
  }

  private static boolean isGroupTrimmingEnabled(RelOptRuleCall call, @Nullable Map<String, String> hintOptions) {
    if (hintOptions != null) {
      String option = hintOptions.get(PinotHintOptions.AggregateOptions.IS_ENABLE_GROUP_TRIM);
      if (option != null) {
        return Boolean.parseBoolean(option);
      }
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
