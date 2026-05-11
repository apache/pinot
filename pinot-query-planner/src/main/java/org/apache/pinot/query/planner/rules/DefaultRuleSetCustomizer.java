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
package org.apache.pinot.query.planner.rules;

import java.util.List;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.rules.AggregateCaseToFilterRule;
import org.apache.calcite.rel.rules.AggregateJoinTransposeRule;
import org.apache.calcite.rel.rules.AggregateProjectMergeRule;
import org.apache.calcite.rel.rules.AggregateRemoveRule;
import org.apache.calcite.rel.rules.AggregateUnionAggregateRule;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.FilterAggregateTransposeRule;
import org.apache.calcite.rel.rules.FilterMergeRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.FilterSetOpTransposeRule;
import org.apache.calcite.rel.rules.JoinPushExpressionsRule;
import org.apache.calcite.rel.rules.ProjectFilterTransposeRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rel.rules.ProjectSetOpTransposeRule;
import org.apache.calcite.rel.rules.ProjectToWindowRule;
import org.apache.calcite.rel.rules.ProjectWindowTransposeRule;
import org.apache.calcite.rel.rules.PruneEmptyRules;
import org.apache.calcite.rel.rules.SemiJoinRule;
import org.apache.calcite.rel.rules.SortJoinCopyRule;
import org.apache.calcite.rel.rules.SortJoinTransposeRule;
import org.apache.calcite.rel.rules.SortRemoveRule;
import org.apache.calcite.rel.rules.UnionToDistinctRule;
import org.apache.pinot.calcite.rel.rules.PinotAggregateExchangeNodeInsertRule;
import org.apache.pinot.calcite.rel.rules.PinotAggregateFunctionRewriteRule;
import org.apache.pinot.calcite.rel.rules.PinotAggregateReduceFunctionsRule;
import org.apache.pinot.calcite.rel.rules.PinotEnrichedJoinRule;
import org.apache.pinot.calcite.rel.rules.PinotEvaluateLiteralRule;
import org.apache.pinot.calcite.rel.rules.PinotExchangeEliminationRule;
import org.apache.pinot.calcite.rel.rules.PinotFilterJoinRule.PinotFilterIntoJoinRule;
import org.apache.pinot.calcite.rel.rules.PinotFilterJoinRule.PinotJoinConditionPushRule;
import org.apache.pinot.calcite.rel.rules.PinotJoinExchangeNodeInsertRule;
import org.apache.pinot.calcite.rel.rules.PinotJoinPushTransitivePredicatesRule;
import org.apache.pinot.calcite.rel.rules.PinotJoinToDynamicBroadcastRule;
import org.apache.pinot.calcite.rel.rules.PinotLogicalAggregateRule;
import org.apache.pinot.calcite.rel.rules.PinotProjectJoinTransposeRule;
import org.apache.pinot.calcite.rel.rules.PinotSemiJoinDistinctProjectRule;
import org.apache.pinot.calcite.rel.rules.PinotSetOpExchangeNodeInsertRule;
import org.apache.pinot.calcite.rel.rules.PinotSingleValueAggregateRemoveRule;
import org.apache.pinot.calcite.rel.rules.PinotSortExchangeCopyRule;
import org.apache.pinot.calcite.rel.rules.PinotSortExchangeNodeInsertRule;
import org.apache.pinot.calcite.rel.rules.PinotTableScanConverterRule;
import org.apache.pinot.calcite.rel.rules.PinotWindowExchangeNodeInsertRule;
import org.apache.pinot.calcite.rel.rules.PinotWindowSplitRule;
import org.apache.pinot.spi.utils.CommonConstants.Broker.PlannerRuleNames;


/// [RuleSetCustomizer] that seeds every [Phase] with the OSS default Calcite
/// rules for the multi-stage query planner. Registered as a [java.util.ServiceLoader]
/// service entry so it is picked up automatically by [PinotRuleSet].
///
/// `POST_LOGICAL` includes `PinotSortExchangeCopyRule.SORT_EXCHANGE_COPY`
/// configured with the rule's hard-coded default `fetchLimitThreshold`.
/// Per-query overrides (and broker-config overrides) of that limit are
/// applied later by `QueryEnvironment.getTraitProgram`, which swaps the
/// configured `PinotSortExchangeCopyRule` on a per-query copy of the
/// `POST_LOGICAL` list.
public final class DefaultRuleSetCustomizer implements RuleSetCustomizer {

  //@formatter:off
  public static final List<RelOptRule> BASIC_RULES = List.of(
      // push a filter into a join
      PinotFilterIntoJoinRule
          .instanceWithDescription(PlannerRuleNames.FILTER_INTO_JOIN),
      // push filter through an aggregation
      FilterAggregateTransposeRule.Config.DEFAULT
          .withDescription(PlannerRuleNames.FILTER_AGGREGATE_TRANSPOSE).toRule(),
      // push filter through set operation
      FilterSetOpTransposeRule.Config.DEFAULT
          .withDescription(PlannerRuleNames.FILTER_SET_OP_TRANSPOSE).toRule(),
      // push project through join,
      PinotProjectJoinTransposeRule
          .instanceWithDescription(PlannerRuleNames.PROJECT_JOIN_TRANSPOSE),
      // push project through set operation
      ProjectSetOpTransposeRule.Config.DEFAULT
          .withDescription(PlannerRuleNames.PROJECT_SET_OP_TRANSPOSE).toRule(),

      // push a filter past a project
      FilterProjectTransposeRule.Config.DEFAULT
          .withDescription(PlannerRuleNames.FILTER_PROJECT_TRANSPOSE).toRule(),
      // push parts of the join condition to its inputs
      PinotJoinConditionPushRule
          .instanceWithDescription(PlannerRuleNames.JOIN_CONDITION_PUSH),
      // remove identity project
      ProjectRemoveRule.Config.DEFAULT
          .withDescription(PlannerRuleNames.PROJECT_REMOVE).toRule(),

      // convert OVER aggregate to logical WINDOW
      ProjectToWindowRule.ProjectToLogicalProjectAndWindowRule.ProjectToLogicalProjectAndWindowRuleConfig.DEFAULT
          .withDescription(PlannerRuleNames.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW).toRule(),
      // push project through WINDOW
      ProjectWindowTransposeRule.Config.DEFAULT
          .withDescription(PlannerRuleNames.PROJECT_WINDOW_TRANSPOSE).toRule(),

      // literal rules
      // TODO: Revisit and see if they can be replaced with
      //     CoreRules.PROJECT_REDUCE_EXPRESSIONS and
      //     CoreRules.FILTER_REDUCE_EXPRESSIONS
      PinotEvaluateLiteralRule.Project
          .instanceWithDescription(PlannerRuleNames.EVALUATE_LITERAL_PROJECT),
      PinotEvaluateLiteralRule.Filter
          .instanceWithDescription(PlannerRuleNames.EVALUATE_LITERAL_FILTER),

      // sort join rules
      // push sort through join for left/right outer join only, disabled by default
      SortJoinTransposeRule.Config.DEFAULT
          .withDescription(PlannerRuleNames.SORT_JOIN_TRANSPOSE).toRule(),
      // copy sort below join without offset and limit, disabled by default
      SortJoinCopyRule.Config.DEFAULT
          .withDescription(PlannerRuleNames.SORT_JOIN_COPY).toRule(),

      // join rules
      JoinPushExpressionsRule.Config.DEFAULT
          .withDescription(PlannerRuleNames.JOIN_PUSH_EXPRESSIONS).toRule(),

      // join and semi-join rules
      SemiJoinRule.ProjectToSemiJoinRule.ProjectToSemiJoinRuleConfig.DEFAULT
          .withDescription(PlannerRuleNames.PROJECT_TO_SEMI_JOIN).toRule(),
      PinotSemiJoinDistinctProjectRule
          .instanceWithDescription(PlannerRuleNames.SEMI_JOIN_DISTINCT_PROJECT),

      // Consider semijoin optimizations first before push transitive predicate
      // Pinot version doesn't push predicates to the right in case of lookup join
      PinotJoinPushTransitivePredicatesRule
          .instanceWithDescription(PlannerRuleNames.JOIN_PUSH_TRANSITIVE_PREDICATES),

      // convert non-all union into all-union + distinct
      UnionToDistinctRule.Config.DEFAULT
          .withDescription(PlannerRuleNames.UNION_TO_DISTINCT).toRule(),

      // remove aggregation if it does not aggregate and input is already distinct
      AggregateRemoveRule.Config.DEFAULT
          .withDescription(PlannerRuleNames.AGGREGATE_REMOVE).toRule(),
      // push aggregate through join
      AggregateJoinTransposeRule.Config.DEFAULT
          .withDescription(PlannerRuleNames.AGGREGATE_JOIN_TRANSPOSE).toRule(),
      // push aggregate functions through join, disabled by default
      AggregateJoinTransposeRule.Config.EXTENDED
          .withDescription(PlannerRuleNames.AGGREGATE_JOIN_TRANSPOSE_EXTENDED).toRule(),
      // aggregate union rule
      AggregateUnionAggregateRule.Config.DEFAULT
          .withDescription(PlannerRuleNames.AGGREGATE_UNION_AGGREGATE).toRule(),

      // reduce SUM and AVG
      // TODO: Consider not reduce at all. This can now be controlled by specifying
      //    `plannerRule_skipAggregateReduceFunctions=true` in query option
      PinotAggregateReduceFunctionsRule
          .instanceWithDescription(PlannerRuleNames.AGGREGATE_REDUCE_FUNCTIONS),

      PinotAggregateFunctionRewriteRule
          .instanceWithDescription(PlannerRuleNames.AGGREGATE_FUNCTION_REWRITE),

      // convert CASE-style filtered aggregates into true filtered aggregates
      // put it after AGGREGATE_REDUCE_FUNCTIONS where SUM is converted to SUM0
      AggregateCaseToFilterRule.Config.DEFAULT
          .withDescription(PlannerRuleNames.AGGREGATE_CASE_TO_FILTER).toRule()
  );

  // Filter pushdown rules run using a RuleCollection since we want to push down a filter as much as possible in a
  // single HepInstruction.
  public static final List<RelOptRule> FILTER_PUSHDOWN_RULES = List.of(
      PinotFilterIntoJoinRule
          .instanceWithDescription(PlannerRuleNames.FILTER_INTO_JOIN),
      FilterAggregateTransposeRule.Config.DEFAULT
          .withDescription(PlannerRuleNames.FILTER_AGGREGATE_TRANSPOSE).toRule(),
      FilterSetOpTransposeRule.Config.DEFAULT
          .withDescription(PlannerRuleNames.FILTER_SET_OP_TRANSPOSE).toRule(),
      FilterProjectTransposeRule.Config.DEFAULT
          .withDescription(PlannerRuleNames.FILTER_PROJECT_TRANSPOSE).toRule()
  );

  // Project pushdown rules run using a RuleCollection since we want to push down a project as much as possible in a
  // single HepInstruction.
  public static final List<RelOptRule> PROJECT_PUSHDOWN_RULES = List.of(
      ProjectFilterTransposeRule.Config.DEFAULT
          .withDescription(PlannerRuleNames.PROJECT_FILTER_TRANSPOSE).toRule(),
      PinotProjectJoinTransposeRule
          .instanceWithDescription(PlannerRuleNames.PROJECT_JOIN_TRANSPOSE),
      ProjectMergeRule.Config.DEFAULT
          .withDescription(PlannerRuleNames.PROJECT_MERGE).toRule()
  );

  // The pruner rules run top-down to ensure Calcite restarts from root node after applying a transformation.
  public static final List<RelOptRule> PRUNE_RULES = List.of(
      AggregateProjectMergeRule.Config.DEFAULT
          .withDescription(PlannerRuleNames.AGGREGATE_PROJECT_MERGE).toRule(),
      ProjectMergeRule.Config.DEFAULT
          .withDescription(PlannerRuleNames.PROJECT_MERGE).toRule(),
      ProjectRemoveRule.Config.DEFAULT
          .withDescription(PlannerRuleNames.PROJECT_REMOVE).toRule(),
      FilterMergeRule.Config.DEFAULT
          .withDescription(PlannerRuleNames.FILTER_MERGE).toRule(),
      AggregateRemoveRule.Config.DEFAULT
          .withDescription(PlannerRuleNames.AGGREGATE_REMOVE).toRule(),
      SortRemoveRule.Config.DEFAULT
          .withDescription(PlannerRuleNames.SORT_REMOVE).toRule(),
      PruneEmptyRules.CorrelateLeftEmptyRuleConfig.DEFAULT
          .withDescription(PlannerRuleNames.PRUNE_EMPTY_CORRELATE_LEFT).toRule(),
      PruneEmptyRules.CorrelateRightEmptyRuleConfig.DEFAULT
          .withDescription(PlannerRuleNames.PRUNE_EMPTY_CORRELATE_RIGHT).toRule(),
      PruneEmptyRules.RemoveEmptySingleRule.RemoveEmptySingleRuleConfig.AGGREGATE
          .withDescription(PlannerRuleNames.PRUNE_EMPTY_AGGREGATE).toRule(),
      PruneEmptyRules.RemoveEmptySingleRule.RemoveEmptySingleRuleConfig.FILTER
          .withDescription(PlannerRuleNames.PRUNE_EMPTY_FILTER).toRule(),
      PruneEmptyRules.JoinLeftEmptyRuleConfig.DEFAULT.
          withDescription(PlannerRuleNames.PRUNE_EMPTY_JOIN_LEFT).toRule(),
      PruneEmptyRules.JoinRightEmptyRuleConfig.DEFAULT
          .withDescription(PlannerRuleNames.PRUNE_EMPTY_JOIN_RIGHT).toRule(),
      PruneEmptyRules.RemoveEmptySingleRule.RemoveEmptySingleRuleConfig.PROJECT
          .withDescription(PlannerRuleNames.PRUNE_EMPTY_PROJECT).toRule(),
      PruneEmptyRules.RemoveEmptySingleRule.RemoveEmptySingleRuleConfig.SORT
          .withDescription(PlannerRuleNames.PRUNE_EMPTY_SORT).toRule(),
      PruneEmptyRules.UnionEmptyPruneRuleConfig.DEFAULT
          .withDescription(PlannerRuleNames.PRUNE_EMPTY_UNION).toRule()
  );

  /// Pinot specific rules that should be run AFTER all other rules.
  /// Includes [PinotSortExchangeCopyRule#SORT_EXCHANGE_COPY] (configured with
  /// the rule's hard-coded default fetch limit). `QueryEnvironment` swaps the
  /// rule on a per-query copy of this list when the per-query
  /// `sortExchangeCopyLimit` differs from that default.
  public static final List<RelOptRule> POST_LOGICAL_RULES = List.of(
      // TODO: Merge the following 2 rules into a single rule
      // add an extra exchange for sort
      PinotSortExchangeNodeInsertRule.INSTANCE,
      PinotSortExchangeCopyRule.SORT_EXCHANGE_COPY,

      PinotSingleValueAggregateRemoveRule.INSTANCE,
      PinotJoinExchangeNodeInsertRule.INSTANCE,
      PinotAggregateExchangeNodeInsertRule.SortProjectAggregate.INSTANCE,
      PinotAggregateExchangeNodeInsertRule.SortAggregate.INSTANCE,
      PinotAggregateExchangeNodeInsertRule.WithoutSort.INSTANCE,
      PinotWindowSplitRule.INSTANCE,
      PinotWindowExchangeNodeInsertRule.INSTANCE,
      PinotSetOpExchangeNodeInsertRule.INSTANCE,

      // apply dynamic broadcast rule after exchange is inserted
      PinotJoinToDynamicBroadcastRule.INSTANCE,

      // remove exchanges when there's duplicates
      PinotExchangeEliminationRule.INSTANCE,

      // Evaluate the Literal filter nodes
      CoreRules.FILTER_REDUCE_EXPRESSIONS,
      PinotTableScanConverterRule.INSTANCE
  );

  public static final List<RelOptRule> POST_LOGICAL_PHYSICAL_RULES = List.of(
      PinotTableScanConverterRule.INSTANCE,
      PinotLogicalAggregateRule.SortProjectAggregate.INSTANCE,
      PinotLogicalAggregateRule.SortAggregate.INSTANCE,
      PinotLogicalAggregateRule.PinotLogicalAggregateConverter.INSTANCE,
      PinotWindowSplitRule.INSTANCE,
      // Evaluate the Literal filter nodes
      CoreRules.FILTER_REDUCE_EXPRESSIONS
  );
  //@formatter:on

  /// No-arg constructor required by [java.util.ServiceLoader].
  public DefaultRuleSetCustomizer() {
  }

  @Override
  public void customize(Phase phase, List<RelOptRule> rules) {
    switch (phase) {
      case BASIC:
        rules.addAll(BASIC_RULES);
        return;
      case FILTER_PUSHDOWN:
        rules.addAll(FILTER_PUSHDOWN_RULES);
        return;
      case PROJECT_PUSHDOWN:
        rules.addAll(PROJECT_PUSHDOWN_RULES);
        return;
      case PRUNE:
        rules.addAll(PRUNE_RULES);
        return;
      case POST_LOGICAL:
        rules.addAll(POST_LOGICAL_RULES);
        return;
      case POST_LOGICAL_PHYSICAL:
        rules.addAll(POST_LOGICAL_PHYSICAL_RULES);
        return;
      case POST_LOGICAL_ENRICHED_JOIN:
        rules.addAll(PinotEnrichedJoinRule.PINOT_ENRICHED_JOIN_RULES);
        return;
      default:
        throw new IllegalStateException(
            "DefaultRuleSetCustomizer is missing OSS rule defaults for Phase." + phase
                + "; extend the switch when adding a new Phase value.");
    }
  }
}
