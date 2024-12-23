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

import java.util.List;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.rules.AggregateCaseToFilterRule;
import org.apache.calcite.rel.rules.AggregateJoinTransposeRule;
import org.apache.calcite.rel.rules.AggregateProjectMergeRule;
import org.apache.calcite.rel.rules.AggregateRemoveRule;
import org.apache.calcite.rel.rules.AggregateUnionAggregateRule;
import org.apache.calcite.rel.rules.FilterAggregateTransposeRule;
import org.apache.calcite.rel.rules.FilterJoinRule;
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
import org.apache.calcite.rel.rules.ReduceExpressionsRule;
import org.apache.calcite.rel.rules.SemiJoinRule;
import org.apache.calcite.rel.rules.SortRemoveRule;
import org.apache.calcite.rel.rules.UnionToDistinctRule;
import org.apache.pinot.calcite.rel.logical.PinotLogicalJoin;
import org.apache.pinot.calcite.rel.rules.PinotFilterJoinRule.PinotFilterIntoJoinRule;
import org.apache.pinot.calcite.rel.rules.PinotFilterJoinRule.PinotJoinConditionPushRule;


/**
 * Default rule sets for Pinot query
 */
public class PinotQueryRuleSets {
  private PinotQueryRuleSets() {
  }

  //@formatter:off
  public static final List<RelOptRule> BASIC_RULES = List.of(
      // push a filter into a join
      PinotFilterIntoJoinRule.INSTANCE,
      // push filter through an aggregation
      fromConfig(FilterAggregateTransposeRule.Config.DEFAULT),
      // push filter through set operation
      fromConfig(FilterSetOpTransposeRule.Config.DEFAULT),
      // push project through join,
      PinotProjectJoinTransposeRule.INSTANCE,
      // push project through set operation
      fromConfig(ProjectSetOpTransposeRule.Config.DEFAULT),

      // push a filter past a project
      fromConfig(FilterProjectTransposeRule.Config.DEFAULT),
      // push parts of the join condition to its inputs
      PinotJoinConditionPushRule.INSTANCE,
      // remove identity project
      fromConfig(ProjectRemoveRule.Config.DEFAULT),

      // convert OVER aggregate to logical WINDOW
      fromConfig(
          ProjectToWindowRule.ProjectToLogicalProjectAndWindowRule.ProjectToLogicalProjectAndWindowRuleConfig.DEFAULT),
      // push project through WINDOW
      fromConfig(ProjectWindowTransposeRule.Config.DEFAULT),

      // literal rules
      // TODO: Revisit and see if they can be replaced with
      //     CoreRules.PROJECT_REDUCE_EXPRESSIONS and
      //     CoreRules.FILTER_REDUCE_EXPRESSIONS
      PinotEvaluateLiteralRule.Project.INSTANCE,
      PinotEvaluateLiteralRule.Filter.INSTANCE,

      // sort join rules
      // TODO: evaluate the SORT_JOIN_TRANSPOSE and SORT_JOIN_COPY rules

      // join rules
      fromConfig(JoinPushExpressionsRule.Config.DEFAULT),

      // join and semi-join rules
      fromConfig(SemiJoinRule.ProjectToSemiJoinRule.ProjectToSemiJoinRuleConfig.DEFAULT),
      PinotAggregateToSemiJoinRule.INSTANCE,

      // convert non-all union into all-union + distinct
      fromConfig(UnionToDistinctRule.Config.DEFAULT),

      // remove aggregation if it does not aggregate and input is already distinct
      fromConfig(AggregateRemoveRule.Config.DEFAULT),
      // push aggregate through join
      fromConfig(
          AggregateJoinTransposeRule.Config.DEFAULT.withOperandFor(LogicalAggregate.class, PinotLogicalJoin.class,
              false)),
      // aggregate union rule
      fromConfig(AggregateUnionAggregateRule.Config.DEFAULT),

      // reduce SUM and AVG
      // TODO: Consider not reduce at all.
      PinotAggregateReduceFunctionsRule.INSTANCE,

      // convert CASE-style filtered aggregates into true filtered aggregates
      // put it after AGGREGATE_REDUCE_FUNCTIONS where SUM is converted to SUM0
      fromConfig(AggregateCaseToFilterRule.Config.DEFAULT)
  );

  // Filter pushdown rules run using a RuleCollection since we want to push down a filter as much as possible in a
  // single HepInstruction.
  public static final List<RelOptRule> FILTER_PUSHDOWN_RULES = List.of(
      fromConfig(FilterJoinRule.FilterIntoJoinRule.FilterIntoJoinRuleConfig.DEFAULT),
      fromConfig(FilterAggregateTransposeRule.Config.DEFAULT),
      fromConfig(FilterSetOpTransposeRule.Config.DEFAULT),
      fromConfig(FilterProjectTransposeRule.Config.DEFAULT)
  );

  // Project pushdown rules run using a RuleCollection since we want to push down a project as much as possible in a
  // single HepInstruction.
  public static final List<RelOptRule> PROJECT_PUSHDOWN_RULES = List.of(
      fromConfig(ProjectFilterTransposeRule.Config.DEFAULT),
      PinotProjectJoinTransposeRule.INSTANCE,
      fromConfig(ProjectMergeRule.Config.DEFAULT)
  );

  // The pruner rules run top-down to ensure Calcite restarts from root node after applying a transformation.
  public static final List<RelOptRule> PRUNE_RULES = List.of(
      fromConfig(AggregateProjectMergeRule.Config.DEFAULT),
      fromConfig(ProjectMergeRule.Config.DEFAULT),
      fromConfig(FilterMergeRule.Config.DEFAULT),
      fromConfig(AggregateRemoveRule.Config.DEFAULT),
      fromConfig(SortRemoveRule.Config.DEFAULT),
      fromConfig(PruneEmptyRules.RemoveEmptySingleRule.RemoveEmptySingleRuleConfig.AGGREGATE),
      fromConfig(PruneEmptyRules.RemoveEmptySingleRule.RemoveEmptySingleRuleConfig.FILTER),
      fromConfig(PruneEmptyRules.JoinLeftEmptyRuleConfig.DEFAULT),
      fromConfig(PruneEmptyRules.JoinRightEmptyRuleConfig.DEFAULT),
      fromConfig(PruneEmptyRules.RemoveEmptySingleRule.RemoveEmptySingleRuleConfig.PROJECT),
      fromConfig(PruneEmptyRules.RemoveEmptySingleRule.RemoveEmptySingleRuleConfig.SORT),
      fromConfig(PruneEmptyRules.UnionEmptyPruneRuleConfig.DEFAULT)
  );

  // Pinot specific rules that should be run AFTER all other rules
  public static final List<RelOptRule> PINOT_POST_RULES = List.of(
      // TODO: Merge the following 2 rules into a single rule
      // add an extra exchange for sort
      PinotSortExchangeNodeInsertRule.INSTANCE,
      // copy exchanges down, this must be done after SortExchangeNodeInsertRule
      PinotSortExchangeCopyRule.SORT_EXCHANGE_COPY,

      PinotSingleValueAggregateRemoveRule.INSTANCE,
      PinotJoinExchangeNodeInsertRule.INSTANCE,
      PinotAggregateExchangeNodeInsertRule.INSTANCE,
      PinotWindowExchangeNodeInsertRule.INSTANCE,
      PinotSetOpExchangeNodeInsertRule.INSTANCE,

      // apply dynamic broadcast rule after exchange is inserted/
      PinotJoinToDynamicBroadcastRule.INSTANCE,

      // remove exchanges when there's duplicates
      PinotExchangeEliminationRule.INSTANCE,

      // Expand all SEARCH nodes to simplified filter nodes. SEARCH nodes get created for queries with range predicates,
      // in-clauses, etc.
      // NOTE: Keep this rule at the end because it can potentially create a lot of predicates joined by OR/AND for IN/
      //       NOT IN clause, which can be expensive to process in other rules.
      // TODO: Consider removing this rule and directly handle SEARCH in RexExpressionUtils.
      PinotFilterExpandSearchRule.INSTANCE,
      // Evaluate the Literal filter nodes
      fromConfig(ReduceExpressionsRule.FilterReduceExpressionsRule.FilterReduceExpressionsRuleConfig.DEFAULT)
  );
  //@formatter:on

  private static RelOptRule fromConfig(RelRule.Config config) {
    return config.withRelBuilderFactory(PinotRuleUtils.PINOT_REL_FACTORY).toRule();
  }
}
