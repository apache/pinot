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
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.PruneEmptyRules;
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
      CoreRules.FILTER_AGGREGATE_TRANSPOSE,
      // push filter through set operation
      CoreRules.FILTER_SET_OP_TRANSPOSE,
      // push project through join,
      PinotProjectJoinTransposeRule.INSTANCE,
      // push project through set operation
      CoreRules.PROJECT_SET_OP_TRANSPOSE,

      // push a filter past a project
      CoreRules.FILTER_PROJECT_TRANSPOSE,
      // push parts of the join condition to its inputs
      PinotJoinConditionPushRule.INSTANCE,
      // remove identity project
      CoreRules.PROJECT_REMOVE,

      // convert OVER aggregate to logical WINDOW
      CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW,
      // push project through WINDOW
      CoreRules.PROJECT_WINDOW_TRANSPOSE,

      // literal rules
      // TODO: Revisit and see if they can be replaced with
      //     CoreRules.PROJECT_REDUCE_EXPRESSIONS and
      //     CoreRules.FILTER_REDUCE_EXPRESSIONS
      PinotEvaluateLiteralRule.Project.INSTANCE,
      PinotEvaluateLiteralRule.Filter.INSTANCE,

      // sort join rules
      // TODO: evaluate the SORT_JOIN_TRANSPOSE and SORT_JOIN_COPY rules

      // join rules
      CoreRules.JOIN_PUSH_EXPRESSIONS,

      // join and semi-join rules
      CoreRules.PROJECT_TO_SEMI_JOIN,
      PinotSeminJoinDistinctProjectRule.INSTANCE,

      // convert non-all union into all-union + distinct
      CoreRules.UNION_TO_DISTINCT,

      // remove aggregation if it does not aggregate and input is already distinct
      CoreRules.AGGREGATE_REMOVE,
      // push aggregate through join
      CoreRules.AGGREGATE_JOIN_TRANSPOSE,
      // aggregate union rule
      CoreRules.AGGREGATE_UNION_AGGREGATE,

      // reduce SUM and AVG
      // TODO: Consider not reduce at all.
      PinotAggregateReduceFunctionsRule.INSTANCE,

      // convert CASE-style filtered aggregates into true filtered aggregates
      // put it after AGGREGATE_REDUCE_FUNCTIONS where SUM is converted to SUM0
      CoreRules.AGGREGATE_CASE_TO_FILTER
  );

  // Filter pushdown rules run using a RuleCollection since we want to push down a filter as much as possible in a
  // single HepInstruction.
  public static final List<RelOptRule> FILTER_PUSHDOWN_RULES = List.of(
      CoreRules.FILTER_INTO_JOIN,
      CoreRules.FILTER_AGGREGATE_TRANSPOSE,
      CoreRules.FILTER_SET_OP_TRANSPOSE,
      CoreRules.FILTER_PROJECT_TRANSPOSE
  );

  // Project pushdown rules run using a RuleCollection since we want to push down a project as much as possible in a
  // single HepInstruction.
  public static final List<RelOptRule> PROJECT_PUSHDOWN_RULES = List.of(
      CoreRules.PROJECT_FILTER_TRANSPOSE,
      PinotProjectJoinTransposeRule.INSTANCE,
      CoreRules.PROJECT_MERGE
  );

  // The pruner rules run top-down to ensure Calcite restarts from root node after applying a transformation.
  public static final List<RelOptRule> PRUNE_RULES = List.of(
      CoreRules.AGGREGATE_PROJECT_MERGE,
      CoreRules.PROJECT_MERGE,
      CoreRules.FILTER_MERGE,
      CoreRules.AGGREGATE_REMOVE,
      CoreRules.SORT_REMOVE,
      PruneEmptyRules.AGGREGATE_INSTANCE,
      PruneEmptyRules.FILTER_INSTANCE,
      PruneEmptyRules.JOIN_LEFT_INSTANCE,
      PruneEmptyRules.JOIN_RIGHT_INSTANCE,
      PruneEmptyRules.PROJECT_INSTANCE,
      PruneEmptyRules.SORT_INSTANCE,
      PruneEmptyRules.UNION_INSTANCE
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
      PinotAggregateExchangeNodeInsertRule.SortProjectAggregate.INSTANCE,
      PinotAggregateExchangeNodeInsertRule.SortAggregate.INSTANCE,
      PinotAggregateExchangeNodeInsertRule.WithoutSort.INSTANCE,
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
      CoreRules.FILTER_REDUCE_EXPRESSIONS
  );
  //@formatter:on
}
