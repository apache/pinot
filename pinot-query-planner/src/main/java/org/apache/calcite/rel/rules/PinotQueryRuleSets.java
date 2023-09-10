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

import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.Collection;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.plan.RelOptRule;


/**
 * Default rule sets for Pinot query
 */
public class PinotQueryRuleSets {
  private PinotQueryRuleSets() {
    // do not instantiate.
  }

  public static final Collection<RelOptRule> BASIC_RULES =
      Arrays.asList(EnumerableRules.ENUMERABLE_FILTER_RULE, EnumerableRules.ENUMERABLE_JOIN_RULE,
          EnumerableRules.ENUMERABLE_PROJECT_RULE, EnumerableRules.ENUMERABLE_WINDOW_RULE,
          EnumerableRules.ENUMERABLE_SORT_RULE, EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE,

          // push a filter into a join
          CoreRules.FILTER_INTO_JOIN,
          // push filter through an aggregation
          CoreRules.FILTER_AGGREGATE_TRANSPOSE,
          // push filter through set operation
          CoreRules.FILTER_SET_OP_TRANSPOSE,
          // push project through join,
          CoreRules.PROJECT_JOIN_TRANSPOSE,
          // push project through set operation
          CoreRules.PROJECT_SET_OP_TRANSPOSE,

          // aggregation and projection rules
          CoreRules.AGGREGATE_PROJECT_MERGE, CoreRules.AGGREGATE_PROJECT_PULL_UP_CONSTANTS,
          // push a projection past a filter or vice versa
          CoreRules.PROJECT_FILTER_TRANSPOSE, CoreRules.FILTER_PROJECT_TRANSPOSE,
          // push a projection to the children of a join
          // push all expressions to handle the time indicator correctly
          CoreRules.JOIN_CONDITION_PUSH,
          // merge projections
          CoreRules.PROJECT_MERGE,
          // remove identity project
          CoreRules.PROJECT_REMOVE,

          // convert OVER aggregate to logical WINDOW
          CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW,
          // push project through WINDOW
          CoreRules.PROJECT_WINDOW_TRANSPOSE,

          // TODO: Revisit and see if they can be replaced with CoreRules.PROJECT_REDUCE_EXPRESSIONS and
          //       CoreRules.FILTER_REDUCE_EXPRESSIONS
          PinotEvaluateLiteralRule.Project.INSTANCE, PinotEvaluateLiteralRule.Filter.INSTANCE,

          // TODO: evaluate the SORT_JOIN_TRANSPOSE and SORT_JOIN_COPY rules

          // join rules
          CoreRules.JOIN_PUSH_EXPRESSIONS,
          CoreRules.PROJECT_TO_SEMI_JOIN,
          PinotAggregateToSemiJoinRule.INSTANCE,

          // convert non-all union into all-union + distinct
          CoreRules.UNION_TO_DISTINCT,

          // remove aggregation if it does not aggregate and input is already distinct
          CoreRules.AGGREGATE_REMOVE,
          // push aggregate through join
          CoreRules.AGGREGATE_JOIN_TRANSPOSE,
          // aggregate union rule
          CoreRules.AGGREGATE_UNION_AGGREGATE,

          // reduce aggregate functions like AVG, STDDEV_POP etc.
          CoreRules.AGGREGATE_REDUCE_FUNCTIONS
          );

  // Filter pushdown rules run using a RuleCollection since we want to push down a filter as much as possible in a
  // single HepInstruction.
  public static final Collection<RelOptRule> FILTER_PUSHDOWN_RULES = ImmutableList.of(
      CoreRules.FILTER_INTO_JOIN,
      CoreRules.FILTER_AGGREGATE_TRANSPOSE,
      CoreRules.FILTER_SET_OP_TRANSPOSE,
      CoreRules.FILTER_PROJECT_TRANSPOSE
  );

  // The pruner rules run top-down to ensure Calcite restarts from root node after applying a transformation.
  public static final Collection<RelOptRule> PRUNE_RULES = ImmutableList.of(
      CoreRules.PROJECT_MERGE,
      CoreRules.AGGREGATE_REMOVE,
      CoreRules.SORT_REMOVE,
      PruneEmptyRules.AGGREGATE_INSTANCE, PruneEmptyRules.FILTER_INSTANCE, PruneEmptyRules.JOIN_LEFT_INSTANCE,
      PruneEmptyRules.JOIN_RIGHT_INSTANCE, PruneEmptyRules.PROJECT_INSTANCE, PruneEmptyRules.SORT_INSTANCE,
      PruneEmptyRules.UNION_INSTANCE
  );

  // Pinot specific rules that should be run BEFORE all other rules
  public static final Collection<RelOptRule> PINOT_PRE_RULES = ImmutableList.of(
      PinotAggregateLiteralAttachmentRule.INSTANCE
  );


  // Pinot specific rules that should be run AFTER all other rules
  public static final Collection<RelOptRule> PINOT_POST_RULES = ImmutableList.of(
      // Evaluate the Literal filter nodes
      CoreRules.FILTER_REDUCE_EXPRESSIONS,
      // Expand all SEARCH nodes to simplified filter nodes. SEARCH nodes get created for queries with range
      // predicates, in-clauses, etc.
      PinotFilterExpandSearchRule.INSTANCE,
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
      PinotJoinToDynamicBroadcastRule.INSTANCE
  );
}
