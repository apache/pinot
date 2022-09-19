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

  public static final Collection<RelOptRule> LOGICAL_OPT_RULES =
      Arrays.asList(EnumerableRules.ENUMERABLE_FILTER_RULE, EnumerableRules.ENUMERABLE_JOIN_RULE,
          EnumerableRules.ENUMERABLE_PROJECT_RULE, EnumerableRules.ENUMERABLE_SORT_RULE,
          EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE,

          // push a filter into a join, replaced CoreRules.FILTER_INTO_JOIN with special config
          PinotFilterIntoJoinRule.INSTANCE,
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
          // reorder sort and projection
          CoreRules.SORT_PROJECT_TRANSPOSE,

          // join rules
          CoreRules.JOIN_PUSH_EXPRESSIONS,

          // convert non-all union into all-union + distinct
          CoreRules.UNION_TO_DISTINCT,

          // remove aggregation if it does not aggregate and input is already distinct
          CoreRules.AGGREGATE_REMOVE,
          // push aggregate through join
          CoreRules.AGGREGATE_JOIN_TRANSPOSE,
          // aggregate union rule
          CoreRules.AGGREGATE_UNION_AGGREGATE,

          // reduce aggregate functions like AVG, STDDEV_POP etc.
          CoreRules.AGGREGATE_REDUCE_FUNCTIONS,

          // remove unnecessary sort rule
          CoreRules.SORT_REMOVE,

          // projection to SEMI JOIN.
          CoreRules.PROJECT_TO_SEMI_JOIN,

          // prune empty results rules
          PruneEmptyRules.AGGREGATE_INSTANCE, PruneEmptyRules.FILTER_INSTANCE, PruneEmptyRules.JOIN_LEFT_INSTANCE,
          PruneEmptyRules.JOIN_RIGHT_INSTANCE, PruneEmptyRules.PROJECT_INSTANCE, PruneEmptyRules.SORT_INSTANCE,
          PruneEmptyRules.UNION_INSTANCE,

          // Pinot specific rules
          PinotJoinExchangeNodeInsertRule.INSTANCE,
          PinotAggregateExchangeNodeInsertRule.INSTANCE,
          PinotSortExchangeNodeInsertRule.INSTANCE
      );
}
