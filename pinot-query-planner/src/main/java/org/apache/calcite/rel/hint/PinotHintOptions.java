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
package org.apache.calcite.rel.hint;

import org.apache.pinot.query.planner.logical.LiteralHintUtils;


/**
 * {@code PinotHintOptions} specified the supported hint options by Pinot based a particular type of relation node.
 *
 * <p>for each {@link org.apache.calcite.rel.RelNode} type we support an option hint name.</p>
 * <p>for each option hint name there's a corresponding {@link RelHint} that supported only key-value option stored
 * in {@link RelHint#kvOptions}</p>
 */
public class PinotHintOptions {
  public static final String AGGREGATE_HINT_OPTIONS = "aggOptions";
  public static final String JOIN_HINT_OPTIONS = "joinOptions";
  public static final String TABLE_HINT_OPTIONS = "tableOptions";

  /**
   * Hint to denote that the aggregation node is the final aggregation stage which extracts the final result.
   */
  public static final String INTERNAL_AGG_OPTIONS = "aggOptionsInternal";

  private PinotHintOptions() {
    // do not instantiate.
  }

  public static class InternalAggregateOptions {
    public static final String AGG_TYPE = "agg_type";
    /**
     * agg call signature is used to store LITERAL inputs to the Aggregate Call. which is not supported in Calcite
     * here
     * 1. we store the Map of Pair[aggCallIdx, argListIdx] to RexLiteral to indicate the RexLiteral being passed into
     *     the aggregateCalls[aggCallIdx].operandList[argListIdx] is supposed to be a RexLiteral.
     * 2. not all RexLiteral types are supported to be part of the input constant call signature.
     * 3. RexLiteral are encoded as String and decoded as Pinot Literal objects.
     *
     * see: {@link LiteralHintUtils}.
     * see: https://issues.apache.org/jira/projects/CALCITE/issues/CALCITE-5833
     */
    public static final String AGG_CALL_SIGNATURE = "agg_call_signature";
  }

  public static class AggregateOptions {
    public static final String IS_PARTITIONED_BY_GROUP_BY_KEYS = "is_partitioned_by_group_by_keys";
    public static final String SKIP_LEAF_STAGE_GROUP_BY_AGGREGATION = "is_skip_leaf_stage_group_by";

    public static final String NUM_GROUPS_LIMIT = "num_groups_limit";
    public static final String MAX_INITIAL_RESULT_HOLDER_CAPACITY = "max_initial_result_holder_capacity";
  }

  public static class JoinHintOptions {
    public static final String JOIN_STRATEGY = "join_strategy";
    public static final String DYNAMIC_BROADCAST_JOIN_STRATEGY = "dynamic_broadcast";
    public static final String HASH_TABLE_JOIN_STRATEGY = "hash_table";
    /**
     * Max rows allowed to build the right table hash collection.
     */
    public static final String MAX_ROWS_IN_JOIN = "max_rows_in_join";
    /**
     * Mode when join overflow happens, supported values: THROW or BREAK.
     *   THROW(default): Break right table build process, and throw exception, no JOIN with left table performed.
     *   BREAK: Break right table build process, continue to perform JOIN operation, results might be partial.
     */
    public static final String JOIN_OVERFLOW_MODE = "join_overflow_mode";
  }

  public static class TableHintOptions {
    public static final String PARTITION_KEY = "partition_key";
    public static final String PARTITION_SIZE = "partition_size";
    public static final String PARTITION_PARALLELISM = "partition_parallelism";
  }
}
