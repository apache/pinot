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
package org.apache.pinot.calcite.rel.hint;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.hint.RelHint;


/**
 * {@code PinotHintOptions} specified the supported hint options by Pinot based a particular type of relation node.
 *
 * <p>for each {@link RelNode} type we support an option hint name.</p>
 * <p>for each option hint name there's a corresponding {@link RelHint} that supported only key-value option stored
 * in {@link RelHint#kvOptions}</p>
 */
public class PinotHintOptions {
  private PinotHintOptions() {
  }

  public static final String AGGREGATE_HINT_OPTIONS = "aggOptions";
  public static final String JOIN_HINT_OPTIONS = "joinOptions";
  public static final String TABLE_HINT_OPTIONS = "tableOptions";
  public static final String WINDOW_HINT_OPTIONS = "windowOptions";

  public static class AggregateOptions {
    public static final String IS_PARTITIONED_BY_GROUP_BY_KEYS = "is_partitioned_by_group_by_keys";
    public static final String SKIP_LEAF_STAGE_GROUP_BY_AGGREGATION = "is_skip_leaf_stage_group_by";

    public static final String NUM_GROUPS_LIMIT = "num_groups_limit";
    public static final String MAX_INITIAL_RESULT_HOLDER_CAPACITY = "max_initial_result_holder_capacity";
  }

  public static class WindowHintOptions {
    /**
     * Max rows allowed to cache the rows in window for further processing.
     */
    public static final String MAX_ROWS_IN_WINDOW = "max_rows_in_window";
    /**
     * Mode when window overflow happens, supported values: THROW or BREAK.
     *   THROW(default): Break window cache build process, and throw exception, no further WINDOW operation performed.
     *   BREAK: Break window cache build process, continue to perform WINDOW operation, results might be partial.
     */
    public static final String WINDOW_OVERFLOW_MODE = "window_overflow_mode";
  }

  public static class JoinHintOptions {
    public static final String JOIN_STRATEGY = "join_strategy";
    // "hash" is the default strategy for non-SEMI joins
    public static final String HASH_JOIN_STRATEGY = "hash";
    // "dynamic_broadcast" is the default strategy for SEMI joins
    public static final String DYNAMIC_BROADCAST_JOIN_STRATEGY = "dynamic_broadcast";
    // "lookup" can be used when the right table is a dimension table replicated to all workers
    public static final String LOOKUP_JOIN_STRATEGY = "lookup";

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

    /**
     * Indicates that the join operator(s) within a certain selection scope are colocated
     */
    public static final String IS_COLOCATED_BY_JOIN_KEYS = "is_colocated_by_join_keys";

    // TODO: Consider adding a Join implementation with join strategy.
    public static boolean useLookupJoinStrategy(Join join) {
      return LOOKUP_JOIN_STRATEGY.equalsIgnoreCase(
          PinotHintStrategyTable.getHintOption(join.getHints(), PinotHintOptions.JOIN_HINT_OPTIONS,
              PinotHintOptions.JoinHintOptions.JOIN_STRATEGY));
    }
  }

  public static class TableHintOptions {
    /**
     * Indicates how many partitions the table must be partitioned by.
     * This must be equal to the partition count of the table in
     * {@code tableIndexConfig.segmentPartitionConfig.columnPartitionMap}.
     */
    public static final String PARTITION_KEY = "partition_key";
    /**
     * The function to use to partition the table.
     * This must be equal to {@code functionName} in {@code tableIndexConfig.segmentPartitionConfig.columnPartitionMap}.
     */
    public static final String PARTITION_FUNCTION = "partition_function";
    /**
     * The size of each partition.
     * This must be equal to {@code numPartition} in {@code tableIndexConfig.segmentPartitionConfig.columnPartitionMap}.
     */
    public static final String PARTITION_SIZE = "partition_size";
    /**
     * The number of workers per partition.
     *
     * How many threads to use in the following stage after partition is joined.
     * When partition info is set, each partition is processed as a separate query in the leaf stage.
     */
    public static final String PARTITION_PARALLELISM = "partition_parallelism";
  }
}
