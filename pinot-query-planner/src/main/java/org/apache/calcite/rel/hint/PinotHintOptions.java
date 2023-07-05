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

  /**
   * Hint to denote that the aggregation node is the final aggregation stage which extracts the final result.
   */
  public static final String INTERNAL_AGG_OPTIONS = "aggOptionsInternal";

  private PinotHintOptions() {
    // do not instantiate.
  }

  public static class InternalAggregateOptions {
    public static final String AGG_TYPE = "agg_type";
    public enum AggType {
      DIRECT,
      LEAF,
      INTERMEDIATE,
      FINAL,
      REDUCE // NOT SUPPORTED
    }
  }

  public static class AggregateOptions {
    public static final String IS_PARTITIONED_BY_GROUP_BY_KEYS = "is_partitioned_by_group_by_keys";
    public static final String SKIP_LEAF_STAGE_GROUP_BY_AGGREGATION = "is_skip_leaf_stage_group_by";
  }

  public static class JoinHintOptions {
    public static final String JOIN_STRATEGY = "join_strategy";
    public static final String IS_COLOCATED_BY_JOIN_KEYS = "is_colocated_by_join_keys";
  }
}
