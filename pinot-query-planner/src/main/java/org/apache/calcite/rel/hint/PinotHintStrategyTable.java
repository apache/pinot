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

import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.spi.utils.BooleanUtils;


/**
 * Default hint strategy set for Pinot query.
 */
public class PinotHintStrategyTable {
  public static final String INTERNAL_AGG_FINAL_STAGE = "aggFinalStage";
  public static final String INTERNAL_AGG_INTERMEDIATE_STAGE = "aggIntermediateStage";
  public static final String INTERNAL_AGG_LEAF_STAGE = "aggLeafStage";
  // Hint to denote that aggregation is optimized to skip the leaf level
  public static final String INTERNAL_AGG_IS_LEAF_STAGE_SKIPPED = "isAggLeafStageSkipped";

  public static final String SKIP_LEAF_STAGE_GROUP_BY_AGGREGATION = "skipLeafStageGroupByAggregation";


  private PinotHintStrategyTable() {
    // do not instantiate.
  }

  public static final HintStrategyTable PINOT_HINT_STRATEGY_TABLE = HintStrategyTable.builder()
      .hintStrategy(INTERNAL_AGG_FINAL_STAGE, HintPredicates.AGGREGATE)
      .hintStrategy(INTERNAL_AGG_INTERMEDIATE_STAGE, HintPredicates.AGGREGATE)
      .hintStrategy(INTERNAL_AGG_LEAF_STAGE, HintPredicates.AGGREGATE)
      .hintStrategy(INTERNAL_AGG_IS_LEAF_STAGE_SKIPPED, HintPredicates.AGGREGATE)
      .hintStrategy(SKIP_LEAF_STAGE_GROUP_BY_AGGREGATION, HintPredicates.AGGREGATE)
      .hintStrategy(PinotHintOptions.AGGREGATE_HINT_OPTIONS, HintPredicates.AGGREGATE)
      .hintStrategy(PinotHintOptions.JOIN_HINT_OPTIONS, HintPredicates.JOIN)
      .build();

  /**
   * Check if a hint-able {@link org.apache.calcite.rel.RelNode} contains a specific {@link RelHint} by name.
   *
   * @param hintList hint list from the {@link org.apache.calcite.rel.RelNode}.
   * @param hintName the name of the {@link RelHint} to be matched
   * @return true if it contains the hint
   */
  public static boolean containsHint(List<RelHint> hintList, String hintName) {
    for (RelHint relHint : hintList) {
      if (relHint.hintName.equals(hintName)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Check if a hint-able {@link org.apache.calcite.rel.RelNode} contains an option key for a specific hint name of
   * {@link RelHint}.
   *
   * @param hintList hint list from the {@link org.apache.calcite.rel.RelNode}.
   * @param hintName the name of the {@link RelHint}.
   * @param optionKey the option key to look for in the {@link RelHint#kvOptions}.
   * @return true if it contains the hint
   */
  public static boolean containsHintOption(List<RelHint> hintList, String hintName, String optionKey) {
    for (RelHint relHint : hintList) {
      if (relHint.hintName.equals(hintName)) {
        return relHint.kvOptions.containsKey(optionKey) && BooleanUtils.toBoolean(relHint.kvOptions.get(optionKey));
      }
    }
    return false;
  }

  /**
   * Retrieve the option value by option key in the {@link RelHint#kvOptions}. the option key is looked up from the
   * specified hint name for a hint-able {@link org.apache.calcite.rel.RelNode}.
   *
   * @param hintList hint list from the {@link org.apache.calcite.rel.RelNode}.
   * @param hintName the name of the {@link RelHint}.
   * @param optionKey the option key to look for in the {@link RelHint#kvOptions}.
   * @return true if it contains the hint
   */
  @Nullable
  public static String getHintOption(List<RelHint> hintList, String hintName, String optionKey) {
    for (RelHint relHint : hintList) {
      if (relHint.hintName.equals(hintName)) {
        return relHint.kvOptions.get(optionKey);
      }
    }
    return null;
  }
}
