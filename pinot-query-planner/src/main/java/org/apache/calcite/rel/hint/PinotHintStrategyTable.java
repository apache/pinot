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

import com.google.common.collect.ImmutableList;


/**
 * Default hint strategy set for Pinot query.
 */
public class PinotHintStrategyTable {
  public static final String INTERNAL_AGG_INTERMEDIATE_STAGE = "aggIntermediateStage";
  public static final String INTERNAL_AGG_FINAL_STAGE = "aggFinalStage";

  public static final String SKIP_LEAF_STAGE_GROUP_BY_AGGREGATION = "skipLeafStageGroupByAggregation";



  private PinotHintStrategyTable() {
    // do not instantiate.
  }

  public static final HintStrategyTable PINOT_HINT_STRATEGY_TABLE = HintStrategyTable.builder()
      .hintStrategy(INTERNAL_AGG_INTERMEDIATE_STAGE, HintPredicates.AGGREGATE)
      .hintStrategy(INTERNAL_AGG_FINAL_STAGE, HintPredicates.AGGREGATE)
      .hintStrategy(SKIP_LEAF_STAGE_GROUP_BY_AGGREGATION, HintPredicates.AGGREGATE)
      .build();

  public static boolean containsHint(ImmutableList<RelHint> hintList, String hintName) {
    for (RelHint relHint : hintList) {
      if (relHint.hintName.equals(hintName)) {
        return true;
      }
    }
    return false;
  }
}
