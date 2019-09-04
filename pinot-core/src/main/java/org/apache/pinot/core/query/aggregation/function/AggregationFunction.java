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
package org.apache.pinot.core.query.aggregation.function;

import org.apache.pinot.common.function.AggregationFunctionType;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;


/**
 * Interface for aggregation functions.
 *
 * @param <IntermediateResult> Intermediate result generated from segment
 * @param <FinalResult> Final result used in broker response
 */
public interface AggregationFunction<IntermediateResult, FinalResult extends Comparable> {

  /**
   * Returns the type of the aggregation function.
   */
  AggregationFunctionType getType();

  /**
   * Returns the result column name for the given aggregation column, e.g. 'SUM(foo)' -> 'sum_foo'.
   */
  String getColumnName(String column);

  /**
   * Accepts an aggregation function visitor to visit.
   */
  void accept(AggregationFunctionVisitorBase visitor);

  /**
   * Returns an aggregation result holder for this function (aggregation only).
   */
  AggregationResultHolder createAggregationResultHolder();

  /**
   * Returns a group-by result holder with the given initial capacity and max capacity for this function (aggregation
   * group-by).
   */
  GroupByResultHolder createGroupByResultHolder(int initialCapacity, int maxCapacity);

  /**
   * Performs aggregation on the given block value sets (aggregation only).
   */
  void aggregate(int length, AggregationResultHolder aggregationResultHolder, BlockValSet... blockValSets);

  /**
   * Performs aggregation on the given group key array and block value sets (aggregation group-by on single-value
   * columns).
   */
  void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      BlockValSet... blockValSets);

  /**
   * Performs aggregation on the given group keys array and block value sets (aggregation group-by on multi-value
   * columns).
   */
  void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      BlockValSet... blockValSets);

  /**
   * Extracts the intermediate result from the aggregation result holder (aggregation only).
   */
  IntermediateResult extractAggregationResult(AggregationResultHolder aggregationResultHolder);

  /**
   * Extracts the intermediate result from the group-by result holder for the given group key (aggregation group-by).
   */
  IntermediateResult extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey);

  /**
   * Merges two intermediate results.
   */
  IntermediateResult merge(IntermediateResult intermediateResult1, IntermediateResult intermediateResult2);

  /**
   * Returns whether the intermediate result is comparable.
   */
  boolean isIntermediateResultComparable();

  /**
   * Returns the {@link ColumnDataType} of the intermediate result.
   * <p>This column data type is used for transferring data in data table.
   */
  ColumnDataType getIntermediateResultColumnType();

  /**
   * Extracts the final result used in the broker response from the given intermediate result.
   */
  FinalResult extractFinalResult(IntermediateResult intermediateResult);
}
