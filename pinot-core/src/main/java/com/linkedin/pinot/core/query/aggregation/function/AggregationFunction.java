/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.query.aggregation.function;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.query.aggregation.AggregationResultHolder;
import com.linkedin.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import javax.annotation.Nonnull;


/**
 * Interface for aggregation functions.
 *
 * @param <IntermediateResult> intermediate result generated from segment.
 * @param <FinalResult> final result used in broker response.
 */
public interface AggregationFunction<IntermediateResult, FinalResult extends Comparable> {

  /**
   * Get the name of the aggregation function.
   */
  @Nonnull
  String getName();

  /**
   * Given the aggregation columns, get the column name for the results.
   */
  @Nonnull
  String getColumnName(@Nonnull String[] columns);

  /**
   * Accept an aggregation function visitor to visit.
   */
  void accept(@Nonnull AggregationFunctionVisitorBase visitor);

  /**
   * Create an aggregation result holder for this function.
   */
  @Nonnull
  AggregationResultHolder createAggregationResultHolder();

  /**
   * Create a group-by result holder with the given initial capacity, max capacity and trim size for this function.
   */
  @Nonnull
  GroupByResultHolder createGroupByResultHolder(int initialCapacity, int maxCapacity, int trimSize);

  /**
   * Perform aggregation on the given projection block value sets.
   */
  void aggregate(int length, @Nonnull AggregationResultHolder aggregationResultHolder,
      @Nonnull BlockValSet... blockValSets);

  /**
   * Perform group-by on the given group key array and projection block value sets.
   * <p>This method is for all single-value group-by columns case, where each docId has only one group key.
   */
  void aggregateGroupBySV(int length, @Nonnull int[] groupKeyArray, @Nonnull GroupByResultHolder groupByResultHolder,
      @Nonnull BlockValSet... blockValSets);

  /**
   * Perform group-by on the given group keys array and projection block value sets.
   * <p>This method is for multi-value group by columns case, where each docId can have multiple group keys.
   */
  void aggregateGroupByMV(int length, @Nonnull int[][] groupKeysArray, @Nonnull GroupByResultHolder groupByResultHolder,
      @Nonnull BlockValSet... blockValSets);

  /**
   * Extract aggregation result from the aggregation result holder.
   */
  @Nonnull
  IntermediateResult extractAggregationResult(@Nonnull AggregationResultHolder aggregationResultHolder);

  /**
   * Extract group-by result from the group-by result holder and group key.
   */
  @Nonnull
  IntermediateResult extractGroupByResult(@Nonnull GroupByResultHolder groupByResultHolder, int groupKey);

  /**
   * Merge two intermediate results.
   */
  @Nonnull
  IntermediateResult merge(@Nonnull IntermediateResult intermediateResult1,
      @Nonnull IntermediateResult intermediateResult2);

  /**
   * Return whether the intermediate result is comparable.
   */
  boolean isIntermediateResultComparable();

  /**
   * Get the {@link FieldSpec.DataType} of the intermediate result.
   * <p>This data type is used for transferring data in data table.
   */
  @Nonnull
  FieldSpec.DataType getIntermediateResultDataType();

  /**
   * Extract the final result used in the broker response from the given intermediate result.
   */
  @Nonnull
  FinalResult extractFinalResult(@Nonnull IntermediateResult intermediateResult);
}
