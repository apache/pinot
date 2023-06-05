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

import java.util.List;
import java.util.Map;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.segment.spi.AggregationFunctionType;


/**
 * Interface for aggregation functions.
 * <p>The implementation should be stateless, and can be shared among multiple segments in multiple threads. The result
 * for each segment should be stored and passed in via the result holder.
 *
 * @param <IntermediateResult> Intermediate result generated from segment
 * @param <FinalResult> Final result used in broker response
 */
@ThreadSafe
@SuppressWarnings("rawtypes")
public interface AggregationFunction<IntermediateResult, FinalResult extends Comparable> {

  /**
   * Returns the type of the aggregation function.
   */
  AggregationFunctionType getType();

  /**
   * Returns the column name to be used in the data schema of results.
   * e.g. 'MINMAXRANGEMV( foo)' -> 'minmaxrangemv(foo)', 'PERCENTILE75(bar)' -> 'percentile75(bar)'
   */
  String getResultColumnName();

  /**
   * Returns a list of input expressions needed for performing aggregation.
   */
  List<ExpressionContext> getInputExpressions();

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
  void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap);

  /**
   * Performs aggregation on the given group key array and block value sets (aggregation group-by on single-value
   * columns).
   */
  void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap);

  /**
   * Performs aggregation on the given group keys array and block value sets (aggregation group-by on multi-value
   * columns).
   */
  void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap);

  /**
   * Extracts the intermediate result from the aggregation result holder (aggregation only).
   * TODO: Support serializing/deserializing null values in DataTable and use null as the empty intermediate result
   */
  IntermediateResult extractAggregationResult(AggregationResultHolder aggregationResultHolder);

  /**
   * Extracts the intermediate result from the group-by result holder for the given group key (aggregation group-by).
   * TODO: Support serializing/deserializing null values in DataTable and use null as the empty intermediate result
   */
  IntermediateResult extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey);

  /**
   * Merges two intermediate results.
   * TODO: Support serializing/deserializing null values in DataTable and use null as the empty intermediate result
   */
  IntermediateResult merge(IntermediateResult intermediateResult1, IntermediateResult intermediateResult2);

  /**
   * Merges two intermediate results and also updates the aggregation result holder. This is needed when aggregation
   * is processed in multiple stages to store the intermediate results.
   */
  default void mergeAndUpdateResultHolder(IntermediateResult intermediateResult,
      AggregationResultHolder aggregationResultHolder) {
    // TODO: Remove when support for all aggregation functions is added to the Multistage engine.
    throw new UnsupportedOperationException("Aggregation operation is not supported.");
  }

  /**
   * Merges two intermediate results and also updates the group by result holder. This is needed when aggregation is
   * processed in multiple stages to store the intermediate results.
   */
  default void mergeAndUpdateResultHolder(IntermediateResult intermediateResult,
      GroupByResultHolder groupByResultHolder,
      int groupKey) {
    // TODO: Remove when support for all aggregation functions is added to the Multistage engine.
    throw new UnsupportedOperationException("Aggregation operation is not supported.");
  }

  /**
   * Returns the {@link ColumnDataType} of the intermediate result.
   * <p>This column data type is used for transferring data in data table.
   */
  ColumnDataType getIntermediateResultColumnType();

  /**
   * Returns the {@link ColumnDataType} of the final result.
   * <p>This column data type is used for constructing the result table</p>
   */
  ColumnDataType getFinalResultColumnType();

  /**
   * Extracts the final result used in the broker response from the given intermediate result.
   * TODO: Support serializing/deserializing null values in DataTable and use null as the empty intermediate result
   */
  FinalResult extractFinalResult(IntermediateResult intermediateResult);

  /** @return Description of this operator for Explain Plan */
  String toExplainString();
}
