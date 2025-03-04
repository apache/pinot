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
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.ObjectSerDeUtils;
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
   */
  @Nullable
  IntermediateResult extractAggregationResult(AggregationResultHolder aggregationResultHolder);

  /**
   * Extracts the intermediate result from the group-by result holder for the given group key (aggregation group-by).
   */
  @Nullable
  IntermediateResult extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey);

  /**
   * Merges two intermediate results.
   */
  @Nullable
  IntermediateResult merge(@Nullable IntermediateResult intermediateResult1,
      @Nullable IntermediateResult intermediateResult2);

  /**
   * Returns the {@link ColumnDataType} of the intermediate result.
   * <p>This column data type is used for transferring data in data table.
   */
  ColumnDataType getIntermediateResultColumnType();

  /**
   * Serializes the intermediate result into a custom object. This method should be implemented if the intermediate
   * result type is OBJECT.
   *
   * TODO: Override this method in the aggregation functions that return OBJECT type intermediate results to reduce the
   *       overhead of instanceof checks in the default implementation.
   */
  default SerializedIntermediateResult serializeIntermediateResult(IntermediateResult intermediateResult) {
    assert getIntermediateResultColumnType() == ColumnDataType.OBJECT;
    int type = ObjectSerDeUtils.ObjectType.getObjectType(intermediateResult).getValue();
    byte[] bytes = ObjectSerDeUtils.serialize(intermediateResult, type);
    return new SerializedIntermediateResult(type, bytes);
  }

  /**
   * Serialized intermediate result. Type can be used to identify the intermediate result type when deserializing it.
   */
  class SerializedIntermediateResult {
    private final int _type;
    private final byte[] _bytes;

    public SerializedIntermediateResult(int type, byte[] buffer) {
      _type = type;
      _bytes = buffer;
    }

    public int getType() {
      return _type;
    }

    public byte[] getBytes() {
      return _bytes;
    }
  }

  /**
   * Deserializes the intermediate result from the custom object. This method should be implemented if the intermediate
   * result type is OBJECT.
   *
   * TODO: Override this method in the aggregation functions that return OBJECT type intermediate results to not rely
   *       on the type to decouple this from ObjectSerDeUtils.
   */
  default IntermediateResult deserializeIntermediateResult(CustomObject customObject) {
    assert getIntermediateResultColumnType() == ColumnDataType.OBJECT;
    return ObjectSerDeUtils.deserialize(customObject);
  }

  /**
   * Returns the {@link ColumnDataType} of the final result.
   * <p>This column data type is used for constructing the result table</p>
   */
  ColumnDataType getFinalResultColumnType();

  /**
   * Extracts the final result used in the broker response from the given intermediate result.
   */
  @Nullable
  FinalResult extractFinalResult(@Nullable IntermediateResult intermediateResult);

  /**
   * Merges two final results. This can be used to optimized certain functions (e.g. DISTINCT_COUNT) when data is
   * partitioned on each server, where we may directly request servers to return final result and merge them on broker.
   */
  @Nullable
  default FinalResult mergeFinalResult(@Nullable FinalResult finalResult1, @Nullable FinalResult finalResult2) {
    throw new UnsupportedOperationException("Cannot merge final results for function: " + getType());
  }

  /**
   * Returns whether a star-tree index with the specified properties can be used for this aggregation function.
   */
  default boolean canUseStarTree(Map<String, Object> functionParameters) {
    // Implementations can override this method to perform additional checks on the function parameters
    return true;
  }

  /** @return Description of this operator for Explain Plan */
  String toExplainString();
}
