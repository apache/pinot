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
package org.apache.pinot.core.query.aggregation.function.array;

import java.nio.ByteBuffer;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.function.BaseSingleInputAggregationFunction;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.segment.local.aggregator.ArrayAggDistinctValueAggregator.ElementType;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.data.FieldSpec.DataType;


public abstract class BaseArrayAggFunction<I, F extends Comparable> extends BaseSingleInputAggregationFunction<I, F> {

  private final DataSchema.ColumnDataType _resultColumnType;
  private final DataType _elementDataType;

  public BaseArrayAggFunction(ExpressionContext expression, DataType dataType, boolean nullHandlingEnabled) {
    super(expression, nullHandlingEnabled);
    _resultColumnType = DataSchema.ColumnDataType.fromDataTypeMV(dataType);
    _elementDataType = dataType;
  }

  /// Returns the element data type declared in the query (the 2nd `arrayAgg` argument). A star-tree cell stores the
  /// source column's stored type, so star-tree selection must reject a query whose declared element stored type
  /// differs from the column's — the raw path serves those through read-time conversion instead.
  public DataType getElementDataType() {
    return _elementDataType;
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.ARRAYAGG;
  }

  /// The star-tree index stores an arrayAgg cell as a serialized distinct set. Only the distinct variant is
  /// associative under merge (set-union), so only distinct arrayAgg can be served from a star-tree. Non-distinct
  /// variants keep the default `false` and fall back to the raw scan. See [ArrayAggDistinctValueAggregator].
  @Override
  public boolean canUseStarTree(Map<String, Object> functionParameters) {
    return false;
  }

  /// Returns a [ByteBuffer] positioned at the payload of a star-tree pre-aggregated arrayAgg cell, after validating
  /// the leading element-type tag matches `expectedElementType`. The payload that follows is byte-for-byte identical
  /// to the corresponding `ObjectSerDeUtils.*_SET_SER_DE` format, so callers deserialize it with the matching set
  /// SerDe's `deserialize(ByteBuffer)` overload. See [ArrayAggDistinctValueAggregator] for the writer.
  protected static ByteBuffer starTreeSetPayload(byte[] cell, ElementType expectedElementType) {
    ByteBuffer buffer = ByteBuffer.wrap(cell);
    ElementType elementType = ElementType.fromTag(buffer.get());
    if (elementType != expectedElementType) {
      throw new IllegalStateException(
          "Star-tree arrayAgg cell element type " + elementType + " does not match expected " + expectedElementType);
    }
    return buffer;
  }

  @Override
  public AggregationResultHolder createAggregationResultHolder() {
    return new ObjectAggregationResultHolder();
  }

  @Override
  public GroupByResultHolder createGroupByResultHolder(int initialCapacity, int maxCapacity) {
    return new ObjectGroupByResultHolder(initialCapacity, maxCapacity);
  }

  @Override
  public DataSchema.ColumnDataType getIntermediateResultColumnType() {
    return DataSchema.ColumnDataType.OBJECT;
  }

  @Override
  public DataSchema.ColumnDataType getFinalResultColumnType() {
    return _resultColumnType;
  }

  @Nullable
  @Override
  public I extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    return aggregationResultHolder.getResult();
  }

  @Nullable
  @Override
  public I extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    return groupByResultHolder.getResult(groupKey);
  }
}
