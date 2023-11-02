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

import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.function.BaseSingleInputAggregationFunction;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.datasource.NullMode;
import org.apache.pinot.spi.data.FieldSpec;
import org.roaringbitmap.RoaringBitmap;


public abstract class BaseArrayAggFunction<I, F extends Comparable> extends BaseSingleInputAggregationFunction<I, F> {

  protected final NullMode _nullMode;
  private final DataSchema.ColumnDataType _resultColumnType;

  public BaseArrayAggFunction(ExpressionContext expression, FieldSpec.DataType dataType, NullMode nullMode) {
    super(expression);
    _nullMode = nullMode;
    _resultColumnType = DataSchema.ColumnDataType.fromDataTypeMV(dataType);
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.ARRAYAGG;
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

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    if (_nullMode.nullAtQueryTime()) {
      RoaringBitmap nullBitmap = blockValSet.getNullBitmap(_nullMode);
      if (nullBitmap != null && !nullBitmap.isEmpty()) {
        aggregateArrayWithNull(length, aggregationResultHolder, blockValSet, nullBitmap);
        return;
      }
    }
    aggregateArray(length, aggregationResultHolder, blockValSet);
  }

  protected abstract void aggregateArray(int length, AggregationResultHolder aggregationResultHolder,
      BlockValSet blockValSet);

  protected abstract void aggregateArrayWithNull(int length, AggregationResultHolder aggregationResultHolder,
      BlockValSet blockValSet, RoaringBitmap nullBitmap);

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    if (_nullMode.nullAtQueryTime()) {
      RoaringBitmap nullBitmap = blockValSet.getNullBitmap(_nullMode);
      if (nullBitmap != null && !nullBitmap.isEmpty()) {
        aggregateArrayGroupBySVWithNull(length, groupKeyArray, groupByResultHolder, blockValSet, nullBitmap);
        return;
      }
    }
    aggregateArrayGroupBySV(length, groupKeyArray, groupByResultHolder, blockValSet);
  }

  protected abstract void aggregateArrayGroupBySV(int length, int[] groupKeyArray,
      GroupByResultHolder groupByResultHolder, BlockValSet blockValSet);

  protected abstract void aggregateArrayGroupBySVWithNull(int length, int[] groupKeyArray,
      GroupByResultHolder groupByResultHolder, BlockValSet blockValSet, RoaringBitmap nullBitmap);

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    if (_nullMode.nullAtQueryTime()) {
      RoaringBitmap nullBitmap = blockValSet.getNullBitmap(_nullMode);
      if (nullBitmap != null && !nullBitmap.isEmpty()) {
        aggregateArrayGroupByMVWithNull(length, groupKeysArray, groupByResultHolder, blockValSet, nullBitmap);
        return;
      }
    }
    aggregateArrayGroupByMV(length, groupKeysArray, groupByResultHolder, blockValSet);
  }

  protected abstract void aggregateArrayGroupByMV(int length, int[][] groupKeysArray,
      GroupByResultHolder groupByResultHolder, BlockValSet blockValSet);

  protected abstract void aggregateArrayGroupByMVWithNull(int length, int[][] groupKeysArray,
      GroupByResultHolder groupByResultHolder, BlockValSet blockValSet, RoaringBitmap nullBitmap);

  @Override
  public I extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    return aggregationResultHolder.getResult();
  }

  @Override
  public I extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    return groupByResultHolder.getResult(groupKey);
  }
}
