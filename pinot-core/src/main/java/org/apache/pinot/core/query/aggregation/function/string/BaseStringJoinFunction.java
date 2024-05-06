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
package org.apache.pinot.core.query.aggregation.function.string;

import it.unimi.dsi.fastutil.objects.AbstractObjectCollection;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.function.BaseSingleInputAggregationFunction;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.roaringbitmap.RoaringBitmap;


public abstract class BaseStringJoinFunction<I extends AbstractObjectCollection<String>>
    extends BaseSingleInputAggregationFunction<I, String> {
  protected final boolean _nullHandlingEnabled;
  protected final String _separator;

  public BaseStringJoinFunction(ExpressionContext expression, String separator, boolean nullHandlingEnabled) {
    super(expression);
    _nullHandlingEnabled = nullHandlingEnabled;
    _separator = separator;
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.STRINGJOIN;
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
    return DataSchema.ColumnDataType.STRING;
  }

  @Override
  public I merge(I intermediateResult1, I intermediateResult2) {
    if (intermediateResult1 == null) {
      return intermediateResult2;
    }
    if (intermediateResult2 == null) {
      return intermediateResult1;
    }
    intermediateResult1.addAll(intermediateResult2);
    return intermediateResult1;
  }

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    if (_nullHandlingEnabled) {
      RoaringBitmap nullBitmap = blockValSet.getNullBitmap();
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
    if (_nullHandlingEnabled) {
      RoaringBitmap nullBitmap = blockValSet.getNullBitmap();
      if (nullBitmap != null && !nullBitmap.isEmpty()) {
        aggregateArrayGroupBySVWithNull(length, groupKeyArray, groupByResultHolder, blockValSet, nullBitmap);
        return;
      }
    }
    aggregateArrayGroupBySV(length, groupKeyArray, groupByResultHolder, blockValSet);
  }

  protected void aggregateArrayGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      BlockValSet blockValSet) {
    String[] values = blockValSet.getStringValuesSV();
    for (int i = 0; i < length; i++) {
      setGroupByResult(groupByResultHolder, groupKeyArray[i], values[i]);
    }
  }

  protected void aggregateArrayGroupBySVWithNull(int length, int[] groupKeyArray,
      GroupByResultHolder groupByResultHolder, BlockValSet blockValSet, RoaringBitmap nullBitmap) {
    String[] values = blockValSet.getStringValuesSV();
    for (int i = 0; i < length; i++) {
      if (!nullBitmap.contains(i)) {
        setGroupByResult(groupByResultHolder, groupKeyArray[i], values[i]);
      }
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    if (_nullHandlingEnabled) {
      RoaringBitmap nullBitmap = blockValSet.getNullBitmap();
      if (nullBitmap != null && !nullBitmap.isEmpty()) {
        aggregateArrayGroupByMVWithNull(length, groupKeysArray, groupByResultHolder, blockValSet, nullBitmap);
        return;
      }
    }
    aggregateArrayGroupByMV(length, groupKeysArray, groupByResultHolder, blockValSet);
  }

  protected void aggregateArrayGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      BlockValSet blockValSet) {
    String[] values = blockValSet.getStringValuesSV();
    for (int i = 0; i < length; i++) {
      for (int groupKey : groupKeysArray[i]) {
        setGroupByResult(groupByResultHolder, groupKey, values[i]);
      }
    }
  }

  protected void aggregateArrayGroupByMVWithNull(int length, int[][] groupKeysArray,
      GroupByResultHolder groupByResultHolder, BlockValSet blockValSet, RoaringBitmap nullBitmap) {
    String[] values = blockValSet.getStringValuesSV();
    for (int i = 0; i < length; i++) {
      if (!nullBitmap.contains(i)) {
        for (int groupKey : groupKeysArray[i]) {
          setGroupByResult(groupByResultHolder, groupKey, values[i]);
        }
      }
    }
  }

  abstract void setGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey, String value);

  @Override
  public I extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    return aggregationResultHolder.getResult();
  }

  @Override
  public I extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    return groupByResultHolder.getResult(groupKey);
  }

  @Override
  public String extractFinalResult(I stringCollection) {
    return StringUtils.join(stringCollection, _separator);
  }
}
