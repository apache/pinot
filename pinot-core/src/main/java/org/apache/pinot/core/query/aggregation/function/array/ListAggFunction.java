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

import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
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


public class ListAggFunction
    extends BaseSingleInputAggregationFunction<ObjectArrayList<String>, String> {

  private final String _separator;
  private final boolean _nullHandlingEnabled;
  private final boolean _isDistinct;

  public ListAggFunction(ExpressionContext expression, String separator, boolean isDistinct,
      boolean nullHandlingEnabled) {
    super(expression);
    _separator = separator;
    _isDistinct = isDistinct;
    _nullHandlingEnabled = nullHandlingEnabled;
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.LISTAGG;
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
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    ObjectArrayList<String> valueSet = aggregationResultHolder.getResult();
    if (valueSet == null) {
      valueSet = new ObjectArrayList<>();
      aggregationResultHolder.setValue(valueSet);
    }
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    String[] values = blockValSet.getStringValuesSV();
    if (_nullHandlingEnabled) {
      RoaringBitmap nullBitmap = blockValSet.getNullBitmap();
      if (nullBitmap != null && !nullBitmap.isEmpty()) {
        aggregateArrayWithNull(length, valueSet, values, nullBitmap);
        return;
      }
    }
    aggregateArray(length, valueSet, values);
  }

  private void aggregateArray(int length, ObjectArrayList<String> valueSet, String[] values) {
    ObjectOpenHashSet<String> distinctValueSet = new ObjectOpenHashSet<>();
    for (int i = 0; i < length; i++) {
      String value = values[i];
      if (_isDistinct) {
        if (distinctValueSet.contains(value)) {
          continue;
        } else {
          distinctValueSet.add(value);
        }
      }
      valueSet.add(value);
    }
  }

  private void aggregateArrayWithNull(int length, ObjectArrayList<String> valueSet, String[] values,
      RoaringBitmap nullBitmap) {
    ObjectOpenHashSet<String> distinctValueSet = new ObjectOpenHashSet<>();
    for (int i = 0; i < length; i++) {
      if (!nullBitmap.contains(i)) {
        String value = values[i];
        if (_isDistinct) {
          if (distinctValueSet.contains(value)) {
            continue;
          } else {
            distinctValueSet.add(value);
          }
        }
        valueSet.add(value);
      }
    }
  }

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

  protected void setGroupByResult(GroupByResultHolder resultHolder, int groupKey, String value) {
    ObjectArrayList<String> valueArray = resultHolder.getResult(groupKey);
    if (valueArray == null) {
      valueArray = new ObjectArrayList<>();
      resultHolder.setValueForKey(groupKey, valueArray);
    }
    if (_isDistinct && valueArray.contains(value)) {
      return;
    }
    valueArray.add(value);
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    String[] values = blockValSet.getStringValuesSV();
    for (int i = 0; i < length; i++) {
      for (int groupKey : groupKeysArray[i]) {
        ObjectArrayList<String> valueSet = groupByResultHolder.getResult(groupKey);
        if (valueSet == null) {
          valueSet = new ObjectArrayList<>();
          groupByResultHolder.setValueForKey(groupKey, valueSet);
        }
        if (_nullHandlingEnabled) {
          String value = values[i];
          if (value != null) {
            valueSet.add(value);
          }
        } else {
          valueSet.add(values[i]);
        }
      }
    }
  }

  @Override
  public ObjectArrayList<String> extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    return aggregationResultHolder.getResult();
  }

  @Override
  public ObjectArrayList<String> extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    return groupByResultHolder.getResult(groupKey);
  }

  @Override
  public ObjectArrayList<String> merge(ObjectArrayList<String> intermediateResult1,
      ObjectArrayList<String> intermediateResult2) {
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
  public DataSchema.ColumnDataType getIntermediateResultColumnType() {
    return DataSchema.ColumnDataType.OBJECT;
  }

  @Override
  public DataSchema.ColumnDataType getFinalResultColumnType() {
    return DataSchema.ColumnDataType.STRING;
  }

  @Override
  public String extractFinalResult(ObjectArrayList<String> strings) {
    return StringUtils.join(strings, _separator);
  }
}
