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
import it.unimi.dsi.fastutil.objects.ObjectCollection;
import java.util.Arrays;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.function.NullableSingleInputAggregationFunction;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.segment.spi.AggregationFunctionType;


public class ListAggFunction extends NullableSingleInputAggregationFunction<ObjectCollection<String>, String> {

  private final String _separator;

  public ListAggFunction(ExpressionContext expression, String separator, boolean nullHandlingEnabled) {
    super(expression, nullHandlingEnabled);
    _separator = separator;
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
    ObjectCollection<String> valueSet = getObjectCollection(aggregationResultHolder);
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    String[] values = blockValSet.getStringValuesSV();
    forEachNotNull(length, blockValSet, (from, to) -> {
      valueSet.addAll(Arrays.asList(values).subList(from, to));
    });
  }

  protected ObjectCollection<String> getObjectCollection(AggregationResultHolder aggregationResultHolder) {
    ObjectArrayList<String> valueSet = aggregationResultHolder.getResult();
    if (valueSet == null) {
      valueSet = new ObjectArrayList<>();
      aggregationResultHolder.setValue(valueSet);
    }
    return valueSet;
  }

  protected ObjectCollection<String> getObjectCollection(GroupByResultHolder groupByResultHolder,
      int groupKey) {
    ObjectArrayList<String> valueSet = groupByResultHolder.getResult(groupKey);
    if (valueSet == null) {
      valueSet = new ObjectArrayList<>();
      groupByResultHolder.setValueForKey(groupKey, valueSet);
    }
    return valueSet;
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    String[] values = blockValSet.getStringValuesSV();
    forEachNotNull(length, blockValSet, (from, to) -> {
      for (int i = from; i < to; i++) {
        ObjectCollection<String> groupValueList = getObjectCollection(groupByResultHolder, groupKeyArray[i]);
        groupValueList.add(values[i]);
      }
    });
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    String[] values = blockValSet.getStringValuesSV();
    forEachNotNull(length, blockValSet, (from, to) -> {
      for (int i = from; i < to; i++) {
        for (int groupKey : groupKeysArray[i]) {
          ObjectCollection<String> groupValueList = getObjectCollection(groupByResultHolder, groupKey);
          groupValueList.add(values[i]);
        }
      }
    });
  }

  @Override
  public ObjectCollection<String> extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    return aggregationResultHolder.getResult();
  }

  @Override
  public ObjectCollection<String> extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    return groupByResultHolder.getResult(groupKey);
  }

  @Override
  public ObjectCollection<String> merge(ObjectCollection<String> intermediateResult1,
      ObjectCollection<String> intermediateResult2) {
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
  public ColumnDataType getIntermediateResultColumnType() {
    return ColumnDataType.OBJECT;
  }

  @Override
  public SerializedIntermediateResult serializeIntermediateResult(ObjectCollection<String> strings) {
    return new SerializedIntermediateResult(ObjectSerDeUtils.ObjectType.StringArrayList.getValue(),
        ObjectSerDeUtils.STRING_ARRAY_LIST_SER_DE.serialize((ObjectArrayList<String>) strings));
  }

  @Override
  public ObjectCollection<String> deserializeIntermediateResult(CustomObject customObject) {
    //noinspection unchecked
    return ObjectSerDeUtils.STRING_ARRAY_LIST_SER_DE.deserialize(customObject.getBuffer());
  }

  @Override
  public ColumnDataType getFinalResultColumnType() {
    return ColumnDataType.STRING;
  }

  @Override
  public String extractFinalResult(ObjectCollection<String> strings) {
    if (strings == null) {
      return null;
    }
    return StringUtils.join(strings, _separator);
  }
}
