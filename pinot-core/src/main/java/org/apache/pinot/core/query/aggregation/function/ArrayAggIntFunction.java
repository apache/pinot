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

import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.spi.data.FieldSpec;
import org.roaringbitmap.RoaringBitmap;


public class ArrayAggIntFunction extends ArrayAggFunction<IntArrayList> {
  public ArrayAggIntFunction(ExpressionContext expression, FieldSpec.DataType dataType, boolean nullHandlingEnabled) {
    super(expression, dataType, nullHandlingEnabled);
  }

  @Override
  protected void aggregateArray(int length, AggregationResultHolder aggregationResultHolder,
      BlockValSet blockValSet) {
    ObjectAggregationResultHolder resultHolder = (ObjectAggregationResultHolder) aggregationResultHolder;
    int[] value = blockValSet.getIntValuesSV();
    IntArrayList valueArray = new IntArrayList(length);
    for (int i = 0; i < length; i++) {
      valueArray.add(value[i]);
    }
    resultHolder.setValue(valueArray);
  }

  @Override
  protected void aggregateArrayWithNull(int length, AggregationResultHolder aggregationResultHolder,
      BlockValSet blockValSet, RoaringBitmap nullBitmap) {
    ObjectAggregationResultHolder resultHolder = (ObjectAggregationResultHolder) aggregationResultHolder;
    int[] value = blockValSet.getIntValuesSV();
    IntArrayList valueArray = new IntArrayList(length);
    for (int i = 0; i < length; i++) {
      if (!nullBitmap.contains(i)) {
        valueArray.add(value[i]);
      }
    }
    resultHolder.setValue(valueArray);
  }

  @Override
  protected void aggregateArrayGroupBySV(int length, int[] groupKeyArray,
      GroupByResultHolder groupByResultHolder, BlockValSet blockValSet) {
    int[] values = blockValSet.getIntValuesSV();
    for (int i = 0; i < length; i++) {
      setGroupByResult(groupByResultHolder, groupKeyArray[i], values[i]);
    }
  }

  @Override
  protected void aggregateArrayGroupBySVWithNull(int length, int[] groupKeyArray,
      GroupByResultHolder groupByResultHolder, BlockValSet blockValSet, RoaringBitmap nullBitmap) {
    int[] values = blockValSet.getIntValuesSV();
    for (int i = 0; i < length; i++) {
      if (!nullBitmap.contains(i)) {
        setGroupByResult(groupByResultHolder, groupKeyArray[i], values[i]);
      }
    }
  }

  protected void setGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey, int value) {
    ObjectGroupByResultHolder resultHolder = (ObjectGroupByResultHolder) groupByResultHolder;
    IntArrayList groupValue = resultHolder.getResult(groupKey);
    if (groupValue == null) {
      resultHolder.setValueForKey(groupKey, new IntArrayList(value));
    } else {
      groupValue.add(value);
    }
  }

  @Override
  protected void aggregateArrayGroupByMV(int length, int[][] groupKeysArray,
      GroupByResultHolder groupByResultHolder, BlockValSet blockValSet) {
    int[] values = blockValSet.getIntValuesSV();
    for (int i = 0; i < length; i++) {
      int[] groupKeys = groupKeysArray[i];
      int value = values[i];
      for (int groupKey : groupKeys) {
        setGroupByResult(groupByResultHolder, groupKey, value);
      }
    }
  }

  @Override
  protected void aggregateArrayGroupByMVWithNull(int length, int[][] groupKeysArray,
      GroupByResultHolder groupByResultHolder, BlockValSet blockValSet, RoaringBitmap nullBitmap) {
    int[] values = blockValSet.getIntValuesSV();
    for (int i = 0; i < length; i++) {
      if (!nullBitmap.contains(i)) {
        int[] groupKeys = groupKeysArray[i];
        int value = values[i];
        for (int groupKey : groupKeys) {
          setGroupByResult(groupByResultHolder, groupKey, value);
        }
      }
    }
  }

  @Override
  public IntArrayList merge(IntArrayList intermediateResult1, IntArrayList intermediateResult2) {
    if (intermediateResult1 == null || intermediateResult1.isEmpty()) {
      return intermediateResult2;
    }
    if (intermediateResult2 == null || intermediateResult2.isEmpty()) {
      return intermediateResult1;
    }
    intermediateResult1.addAll(intermediateResult2);
    return intermediateResult1;
  }

  @Override
  public IntArrayList extractFinalResult(IntArrayList intArrayList) {
    return new IntArrayList(intArrayList.elements());
  }
}
