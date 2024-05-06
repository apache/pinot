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

import it.unimi.dsi.fastutil.objects.ObjectArrayPriorityQueue;
import java.util.Comparator;
import java.util.List;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.utils.OrderByComparatorFactory;
import org.apache.pinot.spi.data.FieldSpec;
import org.roaringbitmap.RoaringBitmap;


public class StringJoinOrderByFunction
    extends BaseStringJoinOrderByFunction<ObjectArrayPriorityQueue<Object[]>> {

  public StringJoinOrderByFunction(ExpressionContext expression, String separator,
      List<OrderByExpressionContext> orderByExpressionContext, List<FieldSpec.DataType> orderByDataTypes,
      boolean nullHandlingEnabled) {
    super(expression, separator, orderByExpressionContext, orderByDataTypes, nullHandlingEnabled);
  }

  @Override
  protected void aggregateArray(int length, AggregationResultHolder aggregationResultHolder, BlockValSet blockValSet,
      List<BlockValSet> orderByBlockValSets) {
    ObjectArrayPriorityQueue<Object[]> valueArray = createObjectArrayPriorityQueue(length);
    String[] valueExpr = blockValSet.getStringValuesSV();
    List<Object> orderByExpressions = extractOrderByValues(orderByBlockValSets);
    for (int i = 0; i < length; i++) {
      Object[] values = extractValues(valueExpr, orderByExpressions, i);
      valueArray.enqueue(values);
    }
    aggregationResultHolder.setValue(valueArray);
  }

  @Override
  protected void aggregateArrayWithNull(int length, AggregationResultHolder aggregationResultHolder,
      BlockValSet blockValSet, List<BlockValSet> orderByBlockValSets, RoaringBitmap nullBitmap) {
    ObjectArrayPriorityQueue<Object[]> valueArray = createObjectArrayPriorityQueue(length);
    String[] valueExpr = blockValSet.getStringValuesSV();
    List<Object> orderByExpressions = extractOrderByValues(orderByBlockValSets);
    for (int i = 0; i < length; i++) {
      if (!nullBitmap.contains(i)) {
        Object[] values = extractValues(valueExpr, orderByExpressions, i);
        valueArray.enqueue(values);
      }
    }
    aggregationResultHolder.setValue(valueArray);
  }

  @Override
  public ObjectArrayPriorityQueue<Object[]> merge(ObjectArrayPriorityQueue<Object[]> intermediateResult1,
      ObjectArrayPriorityQueue<Object[]> intermediateResult2) {
    if (intermediateResult1 == null) {
      return intermediateResult2;
    }
    if (intermediateResult2 == null) {
      return intermediateResult1;
    }
    while (!intermediateResult2.isEmpty()) {
      intermediateResult1.enqueue(intermediateResult2.dequeue());
    }
    return intermediateResult1;
  }

  public String extractFinalResult(ObjectArrayPriorityQueue<Object[]> stringList) {
    int valueIndex = _orderByExpressionContext.size();
    StringBuilder finalResultStringBuilder = new StringBuilder();
    finalResultStringBuilder.append(stringList.dequeue()[valueIndex]);
    while (!stringList.isEmpty()) {
      finalResultStringBuilder.append(_separator);
      finalResultStringBuilder.append(stringList.dequeue()[valueIndex]);
    }
    return finalResultStringBuilder.toString();
  }

  protected void setGroupByResult(GroupByResultHolder resultHolder, int groupKey, Object[] value) {
    ObjectArrayPriorityQueue<Object[]> valueArray = resultHolder.getResult(groupKey);
    if (valueArray == null) {
      valueArray = createObjectArrayPriorityQueue(0);
      resultHolder.setValueForKey(groupKey, valueArray);
    }
    valueArray.enqueue(value);
  }

  private ObjectArrayPriorityQueue<Object[]> createObjectArrayPriorityQueue(int length) {
    Comparator<Object[]> comparator = OrderByComparatorFactory.getComparator(_orderByExpressionContext, false);
    return new ObjectArrayPriorityQueue<>(length, comparator);
  }
}
