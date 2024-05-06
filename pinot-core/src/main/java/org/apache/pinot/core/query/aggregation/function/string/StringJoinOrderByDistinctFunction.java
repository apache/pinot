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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.TreeSet;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.utils.OrderByComparatorFactory;
import org.apache.pinot.spi.data.FieldSpec;
import org.roaringbitmap.RoaringBitmap;


public class StringJoinOrderByDistinctFunction
    extends BaseStringJoinOrderByFunction<TreeSet<Object[]>> {

  public StringJoinOrderByDistinctFunction(ExpressionContext expression, String separator,
      List<OrderByExpressionContext> orderByExpressionContext, List<FieldSpec.DataType> orderByDataTypes,
      boolean nullHandlingEnabled) {
    super(expression, separator, orderByExpressionContext, orderByDataTypes, nullHandlingEnabled);
  }

  @Override
  protected void aggregateArray(int length, AggregationResultHolder aggregationResultHolder, BlockValSet blockValSet,
      List<BlockValSet> orderByBlockValSets) {
    TreeSet<Object[]> valueArray = createTreeSet();
    String[] valueExpr = blockValSet.getStringValuesSV();
    List<Object> orderByExpressions = extractOrderByValues(orderByBlockValSets);
    for (int i = 0; i < length; i++) {
      Object[] values = extractValues(valueExpr, orderByExpressions, i);
      valueArray.add(values);
    }
    aggregationResultHolder.setValue(valueArray);
  }

  @Override
  protected void aggregateArrayWithNull(int length, AggregationResultHolder aggregationResultHolder,
      BlockValSet blockValSet, List<BlockValSet> orderByBlockValSets, RoaringBitmap nullBitmap) {
    TreeSet<Object[]> valueArray = createTreeSet();
    String[] valueExpr = blockValSet.getStringValuesSV();
    List<Object> orderByExpressions = extractOrderByValues(orderByBlockValSets);
    for (int i = 0; i < length; i++) {
      if (!nullBitmap.contains(i)) {
        Object[] values = extractValues(valueExpr, orderByExpressions, i);
        valueArray.add(values);
      }
    }
    aggregationResultHolder.setValue(valueArray);
  }

  @Override
  public TreeSet<Object[]> merge(TreeSet<Object[]> intermediateResult1,
      TreeSet<Object[]> intermediateResult2) {
    if (intermediateResult1 == null) {
      return intermediateResult2;
    }
    if (intermediateResult2 == null) {
      return intermediateResult1;
    }
    intermediateResult1.addAll(intermediateResult2);
    return intermediateResult1;
  }

  public String extractFinalResult(TreeSet<Object[]> stringList) {
    int valueIndex = _orderByExpressionContext.size();
    StringBuilder finalResultStringBuilder = new StringBuilder();
    finalResultStringBuilder.append(Objects.requireNonNull(stringList.pollFirst())[valueIndex]);
    while (!stringList.isEmpty()) {
      finalResultStringBuilder.append(_separator);
      finalResultStringBuilder.append(Objects.requireNonNull(stringList.pollFirst())[valueIndex]);
    }
    return finalResultStringBuilder.toString();
  }

  protected void setGroupByResult(GroupByResultHolder resultHolder, int groupKey, Object[] value) {
    TreeSet<Object[]> valueArray = resultHolder.getResult(groupKey);
    if (valueArray == null) {
      valueArray = createTreeSet();
      resultHolder.setValueForKey(groupKey, valueArray);
    }
    valueArray.add(value);
  }

  private TreeSet<Object[]> createTreeSet() {
    List<OrderByExpressionContext> comparators = new ArrayList<>(_orderByExpressionContext);
    comparators.add(new OrderByExpressionContext(_expression, true));
    Comparator<Object[]> comparator = OrderByComparatorFactory.getComparator(comparators, false);
    return new TreeSet<>(comparator);
  }
}
