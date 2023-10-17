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

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.roaringbitmap.RoaringBitmap;


public class ArrayAggDoubleFunction extends ArrayAggBaseDoubleFunction<DoubleArrayList> {
  public ArrayAggDoubleFunction(ExpressionContext expression, boolean nullHandlingEnabled) {
    super(expression, nullHandlingEnabled);
  }

  @Override
  protected void aggregateArray(int length, AggregationResultHolder aggregationResultHolder, BlockValSet blockValSet) {
    DoubleArrayList valueArray = new DoubleArrayList(length);
    double[] value = blockValSet.getDoubleValuesSV();
    for (int i = 0; i < length; i++) {
      valueArray.add(value[i]);
    }
    aggregationResultHolder.setValue(valueArray);
  }

  @Override
  protected void aggregateArrayWithNull(int length, AggregationResultHolder aggregationResultHolder,
      BlockValSet blockValSet, RoaringBitmap nullBitmap) {
    DoubleArrayList valueArray = new DoubleArrayList(length);
    double[] value = blockValSet.getDoubleValuesSV();
    for (int i = 0; i < length; i++) {
      if (!nullBitmap.contains(i)) {
        valueArray.add(value[i]);
      }
    }
    aggregationResultHolder.setValue(valueArray);
  }

  @Override
  protected void setGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey, double value) {
    ObjectGroupByResultHolder resultHolder = (ObjectGroupByResultHolder) groupByResultHolder;
    if (resultHolder.getResult(groupKey) == null) {
      DoubleArrayList valueArray = new DoubleArrayList();
      valueArray.add(value);
      resultHolder.setValueForKey(groupKey, valueArray);
    } else {
      DoubleArrayList valueArray = resultHolder.getResult(groupKey);
      valueArray.add(value);
    }
  }
}
