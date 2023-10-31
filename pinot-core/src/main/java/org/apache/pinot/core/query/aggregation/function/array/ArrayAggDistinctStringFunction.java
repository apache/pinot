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

import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import java.util.Arrays;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.roaringbitmap.RoaringBitmap;


public class ArrayAggDistinctStringFunction extends BaseArrayAggStringFunction<ObjectOpenHashSet<String>> {
  public ArrayAggDistinctStringFunction(ExpressionContext expression, boolean nullHandlingEnabled) {
    super(expression, nullHandlingEnabled);
  }

  @Override
  protected void aggregateArray(int length, AggregationResultHolder aggregationResultHolder, BlockValSet blockValSet) {
    ObjectOpenHashSet<String> valueArray = new ObjectOpenHashSet<>(length);
    String[] value = blockValSet.getStringValuesSV();
    valueArray.addAll(Arrays.asList(value));
    aggregationResultHolder.setValue(valueArray);
  }

  @Override
  protected void aggregateArrayWithNull(int length, AggregationResultHolder aggregationResultHolder,
      BlockValSet blockValSet, RoaringBitmap nullBitmap) {
    ObjectOpenHashSet<String> valueArray = new ObjectOpenHashSet<>(length);
    String[] value = blockValSet.getStringValuesSV();
    for (int i = 0; i < length; i++) {
      if (!nullBitmap.contains(i)) {
        valueArray.add(value[i]);
      }
    }
    aggregationResultHolder.setValue(valueArray);
  }

  @Override
  protected void setGroupByResult(GroupByResultHolder resultHolder, int groupKey, String value) {
    ObjectOpenHashSet<String> valueSet = resultHolder.getResult(groupKey);
    if (valueSet == null) {
      valueSet = new ObjectOpenHashSet<>();
      resultHolder.setValueForKey(groupKey, valueSet);
    }
    valueSet.add(value);
  }
}
