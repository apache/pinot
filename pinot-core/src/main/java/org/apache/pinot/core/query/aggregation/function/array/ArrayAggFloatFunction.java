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

import it.unimi.dsi.fastutil.floats.FloatArrayList;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.segment.spi.datasource.NullMode;
import org.roaringbitmap.RoaringBitmap;


public class ArrayAggFloatFunction extends BaseArrayAggFloatFunction<FloatArrayList> {
  public ArrayAggFloatFunction(ExpressionContext expression, NullMode nullMode) {
    super(expression, nullMode);
  }

  @Override
  protected void aggregateArray(int length, AggregationResultHolder aggregationResultHolder, BlockValSet blockValSet) {
    FloatArrayList valueArray = new FloatArrayList(length);
    float[] value = blockValSet.getFloatValuesSV();
    for (int i = 0; i < length; i++) {
      valueArray.add(value[i]);
    }
    aggregationResultHolder.setValue(valueArray);
  }

  @Override
  protected void aggregateArrayWithNull(int length, AggregationResultHolder aggregationResultHolder,
      BlockValSet blockValSet, RoaringBitmap nullBitmap) {
    FloatArrayList valueArray = new FloatArrayList(length);
    float[] value = blockValSet.getFloatValuesSV();
    for (int i = 0; i < length; i++) {
      if (!nullBitmap.contains(i)) {
        valueArray.add(value[i]);
      }
    }
    aggregationResultHolder.setValue(valueArray);
  }

  @Override

  protected void setGroupByResult(GroupByResultHolder resultHolder, int groupKey, float value) {
    FloatArrayList valueArray = resultHolder.getResult(groupKey);
    if (valueArray == null) {
      valueArray = new FloatArrayList();
      resultHolder.setValueForKey(groupKey, valueArray);
    }
    valueArray.add(value);
  }
}
