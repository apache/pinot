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

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import java.util.Map;
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.spi.data.FieldSpec;


public class ArrayAggDistinctLongFunction extends BaseArrayAggLongFunction<LongSet> {
  public ArrayAggDistinctLongFunction(ExpressionContext expression, FieldSpec.DataType dataType,
      boolean nullHandlingEnabled) {
    super(expression, dataType, nullHandlingEnabled);
  }

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    long[] value = blockValSet.getLongValuesSV();
    LongOpenHashSet valueSet =
        aggregationResultHolder.getResult() != null ? aggregationResultHolder.getResult() : new LongOpenHashSet(length);

    forEachNotNull(length, blockValSet, (from, to) -> {
      for (int i = from; i < to; i++) {
        valueSet.add(value[i]);
      }
    });
    aggregationResultHolder.setValue(valueSet);
  }

  @Override
  protected void setGroupByResult(GroupByResultHolder resultHolder, int groupKey, long value) {
    LongOpenHashSet valueSet = resultHolder.getResult(groupKey);
    if (valueSet == null) {
      valueSet = new LongOpenHashSet();
      resultHolder.setValueForKey(groupKey, valueSet);
    }
    valueSet.add(value);
  }

  @Override
  public SerializedIntermediateResult serializeIntermediateResult(LongSet longSet) {
    return new SerializedIntermediateResult(ObjectSerDeUtils.ObjectType.LongSet.getValue(),
        ObjectSerDeUtils.LONG_SET_SER_DE.serialize(longSet));
  }

  @Override
  public LongSet deserializeIntermediateResult(CustomObject customObject) {
    return ObjectSerDeUtils.LONG_SET_SER_DE.deserialize(customObject.getBuffer());
  }
}
