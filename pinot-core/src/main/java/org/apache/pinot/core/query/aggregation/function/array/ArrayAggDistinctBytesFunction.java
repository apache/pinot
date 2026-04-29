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
import it.unimi.dsi.fastutil.objects.ObjectSet;
import java.util.Map;
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.spi.utils.ByteArray;


public class ArrayAggDistinctBytesFunction extends BaseArrayAggBytesFunction<ObjectSet<ByteArray>> {
  public ArrayAggDistinctBytesFunction(ExpressionContext expression, boolean nullHandlingEnabled) {
    super(expression, nullHandlingEnabled);
  }

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    ObjectOpenHashSet<ByteArray> valueSet =
        aggregationResultHolder.getResult() != null ? aggregationResultHolder.getResult()
            : new ObjectOpenHashSet<>(length);
    if (blockValSet.isSingleValue()) {
      byte[][] values = blockValSet.getBytesValuesSV();
      forEachNotNull(length, blockValSet, (from, to) -> {
        for (int i = from; i < to; i++) {
          valueSet.add(new ByteArray(values[i]));
        }
      });
    } else {
      byte[][][] valuesArray = blockValSet.getBytesValuesMV();
      forEachNotNull(length, blockValSet, (from, to) -> {
        for (int i = from; i < to; i++) {
          byte[][] values = valuesArray[i];
          for (byte[] v : values) {
            valueSet.add(new ByteArray(v));
          }
        }
      });
    }
    aggregationResultHolder.setValue(valueSet);
  }

  @Override
  protected void setGroupByResult(GroupByResultHolder resultHolder, int groupKey, byte[] value) {
    ObjectOpenHashSet<ByteArray> valueSet = resultHolder.getResult(groupKey);
    if (valueSet == null) {
      valueSet = new ObjectOpenHashSet<>();
      resultHolder.setValueForKey(groupKey, valueSet);
    }
    valueSet.add(new ByteArray(value));
  }

  @Override
  public ObjectArrayList<ByteArray> extractFinalResult(ObjectSet<ByteArray> intermediateResult) {
    return intermediateResult == null ? new ObjectArrayList<>() : new ObjectArrayList<>(intermediateResult);
  }

  @Override
  public SerializedIntermediateResult serializeIntermediateResult(ObjectSet<ByteArray> set) {
    return new SerializedIntermediateResult(ObjectSerDeUtils.ObjectType.BytesSet.getValue(),
        ObjectSerDeUtils.BYTES_SET_SER_DE.serialize(set));
  }

  @Override
  public ObjectSet<ByteArray> deserializeIntermediateResult(CustomObject customObject) {
    return (ObjectSet<ByteArray>) ObjectSerDeUtils.BYTES_SET_SER_DE.deserialize(customObject.getBuffer());
  }
}
