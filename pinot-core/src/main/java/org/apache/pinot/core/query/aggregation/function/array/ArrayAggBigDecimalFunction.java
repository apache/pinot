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
import java.math.BigDecimal;
import java.util.Map;
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;


public class ArrayAggBigDecimalFunction extends BaseArrayAggBigDecimalFunction<ObjectArrayList<BigDecimal>> {
  public ArrayAggBigDecimalFunction(ExpressionContext expression, boolean nullHandlingEnabled) {
    super(expression, nullHandlingEnabled);
  }

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    ObjectArrayList<BigDecimal> valueArray =
        aggregationResultHolder.getResult() != null ? aggregationResultHolder.getResult()
            : new ObjectArrayList<>(length);
    if (blockValSet.isSingleValue()) {
      BigDecimal[] values = blockValSet.getBigDecimalValuesSV();
      forEachNotNull(length, blockValSet, (from, to) -> {
        for (int i = from; i < to; i++) {
          valueArray.add(values[i]);
        }
      });
    } else {
      BigDecimal[][] valuesArray = blockValSet.getBigDecimalValuesMV();
      forEachNotNull(length, blockValSet, (from, to) -> {
        for (int i = from; i < to; i++) {
          BigDecimal[] values = valuesArray[i];
          for (BigDecimal v : values) {
            valueArray.add(v);
          }
        }
      });
    }
    aggregationResultHolder.setValue(valueArray);
  }

  @Override
  protected void setGroupByResult(GroupByResultHolder resultHolder, int groupKey, BigDecimal value) {
    ObjectArrayList<BigDecimal> valueArray = resultHolder.getResult(groupKey);
    if (valueArray == null) {
      valueArray = new ObjectArrayList<>();
      resultHolder.setValueForKey(groupKey, valueArray);
    }
    valueArray.add(value);
  }

  @Override
  public SerializedIntermediateResult serializeIntermediateResult(ObjectArrayList<BigDecimal> list) {
    return new SerializedIntermediateResult(ObjectSerDeUtils.ObjectType.BigDecimalArrayList.getValue(),
        ObjectSerDeUtils.BIG_DECIMAL_ARRAY_LIST_SER_DE.serialize(list));
  }

  @Override
  public ObjectArrayList<BigDecimal> deserializeIntermediateResult(CustomObject customObject) {
    return ObjectSerDeUtils.BIG_DECIMAL_ARRAY_LIST_SER_DE.deserialize(customObject.getBuffer());
  }
}
