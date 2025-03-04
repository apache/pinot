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
import java.util.Arrays;
import java.util.Map;
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;


public class ArrayAggStringFunction extends BaseArrayAggStringFunction<ObjectArrayList<String>> {
  public ArrayAggStringFunction(ExpressionContext expression, boolean nullHandlingEnabled) {
    super(expression, nullHandlingEnabled);
  }

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    String[] value = blockValSet.getStringValuesSV();
    ObjectArrayList<String> valueArray = new ObjectArrayList<>(length);
    forEachNotNull(length, blockValSet, (from, to) -> valueArray.addAll(Arrays.asList(value).subList(from, to)));
    aggregationResultHolder.setValue(valueArray);
  }

  @Override
  protected void setGroupByResult(GroupByResultHolder resultHolder, int groupKey, String value) {
    ObjectArrayList<String> valueArray = resultHolder.getResult(groupKey);
    if (valueArray == null) {
      valueArray = new ObjectArrayList<>();
      resultHolder.setValueForKey(groupKey, valueArray);
    }
    valueArray.add(value);
  }

  @Override
  public SerializedIntermediateResult serializeIntermediateResult(ObjectArrayList<String> stringArrayList) {
    return new SerializedIntermediateResult(ObjectSerDeUtils.ObjectType.StringArrayList.getValue(),
        ObjectSerDeUtils.STRING_ARRAY_LIST_SER_DE.serialize(stringArrayList));
  }

  @Override
  public ObjectArrayList<String> deserializeIntermediateResult(CustomObject customObject) {
    //noinspection unchecked
    return ObjectSerDeUtils.STRING_ARRAY_LIST_SER_DE.deserialize(customObject.getBuffer());
  }
}
