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

import it.unimi.dsi.fastutil.objects.ObjectCollection;
import it.unimi.dsi.fastutil.objects.ObjectLinkedOpenHashSet;
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;


/**
 * The {@code ListAggDistinctFunction} extends the {@link ListAggFunction} to use {@link ObjectLinkedOpenHashSet} as
 * the intermediate result to hold distinct values for aggregation.
 */
public class ListAggDistinctFunction extends ListAggFunction {

  public ListAggDistinctFunction(ExpressionContext expression, String separator, boolean nullHandlingEnabled) {
    super(expression, separator, nullHandlingEnabled);
  }

  @Override
  protected ObjectLinkedOpenHashSet<String> getObjectCollection(AggregationResultHolder aggregationResultHolder) {
    ObjectLinkedOpenHashSet<String> valueSet = aggregationResultHolder.getResult();
    if (valueSet == null) {
      valueSet = new ObjectLinkedOpenHashSet<>();
      aggregationResultHolder.setValue(valueSet);
    }
    return valueSet;
  }

  @Override
  protected ObjectLinkedOpenHashSet<String> getObjectCollection(GroupByResultHolder groupByResultHolder,
      int groupKey) {
    ObjectLinkedOpenHashSet<String> valueSet = groupByResultHolder.getResult(groupKey);
    if (valueSet == null) {
      valueSet = new ObjectLinkedOpenHashSet<>();
      groupByResultHolder.setValueForKey(groupKey, valueSet);
    }
    return valueSet;
  }

  @Override
  public SerializedIntermediateResult serializeIntermediateResult(ObjectCollection<String> strings) {
    return new SerializedIntermediateResult(ObjectSerDeUtils.ObjectType.OrderedStringSet.getValue(),
        ObjectSerDeUtils.ORDERED_STRING_SET_SER_DE.serialize((ObjectLinkedOpenHashSet<String>) strings));
  }

  @Override
  public ObjectLinkedOpenHashSet<String> deserializeIntermediateResult(CustomObject customObject) {
    return ObjectSerDeUtils.ORDERED_STRING_SET_SER_DE.deserialize(customObject.getBuffer());
  }
}
