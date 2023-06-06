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
package org.apache.pinot.core.query.distinct.raw;

import it.unimi.dsi.fastutil.floats.FloatHeapPriorityQueue;
import it.unimi.dsi.fastutil.floats.FloatPriorityQueue;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.core.query.distinct.DistinctExecutor;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * {@link DistinctExecutor} for distinct order-by queries with single raw FLOAT column.
 */
public class RawFloatSingleColumnDistinctOrderByExecutor extends BaseRawFloatSingleColumnDistinctExecutor {
  private final FloatPriorityQueue _priorityQueue;

  public RawFloatSingleColumnDistinctOrderByExecutor(ExpressionContext expression, DataType dataType,
      OrderByExpressionContext orderByExpression, int limit, boolean nullHandlingEnabled) {
    super(expression, dataType, limit, nullHandlingEnabled);

    assert orderByExpression.getExpression().equals(expression);
    int comparisonFactor = orderByExpression.isAsc() ? -1 : 1;
    _priorityQueue = new FloatHeapPriorityQueue(Math.min(limit, MAX_INITIAL_CAPACITY),
        (f1, f2) -> Float.compare(f1, f2) * comparisonFactor);
  }

  @Override
  protected boolean add(float value) {
    if (!_valueSet.contains(value)) {
      if (_valueSet.size() < _limit - (_hasNull ? 1 : 0)) {
        _valueSet.add(value);
        _priorityQueue.enqueue(value);
      } else {
        float firstValue = _priorityQueue.firstFloat();
        if (_priorityQueue.comparator().compare(value, firstValue) > 0) {
          _valueSet.remove(firstValue);
          _valueSet.add(value);
          _priorityQueue.dequeueFloat();
          _priorityQueue.enqueue(value);
        }
      }
    }
    return false;
  }
}
