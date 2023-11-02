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

import it.unimi.dsi.fastutil.doubles.DoubleHeapPriorityQueue;
import it.unimi.dsi.fastutil.doubles.DoublePriorityQueue;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.core.query.distinct.DistinctExecutor;
import org.apache.pinot.segment.spi.datasource.NullMode;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * {@link DistinctExecutor} for distinct order-by queries with single raw DOUBLE column.
 */
public class RawDoubleSingleColumnDistinctOrderByExecutor extends BaseRawDoubleSingleColumnDistinctExecutor {
  private final DoublePriorityQueue _priorityQueue;

  public RawDoubleSingleColumnDistinctOrderByExecutor(ExpressionContext expression, DataType dataType,
      OrderByExpressionContext orderByExpression, int limit, NullMode nullMode) {
    super(expression, dataType, limit, nullMode);

    assert orderByExpression.getExpression().equals(expression);
    int comparisonFactor = orderByExpression.isAsc() ? -1 : 1;
    _priorityQueue = new DoubleHeapPriorityQueue(Math.min(limit, MAX_INITIAL_CAPACITY),
        (d1, d2) -> Double.compare(d1, d2) * comparisonFactor);
  }

  @Override
  protected boolean add(double value) {
    if (!_valueSet.contains(value)) {
      if (_valueSet.size() < _limit) {
        _valueSet.add(value);
        _priorityQueue.enqueue(value);
      } else {
        double firstValue = _priorityQueue.firstDouble();
        if (_priorityQueue.comparator().compare(value, firstValue) > 0) {
          _valueSet.remove(firstValue);
          _valueSet.add(value);
          _priorityQueue.dequeueDouble();
          _priorityQueue.enqueue(value);
        }
      }
    }
    return false;
  }
}
