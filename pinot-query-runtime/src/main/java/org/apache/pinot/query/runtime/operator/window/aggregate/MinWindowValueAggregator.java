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
package org.apache.pinot.query.runtime.operator.window.aggregate;

import it.unimi.dsi.fastutil.doubles.DoubleArrayFIFOQueue;
import javax.annotation.Nullable;


/**
 * Window value aggregator for MIN window function.
 */
public class MinWindowValueAggregator implements WindowValueAggregator<Object> {

  private final boolean _supportRemoval;
  private final DoubleArrayFIFOQueue _deque = new DoubleArrayFIFOQueue();
  private Double _minValue = null;

  /**
   * @param supportRemoval whether this window value aggregator should support removal of values. Some cases require
   *                       only addition of values in which case this value aggregator will have O(1) space complexity;
   *                       if {@code supportRemoval} is true, this value aggregator will have O(K) space complexity
   *                       (where K is the max size of the window).
   */
  public MinWindowValueAggregator(boolean supportRemoval) {
    _supportRemoval = supportRemoval;
  }

  @Override
  public void addValue(@Nullable Object value) {
    if (value != null) {
      double doubleValue = ((Number) value).doubleValue();
      if (_supportRemoval) {
        // Remove previously added elements if they're > than the current element since they're no longer useful
        while (!_deque.isEmpty() && _deque.lastDouble() > doubleValue) {
          _deque.dequeueLastDouble();
        }
        _deque.enqueue(doubleValue);
      } else {
        if (_minValue == null || doubleValue < _minValue) {
          _minValue = doubleValue;
        }
      }
    }
  }

  @Override
  public void removeValue(@Nullable Object value) {
    if (!_supportRemoval) {
      throw new UnsupportedOperationException();
    }

    if (value != null) {
      double doubleValue = ((Number) value).doubleValue();
      if (!_deque.isEmpty() && _deque.firstDouble() == doubleValue) {
        _deque.dequeueDouble();
      }
    }
  }

  @Override
  public Object getCurrentAggregatedValue() {
    if (_supportRemoval) {
      if (_deque.isEmpty()) {
        return null;
      }
      return _deque.firstDouble();
    } else {
      return _minValue;
    }
  }

  @Override
  public void clear() {
    _deque.clear();
    _minValue = null;
  }
}
