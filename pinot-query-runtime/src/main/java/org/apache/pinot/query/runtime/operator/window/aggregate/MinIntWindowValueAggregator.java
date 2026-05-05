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

import it.unimi.dsi.fastutil.ints.IntArrayFIFOQueue;
import javax.annotation.Nullable;


/**
 * Window value aggregator for MIN window function over INT types.
 * Uses primitive {@code int} operations and {@link IntArrayFIFOQueue} to avoid boxing overhead and improve cache
 * locality compared to the {@link Comparable}-based aggregator.
 */
public class MinIntWindowValueAggregator implements WindowValueAggregator<Object> {

  private final boolean _supportRemoval;
  private final IntArrayFIFOQueue _deque = new IntArrayFIFOQueue();
  private Integer _minValue = null;

  /**
   * @param supportRemoval whether this window value aggregator should support removal of values. Some cases require
   *                       only addition of values in which case this value aggregator will have O(1) space complexity;
   *                       if {@code supportRemoval} is true, this value aggregator will have O(K) space complexity
   *                       (where K is the max size of the window).
   */
  public MinIntWindowValueAggregator(boolean supportRemoval) {
    _supportRemoval = supportRemoval;
  }

  @Override
  public void addValue(@Nullable Object value) {
    if (value != null) {
      int intValue = ((Number) value).intValue();
      if (_supportRemoval) {
        // Remove previously added elements if they're > than the current element since they're no longer useful
        while (!_deque.isEmpty() && _deque.lastInt() > intValue) {
          _deque.dequeueLastInt();
        }
        _deque.enqueue(intValue);
      } else {
        if (_minValue == null || intValue < _minValue) {
          _minValue = intValue;
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
      int intValue = ((Number) value).intValue();
      if (!_deque.isEmpty() && _deque.firstInt() == intValue) {
        _deque.dequeueInt();
      }
    }
  }

  @Nullable
  @Override
  public Object getCurrentAggregatedValue() {
    if (_supportRemoval) {
      if (_deque.isEmpty()) {
        return null;
      }
      return _deque.firstInt();
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
