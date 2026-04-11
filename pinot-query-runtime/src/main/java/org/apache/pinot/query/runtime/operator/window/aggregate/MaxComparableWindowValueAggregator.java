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

import java.util.ArrayDeque;
import javax.annotation.Nullable;


/**
 * Window value aggregator for MAX window function that preserves the input type by using {@link Comparable} for
 * comparisons. Used for types like BIG_DECIMAL that don't have a dedicated primitive-typed aggregator.
 */
public class MaxComparableWindowValueAggregator implements WindowValueAggregator<Object> {

  private final boolean _supportRemoval;
  private final ArrayDeque<Object> _deque = new ArrayDeque<>();
  private Object _maxValue = null;

  /**
   * @param supportRemoval whether this window value aggregator should support removal of values. Some cases require
   *                       only addition of values in which case this value aggregator will have O(1) space complexity;
   *                       if {@code supportRemoval} is true, this value aggregator will have O(K) space complexity
   *                       (where K is the max size of the window).
   */
  public MaxComparableWindowValueAggregator(boolean supportRemoval) {
    _supportRemoval = supportRemoval;
  }

  @Override
  public void addValue(@Nullable Object value) {
    if (value != null) {
      if (_supportRemoval) {
        // Remove previously added elements if they're < than the current element since they're no longer useful
        while (!_deque.isEmpty() && compare(_deque.peekLast(), value) < 0) {
          _deque.pollLast();
        }
        _deque.addLast(value);
      } else {
        if (_maxValue == null || compare(value, _maxValue) > 0) {
          _maxValue = value;
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
      if (!_deque.isEmpty() && compare(_deque.peekFirst(), value) == 0) {
        _deque.pollFirst();
      }
    }
  }

  @Nullable
  @Override
  public Object getCurrentAggregatedValue() {
    if (_supportRemoval) {
      return _deque.peekFirst();
    } else {
      return _maxValue;
    }
  }

  @Override
  public void clear() {
    _deque.clear();
    _maxValue = null;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static int compare(Object a, Object b) {
    return ((Comparable) a).compareTo(b);
  }
}
