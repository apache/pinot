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

import java.util.Deque;
import java.util.LinkedList;
import javax.annotation.Nullable;


/**
 * Window value aggregator for MIN window function.
 */
public class MinWindowValueAggregator implements WindowValueAggregator<Object> {

  private final Deque<Double> _deque = new LinkedList<>();

  @Override
  public void addValue(@Nullable Object value) {
    if (value != null) {
      Double doubleValue = ((Number) value).doubleValue();
      // Remove previously added elements if they're > than the current element since they're no longer useful
      while (!_deque.isEmpty() && _deque.peekLast().compareTo(doubleValue) > 0) {
        _deque.pollLast();
      }
      _deque.addLast(doubleValue);
    }
  }

  @Override
  public void removeValue(@Nullable Object value) {
    if (value != null) {
      Double doubleValue = ((Number) value).doubleValue();
      if (!_deque.isEmpty() && _deque.peekFirst().compareTo(doubleValue) == 0) {
        _deque.pollFirst();
      }
    }
  }

  @Override
  public Object getCurrentAggregatedValue() {
    if (_deque.isEmpty()) {
      return null;
    }
    return _deque.peekFirst();
  }

  @Override
  public void clear() {
    _deque.clear();
  }
}
