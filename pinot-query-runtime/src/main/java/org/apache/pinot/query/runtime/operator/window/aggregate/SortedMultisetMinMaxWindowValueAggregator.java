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

import java.util.TreeMap;
import javax.annotation.Nullable;


/**
 * MIN / MAX window aggregator backed by a sorted multiset (a {@link TreeMap} of value to occurrence count). Unlike the
 * monotonic-deque aggregators used for sliding-window MIN / MAX, this aggregator supports removal of arbitrary values
 * in any order — necessary for window frames with a non-default {@code EXCLUDE} clause. Add / remove / query are all
 * {@code O(log K)} where {@code K} is the number of distinct values currently in the window.
 *
 * <p>All non-null values added to a single instance must implement {@link Comparable} and share a runtime type that is
 * mutually comparable; mixing types (e.g. {@code Integer} and {@code Long}) is undefined. Callers obtain instances
 * through {@link WindowValueAggregatorFactory}, which guarantees this because window function values come from a single
 * typed input column. {@code removeValue} for a value not currently present is a no-op.
 */
public class SortedMultisetMinMaxWindowValueAggregator implements WindowValueAggregator<Object> {

  private final TreeMap<Object, Integer> _counts = new TreeMap<>(SortedMultisetMinMaxWindowValueAggregator::compare);
  private final boolean _isMin;

  public SortedMultisetMinMaxWindowValueAggregator(boolean isMin) {
    _isMin = isMin;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static int compare(Object a, Object b) {
    return ((Comparable) a).compareTo(b);
  }

  @Override
  public void addValue(@Nullable Object value) {
    if (value != null) {
      _counts.merge(value, 1, Integer::sum);
    }
  }

  @Override
  public void removeValue(@Nullable Object value) {
    if (value == null) {
      return;
    }
    Integer count = _counts.get(value);
    if (count == null) {
      return;
    }
    if (count == 1) {
      _counts.remove(value);
    } else {
      _counts.put(value, count - 1);
    }
  }

  @Nullable
  @Override
  public Object getCurrentAggregatedValue() {
    if (_counts.isEmpty()) {
      return null;
    }
    return _isMin ? _counts.firstKey() : _counts.lastKey();
  }

  @Override
  public void clear() {
    _counts.clear();
  }
}
