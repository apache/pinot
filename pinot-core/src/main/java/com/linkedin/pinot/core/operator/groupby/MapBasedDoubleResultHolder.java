/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.operator.groupby;

import com.linkedin.pinot.core.query.utils.Pair;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;


public class MapBasedDoubleResultHolder implements ResultHolder {

  private Long2DoubleOpenHashMap _resultMap;
  private double _defaultValue;

  public MapBasedDoubleResultHolder(double defaultValue) {
    _defaultValue = defaultValue;
    _resultMap = new Long2DoubleOpenHashMap();
  }

  /**
   * {@inheritDoc}
   * No-op for MapBasedResultHolder.
   *
   * @param maxUniqueKeys
   */
  @Override
  public void ensureCapacity(int maxUniqueKeys) {
  }

  /**
   * {@inheritDoc}
   *
   * @param groupKey
   * @return
   */
  @Override
  public double getDoubleResult(long groupKey) {
    if (!_resultMap.containsKey(groupKey)) {
      return _defaultValue;
    }
    return _resultMap.get(groupKey);
  }

  @Override
  public Pair getResult(long groupKey) {
    throw new RuntimeException("Unsupported method 'getResult' (returning Pair) for class " + getClass().getName());
  }

  /**
   * {@inheritDoc}
   *
   * @param groupKey
   * @param newValue
   */
  @Override
  public void putValueForKey(long groupKey, double newValue) {
    _resultMap.put(groupKey, newValue);
  }

  @Override
  public void putValueForKey(long groupKey, Object newValue) {
    throw new RuntimeException(
        "Unsupported method 'putValueForKey' (with Object input) for class " + getClass().getName());
  }
}
