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

import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;


public class MapBasedResultHolder implements ResultHolder {

  private Long2DoubleOpenHashMap _resultMap;
  private double _defaultValue;

  public MapBasedResultHolder(double defaultValue) {
    _defaultValue = defaultValue;
    _resultMap = new Long2DoubleOpenHashMap();
  }

  /**
   * {@inheritDoc}
   * @return
   */
  @Override
  public Object getResultStore() {
    return _resultMap;
  }

  /**
   * {@inheritDoc}
   * @param groupKey
   * @return
   */
  public double getResultForGroupKey(long groupKey) {
    return _resultMap.get(groupKey);
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
  public double getValueForKey(long groupKey) {
    if (!_resultMap.containsKey(groupKey)) {
      return _defaultValue;
    }
    return _resultMap.get(groupKey);
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
}
