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

/**
 * Result Holder implemented using DoubleArray.
 */
public class DoubleArrayBasedResultHolder implements ResultHolder {
  private DoubleResultArray _resultArray;
  private int _resultHolderCapacity;
  private int _maxGroupKeys;
  private double _defaultValue;

  /**
   * Constructor for the class.
   *
   * @param initialCapacity
   */
  public DoubleArrayBasedResultHolder(DoubleResultArray resultArray, int initialCapacity, int maxGroupKeys,
      double defaultValue) {
    _resultHolderCapacity = initialCapacity;
    _maxGroupKeys = maxGroupKeys;
    _defaultValue = defaultValue;
    _resultArray = resultArray;
  }

  /**
   * {@inheritDoc}
   *
   * @param maxUniqueKeys
   */
  @Override
  public void ensureCapacity(int maxUniqueKeys) {
    if (maxUniqueKeys > _resultHolderCapacity) {
      _resultHolderCapacity = Math.max(_resultHolderCapacity * 2, maxUniqueKeys);

      // Cap the growth to maximum possible number of group keys
      _resultHolderCapacity = Math.min(_resultHolderCapacity, _maxGroupKeys);
      _resultArray.expand(_resultHolderCapacity);
    }
  }

  /**
   * {@inheritDoc}
   *
   * Array based result holder assumes group by key fit within integer.
   * This is a valid assumption as ArrayBasedResultHolder gets instantiated
   * iff groupKey are less than 1M.
   *
   * @param groupKey
   * @return
   */
  @Override
  public double getDoubleResult(long groupKey) {
    return _resultArray.getDoubleResult((int) groupKey);
  }

  @Override
  public Object getResult(long groupKey) {
    throw new RuntimeException(
        "Unsupported method getResult (returning Object) for class " + getClass().getName());
  }

  /**
   * {@inheritDoc}
   *
   * @param groupKey
   * @param newValue
   */
  @Override
  public void putValueForKey(long groupKey, double newValue) {
    _resultArray.set((int) groupKey, newValue);
  }

  @Override
  public void putValueForKey(long groupKey, Object newValue) {
    throw new RuntimeException(
        "Unsupported method 'putValueForKey' (with Object param) for class " + getClass().getName());
  }
}
