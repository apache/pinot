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

import com.google.common.base.Preconditions;

/**
 * Result Holder implemented using DoubleArray.
 */
public class DoubleArrayBasedResultHolder implements ResultHolder {
  private DoubleResultArray _resultArray;
  private int _resultHolderCapacity;
  private final int _maxCapacity;

  /**
   * Constructor for the class.
   *
   * @param resultArray
   * @param initialCapacity
   * @param maxCapacity
   */
  public DoubleArrayBasedResultHolder(DoubleResultArray resultArray, int initialCapacity, int maxCapacity) {
    _resultArray = resultArray;
    _resultHolderCapacity = initialCapacity;
    _maxCapacity = maxCapacity;
  }

  /**
   * {@inheritDoc}
   *
   * @param capacity
   */
  @Override
  public void ensureCapacity(int capacity) {
    Preconditions.checkArgument(capacity <= _maxCapacity);

    if (capacity > _resultHolderCapacity) {
      _resultHolderCapacity = Math.max(_resultHolderCapacity * 2, capacity);

      // Cap the growth to maximum possible number of group keys
      _resultHolderCapacity = Math.min(_resultHolderCapacity, _maxCapacity);
      _resultArray.expand(_resultHolderCapacity);
    }
  }

  @Override
  public double getDoubleResult() {
    return _resultArray.getDoubleResult(0);
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
  public double getDoubleResult(int groupKey) {
    return _resultArray.getDoubleResult(groupKey);
  }

  @Override
  public Object getResult() {
    throw new RuntimeException(
        "Unsupported method getResult (returning Object) for class " + getClass().getName());  }

  @Override
  public Object getResult(long groupKey) {
    throw new RuntimeException(
        "Unsupported method getResult (returning Object) for class " + getClass().getName());
  }

  @Override
  public void setValue(double newValue) {
    Preconditions.checkState(_resultHolderCapacity == 1);
    _resultArray.set(0, newValue);
  }

  @Override
  public void setValue(Object newValue) {
    throw new RuntimeException(
        "Unsupported method 'setValue' (with Object param) for class " + getClass().getName());
  }

  /**
   * {@inheritDoc}
   *
   * @param groupKey
   * @param newValue
   */
  @Override
  public void setValueForKey(long groupKey, double newValue) {
    _resultArray.set((int) groupKey, newValue);
  }

  @Override
  public void setValueForKey(long groupKey, Object newValue) {
    throw new RuntimeException(
        "Unsupported method 'setValueForKey' (with Object param) for class " + getClass().getName());
  }
}
