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

import com.clearspring.analytics.util.Preconditions;


/**
 * Result Holder implemented using ObjectArray.
 */
public class ObjectGroupByResultHolder implements GroupByResultHolder {
  private Object[] _resultArray;
  private int _resultHolderCapacity;
  private int _maxCapacity;

  /**
   * Constructor for the class.
   *
   * @param initialCapacity
   * @param maxCapacity
   */
  public ObjectGroupByResultHolder(int initialCapacity, int maxCapacity) {
    _resultArray = new Object[initialCapacity];
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
      int copyLength = _resultHolderCapacity;
      _resultHolderCapacity = Math.max(_resultHolderCapacity * 2, capacity);

      // Cap the growth to maximum possible number of group keys
      _resultHolderCapacity = Math.min(_resultHolderCapacity, _maxCapacity);

      Object[] current = _resultArray;
      _resultArray = new Object[_resultHolderCapacity];
      System.arraycopy(current, 0, _resultArray, 0, copyLength);
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
  public double getDoubleResult(int groupKey) {
    throw new RuntimeException(
        "Unsupported method getDoubleResult (returning double) for class " + getClass().getName());
  }

  @Override
  public Object getResult(int groupKey) {
    return _resultArray[groupKey];
  }

  /**
   * {@inheritDoc}
   *
   * @param groupKey
   * @param newValue
   */
  @Override
  public void setValueForKey(int groupKey, double newValue) {
    throw new RuntimeException(
        "Unsupported method 'putValueForKey' (with double param) for class " + getClass().getName());
  }

  @Override
  public void setValueForKey(int groupKey, Object newValue) {
    _resultArray[groupKey] = newValue;
  }
}
