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
import java.util.Arrays;


/**
 * Result Holder implemented using DoubleArray.
 */
public class DoubleGroupByResultHolder implements GroupByResultHolder {
  private double[] _resultArray;
  private final double _defaultValue;

  private int _resultHolderCapacity;
  private final int _maxCapacity;

  /**
   * Constructor for the class.
   *
   * @param initialCapacity
   * @param maxCapacity
   * @param defaultValue
   */
  public DoubleGroupByResultHolder(int initialCapacity, int maxCapacity, double defaultValue) {
    _resultHolderCapacity = initialCapacity;
    _defaultValue = defaultValue;
    _maxCapacity = maxCapacity;

    _resultArray = new double[initialCapacity];
    if (defaultValue != 0.0) {
      Arrays.fill(_resultArray, defaultValue);
    }
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

      double[] current = _resultArray;
      _resultArray = new double[_resultHolderCapacity];
      System.arraycopy(current, 0, _resultArray, 0, copyLength);

      if (_defaultValue != 0.0) {
        Arrays.fill(_resultArray, copyLength, _resultHolderCapacity, _defaultValue);
      }
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
    return _resultArray[groupKey];
  }

  @Override
  public Object getResult(int groupKey) {
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
  public void setValueForKey(int groupKey, double newValue) {
    _resultArray[groupKey] = newValue;
  }

  @Override
  public void setValueForKey(int groupKey, Object newValue) {
    throw new RuntimeException(
        "Unsupported method 'setValueForKey' (with Object param) for class " + getClass().getName());
  }
}
