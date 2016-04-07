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

import java.util.Arrays;


public class ArrayBasedResultHolder implements ResultHolder {
  private double[] _resultArray;
  private int _resultHolderCapacity;
  private double _defaultValue;

  /**
   * Constructor for the class.
   *
   * @param resultHolderCapacity
   */
  public ArrayBasedResultHolder(int resultHolderCapacity, double defaultValue) {
    _resultHolderCapacity = resultHolderCapacity;
    _defaultValue = defaultValue;

    _resultArray = new double[resultHolderCapacity];
    Arrays.fill(_resultArray, defaultValue);
  }

  /**
   * {@inheritDoc}
   * @return
   */
  @Override
  public double[] getResultStore() {
    return _resultArray;
  }

  /**
   * {@inheritDoc}
   * @param groupKey
   * @return
   */
  @Override
  public double getResultForGroupKey(long groupKey) {
    return _resultArray[(int) groupKey];
  }

  /**
   * {@inheritDoc}
   *
   * @param maxUniqueKeys
   */
  @Override
  public void ensureCapacity(int maxUniqueKeys) {
    if (maxUniqueKeys > _resultHolderCapacity) {
      double[] tmp = _resultArray;

      _resultHolderCapacity = Math.max(_resultHolderCapacity * 2, maxUniqueKeys);
      _resultArray = new double[_resultHolderCapacity];

      Arrays.fill(_resultArray, _defaultValue);
      System.arraycopy(tmp, 0, _resultArray, 0, tmp.length);
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
  public double getValueForKey(long groupKey) {
    return _resultArray[(int) groupKey];
  }

  /**
   * {@inheritDoc}
   *
   * @param groupKey
   * @param newValue
   */
  @Override
  public void putValueForKey(long groupKey, double newValue) {
    _resultArray[((int) groupKey)] = newValue;
  }
}
