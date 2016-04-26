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


/**
 * ResultHolder implementation using Pair.
 */
public class PairArrayBasedResultHolder implements ResultHolder<Pair> {
  private ResultArray _resultArray;
  private int _resultHolderCapacity;
  private final int _capacityCap;

  /**
   * Constructor for the class.
   *
   * @param resultArray
   * @param initialCapacity
   * @param capacityCap
   */
  public PairArrayBasedResultHolder(ResultArray resultArray, int initialCapacity, int capacityCap) {
    _resultArray = resultArray;
    _resultHolderCapacity = initialCapacity;
    _capacityCap = capacityCap;
  }

  /**
   * {@inheritDoc}
   *
   * @param capacity
   */
  @Override
  public void ensureCapacity(int capacity) {
    if (capacity > _resultHolderCapacity) {
      _resultHolderCapacity = Math.max(_resultHolderCapacity * 2, capacity);

      // Cap the growth to maximum possible number of group keys
      _resultHolderCapacity = Math.min(_resultHolderCapacity, _capacityCap);
      _resultArray.expand(_resultHolderCapacity);
    }
  }

  @Override
  public double getDoubleResult(int groupKey) {
    throw new RuntimeException("Unsupported method 'getDoubleResult' for class " + getClass().getName());
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
  public Pair getResult(long groupKey) {
    return _resultArray.getResult((int) groupKey);
  }

  /**
   * {@inheritDoc}
   *
   * @param groupKey
   * @param newValue
   */
  @Override
  public void putValueForKey(long groupKey, double newValue) {
    throw new RuntimeException("Unsupported method 'putValueForKey (of double Type) for class " + getClass().getName());
  }

  @Override
  public void putValueForKey(long groupKey, Pair pair) {
    _resultArray.set((int) groupKey, pair);
  }
}
