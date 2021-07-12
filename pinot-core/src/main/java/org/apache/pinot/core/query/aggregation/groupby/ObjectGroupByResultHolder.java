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
package org.apache.pinot.core.query.aggregation.groupby;

import com.google.common.base.Preconditions;


/**
 * Result Holder implemented using ObjectArray.
 */
public class ObjectGroupByResultHolder implements GroupByResultHolder {
  private final int _maxCapacity;

  private int _resultHolderCapacity;
  private Object[] _resultArray;

  /**
   * Constructor for the class.
   *
   * @param initialCapacity Initial capacity of the result holder
   * @param maxCapacity Maximum capacity of the result holder
   */
  public ObjectGroupByResultHolder(int initialCapacity, int maxCapacity) {
    _maxCapacity = maxCapacity;

    _resultHolderCapacity = initialCapacity;
    _resultArray = new Object[initialCapacity];
  }

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

  @Override
  public double getDoubleResult(int groupKey) {
    throw new UnsupportedOperationException();
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T getResult(int groupKey) {
    if (groupKey == GroupKeyGenerator.INVALID_ID) {
      return null;
    } else {
      return (T) _resultArray[groupKey];
    }
  }

  @Override
  public void setValueForKey(int groupKey, double newValue) {
    throw new UnsupportedOperationException();
  }

  @SuppressWarnings("unchecked")
  @Override
  public void setValueForKey(int groupKey, Object newValue) {
    if (groupKey != GroupKeyGenerator.INVALID_ID) {
      _resultArray[groupKey] = newValue;
    }
  }

  @Override
  public int size() {
    return _resultArray.length;
  }

  @Override
  public void clearResultHolder(int size) {
    _resultArray = new Object[size];
  }

  @Override
  public type getType() {
    return type.OBJECT;
  }
}
