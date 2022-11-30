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
import java.util.Arrays;


public class IntGroupByResultHolder implements GroupByResultHolder {

  private final int _maxCapacity;
  private final int _defaultValue;

  private int _resultHolderCapacity;
  private int[] _resultArray;

  /**
   * Constructor for the class.
   *
   * @param initialCapacity Initial capacity of the result holder
   * @param maxCapacity Maximum capacity of the result holder
   * @param defaultValue Default value of un-initialized results
   */
  public IntGroupByResultHolder(int initialCapacity, int maxCapacity, int defaultValue) {
    _maxCapacity = maxCapacity;
    _defaultValue = defaultValue;

    _resultHolderCapacity = initialCapacity;
    _resultArray = new int[initialCapacity];
    if (defaultValue != 0) {
      Arrays.fill(_resultArray, defaultValue);
    }
  }

  @Override
  public void ensureCapacity(int capacity) {
    Preconditions.checkArgument(capacity <= _maxCapacity);

    if (capacity > _resultHolderCapacity) {
      int copyLength = _resultHolderCapacity;
      _resultHolderCapacity = Math.max(_resultHolderCapacity * 2, capacity);

      // Cap the growth to maximum possible number of group keys
      _resultHolderCapacity = Math.min(_resultHolderCapacity, _maxCapacity);

      int[] current = _resultArray;
      _resultArray = new int[_resultHolderCapacity];
      System.arraycopy(current, 0, _resultArray, 0, copyLength);

      if (_defaultValue != 0) {
        Arrays.fill(_resultArray, copyLength, _resultHolderCapacity, _defaultValue);
      }
    }
  }

  @Override
  public double getDoubleResult(int groupKey) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getIntResult(int groupKey) {
    if (groupKey == GroupKeyGenerator.INVALID_ID) {
      return _defaultValue;
    } else {
      return _resultArray[groupKey];
    }
  }

  @Override
  public <T> T getResult(int groupKey) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setValueForKey(int groupKey, double newValue) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setValueForKey(int groupKey, int newValue) {
    if (groupKey != GroupKeyGenerator.INVALID_ID) {
      _resultArray[groupKey] = newValue;
    }
  }

  @Override
  public void setValueForKey(int groupKey, Object newValue) {
    throw new UnsupportedOperationException();
  }
}
