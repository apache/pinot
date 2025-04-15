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
package org.apache.pinot.common.collections;

import java.util.AbstractList;


/**
 * An immutable list like the one returned by {@link java.util.Collections#nCopies(int, Object)}, but with two values
 * (that are not interleaved) instead of a single one. Useful for avoiding unnecessary allocations.
 */
public class DualValueList<E> extends AbstractList<E> {

  private final E _firstValue;
  private final E _secondValue;
  private final int _firstCount;
  private final int _totalSize;

  public DualValueList(E firstValue, int firstCount, E secondValue, int secondCount) {
    _firstValue = firstValue;
    _firstCount = firstCount;
    _secondValue = secondValue;
    _totalSize = firstCount + secondCount;
  }

  @Override
  public E get(int index) {
    if (index < 0 || index >= _totalSize) {
      throw new IndexOutOfBoundsException(index);
    }
    return index < _firstCount ? _firstValue : _secondValue;
  }

  @Override
  public int size() {
    return _totalSize;
  }
}
