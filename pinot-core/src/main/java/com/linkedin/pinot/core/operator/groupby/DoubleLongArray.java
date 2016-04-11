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
import com.linkedin.pinot.core.query.aggregation.function.AvgAggregationFunction;
import com.linkedin.pinot.core.query.utils.Pair;
import java.util.Arrays;


/**
 * ResultArray implementation using Pair of Double and Long.
 */
public class DoubleLongArray implements ResultArray {
  private double[] _doubles;
  private long[] _longs;

  public DoubleLongArray(int capacity, Pair<Double, Long> valuePair) {
    _doubles = new double[capacity];
    _longs = new long[capacity];
    setAll(valuePair);
  }

  @Override
  public void set(int index, double value) {
    throw new RuntimeException("Method 'set' not supported for class " + getClass().getName());
  }

  /**
   * {@inheritDoc}
   *
   * @param pair
   * @param index
   */
  @Override
  public void set(Pair pair, int index) {
    _doubles[index] = (double) pair.getFirst();
    _longs[index] = (long) pair.getSecond();
  }

  @Override
  public void setAll(double value) {
    throw new RuntimeException("Method 'setAll(double)' not supported for class " + getClass().getName());
  }

  /**
   * {@inheritDoc}
   *
   * @param doubleLongPair
   */
  @Override
  public void setAll(Pair doubleLongPair) {
    Arrays.fill(_doubles, (double) doubleLongPair.getFirst());
    Arrays.fill(_longs, (long) doubleLongPair.getSecond());
  }

  @Override
  public double getDoubleResult(int index) {
    throw new RuntimeException("Method 'getDoubleResult' not supported for class " + getClass().getName());
  }

  /**
   * {@inheritDoc}
   *
   * @param index
   * @return
   */
  @Override
  public AvgAggregationFunction.AvgPair getResult(int index) {
    return new AvgAggregationFunction.AvgPair(_doubles[index], _longs[index]);
  }

  /**
   * {@inheritDoc}
   * @return
   */
  @Override
  public int size() {
    return _doubles.length;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void expand(int newSize) {
    Preconditions.checkArgument(newSize > _doubles.length);

    double[] tmp = _doubles;
    int newLength = 2 * tmp.length;

    _doubles = new double[newLength];
    System.arraycopy(_doubles, 0, tmp, 0, tmp.length);

    long[] tmp1 = _longs;
    _longs = new long[newSize];
    System.arraycopy(_longs, 0, tmp1, 0, tmp1.length);
  }

  @Override
  public void copy(int position, DoubleArray that, int start, int end) {
    throw new RuntimeException("Unsupported method 'copy' from DoubleArray for class " + getClass().getName());
  }

  /**
   * {@inheritDoc}
   *
   * @param position
   * @param that
   * @param start
   * @param end
   */
  @Override
  public void copy(int position, Pair[] that, int start, int end) {
    Preconditions.checkArgument((_doubles.length - position) >= (end - start));
    Preconditions.checkState(_doubles.length == _longs.length);

    for (int i = start; i < end; i++) {
      _doubles[position] = (double) that[i].getFirst();
      _longs[position++] = (long) that[i].getSecond();
    }
  }
}
