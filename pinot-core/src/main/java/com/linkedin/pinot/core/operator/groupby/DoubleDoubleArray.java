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
import com.linkedin.pinot.core.query.aggregation.function.MinMaxRangeAggregationFunction;
import com.linkedin.pinot.core.query.utils.Pair;
import java.util.Arrays;


public class DoubleDoubleArray implements ResultArray {
  private double[] _doubles1;
  private double[] _doubles2;

  /**
   * Constructor for the class.
   *
   * @param capacity
   * @param valuePair
   */
  public DoubleDoubleArray(int capacity, Pair<Double, Double> valuePair) {
    _doubles1 = new double[capacity];
    _doubles2 = new double[capacity];
    setAll(valuePair);
  }

  /**
   * {@inheritDoc}
   *
   * @param valuePair
   * @param index
   */
  @Override
  public void set(Pair valuePair, int index) {
    _doubles1[index] = (double) valuePair.getFirst();
    _doubles2[index] = (double) valuePair.getSecond();
  }

  @Override
  public void set(int index, double value) {
    throw new RuntimeException("Unsupported method set(int, double) for class " + getClass().getName());
  }

  @Override
  public void setAll(double value) {
    throw new RuntimeException("Unsupported method setAll(double value) for class " + getClass().getName());
  }

  /**
   * {@inheritDoc}
   *
   * @param doubleDoublePair
   */
  @Override
  public void setAll(Pair doubleDoublePair) {
    Arrays.fill(_doubles1, (double) doubleDoublePair.getFirst());
    Arrays.fill(_doubles2, (double) doubleDoublePair.getSecond());
  }

  @Override
  public double getDoubleResult(int index) {
    throw new RuntimeException("Method getDoubleResult not supported for class " + getClass().getName());
  }

  /**
   * {@inheritDoc}
   *
   * @param index
   * @return
   */
  @Override
  public MinMaxRangeAggregationFunction.MinMaxRangePair getResult(int index) {
    return new MinMaxRangeAggregationFunction.MinMaxRangePair(_doubles1[index], _doubles2[index]);
  }

  @Override
  public int size() {
    return _doubles1.length;
  }

  @Override
  public void expand(int newSize) {
    _doubles1 = expandArray(_doubles1, newSize);
    _doubles2 = expandArray(_doubles2, newSize);

  }

  /**
   * Helper method to expand the provided array.
   * Returns a new array with double the capacity, and values
   * copied from the original array.
   *
   * @param doubles
   */
  private double[] expandArray(double[] doubles, int newSize) {
    Preconditions.checkArgument(newSize > doubles.length);

    double[] expanded = new double[newSize];
    System.arraycopy(expanded, 0, doubles, 0, doubles.length);
    return expanded;
  }

  @Override
  public void copy(int position, DoubleArray that, int start, int end) {
    throw new RuntimeException("Unsupported method copy from DoubleArray for class " + getClass().getName());
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
    Preconditions.checkArgument((_doubles1.length - position) >= (end - start));
    Preconditions.checkState(_doubles1.length == _doubles1.length);

    for (int i = start; i < end; i++) {
      _doubles1[position] = (double) that[i].getFirst();
      _doubles2[position++] = (double) that[i].getSecond();
    }
  }
}
