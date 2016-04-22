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
import com.linkedin.pinot.core.query.utils.Pair;
import java.util.Arrays;


/**
 * Array of primitive doubles.
 */
public class DoubleResultArray implements ResultArray {
  double[] _doubles;
  private final double _defaultValue;

  /**
   * Constructor for the class.
   *
   * @param length
   * @param defaultValue
   */
  public DoubleResultArray(int length, double defaultValue) {
    _doubles = new double[length];
    _defaultValue = defaultValue;
    setAll(defaultValue);
  }

  /**
   * {@inheritDoc}
   *
   * @param index
   * @param value
   */
  @Override
  public void set(int index, double value) {
    _doubles[index] = value;
  }

  @Override
  public void set(int index, Pair pair) {
    throw new RuntimeException("Unsupported method set(Object, index) for class: " + getClass().getName());
  }

  /**
   * {@inheritDoc}
   *
   * @param value
   */
  @Override
  public void setAll(double value) {
    Arrays.fill(_doubles, value);
  }

  @Override
  public void setAll(Pair pair) {
    throw new RuntimeException("Unsupported method setAll(Object) for class " + getClass().getName());
  }

  /**
   * {@inheritDoc}
   *
   * @param index
   * @return
   */
  @Override
  public double getDoubleResult(int index) {
    return _doubles[index];
  }

  @Override
  public Pair getResult(int index) {
    throw new RuntimeException("Unsupported method getResult (returning Object) for class " + getClass().getName());
  }

  /**
   * {@inheritDoc}
   *
   * @return
   */
  @Override
  public int size() {
    return _doubles.length;
  }

  @Override
  public void expand(int newSize) {
    Preconditions.checkArgument(newSize > _doubles.length);

    double[] tmp = _doubles;
    _doubles = new double[newSize];
    System.arraycopy(tmp, 0, _doubles, 0, tmp.length);
    Arrays.fill(_doubles, tmp.length, newSize, _defaultValue);
  }

  /**
   * {@inheritDoc}
   *
   * @param that
   * @param start
   * @param end
   */
  @Override
  public void copy(int position, DoubleResultArray that, int start, int end) {
    Preconditions.checkArgument((_doubles.length - position) >= (end - start));
    System.arraycopy(_doubles, position, that._doubles, start, end);
  }

  @Override
  public void copy(int position, Pair[] that, int start, int end) {
    throw new RuntimeException("Unsupported method 'copy' from Pair[] for class " + getClass().getName());
  }
}
