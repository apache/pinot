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
 * Interface for modelling array storage of AggregationGroupBy result.
 * Allows for primitive double and Pair values.
 */
public interface ResultArray {

  /**
   * Set passed in value at array index
   *
   * @param index
   * @param value
   */
  void set(int index, double value);

  /**
   * Set the pair value at index, (for result arrays that are pair).
   *
   * @param pair
   * @param index
   */
  void set(Pair pair, int index);

  /**
   * Set all values in the array
   *
   * @param value
   */
  void setAll(double value);

  /**
   * Set all values in the array from ResultType object.
   */
  void setAll(Pair resultType);

  /**
   * Returns the double (primitive) value at array index.
   *
   * @param index
   * @return
   */
  double getDoubleResult(int index);

  /**
   * Get the value (of type ResultType) at index, (for result arrays that are pair).
   *
   * @param index
   * @return
   */
  Pair getResult(int index);

  /**
   * Return the current size of the array.
   * @return
   */
  int size();

  /**
   * Expand the array to its new size.
   */
  void expand(int newSize);

  /**
   * Copies the passed in array from start to end index.
   * Assumes that constructor was called with enough capacity to be
   * able to accommodate the provided array.
   *
   * @param position
   * @param that
   * @param start
   * @param end
   */
  void copy(int position, DoubleResultArray that, int start, int end);

  /**
   * Copies the array of ResultType into the current object.
   * Assumes that constructor was called with enough capacity to be abel to
   * accommodate the provided array.
   *
   * @param position
   * @param that
   * @param start
   * @param end
   */
  void copy(int position, Pair[] that, int start, int end);
}
