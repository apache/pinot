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
package org.apache.pinot.common.utils;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;


/// Utility class for [LongArrayList] and [DoubleArrayList].
public class ArrayListUtils {
  private ArrayListUtils() {
  }

  /// Best effort extract the given [LongArrayList] to a long array without copying the elements.
  /// The [LongArrayList#elements()] returned long array may be longer than the actual size of the
  /// [LongArrayList], and the actual size of the [LongArrayList] can be retrieved using
  /// [LongArrayList#size()].
  /// This method checks the length of the returned long array and returns the same if it is equal to the size of the
  /// [LongArrayList], otherwise, it copies the elements to a new long array and returns it.
  ///
  /// Use this method only if you are sure that the returned long array will not be modified.
  ///
  /// Otherwise, use [LongArrayList#toLongArray()].
  ///
  /// @param longArrayList Input [LongArrayList]
  /// @return Best effort extracted long array without copying the elements
  public static long[] toLongArray(LongArrayList longArrayList) {
    long[] longArrayListElements = longArrayList.elements();
    return longArrayListElements.length == longArrayList.size() ? longArrayListElements : longArrayList.toLongArray();
  }

  /// Best effort extract the given [DoubleArrayList] to a double array without copying the elements.
  /// The [DoubleArrayList#elements()] returned double array may be longer than the actual size of the
  /// [DoubleArrayList], and the actual size of the [DoubleArrayList] can be retrieved using
  /// [DoubleArrayList#size()].
  /// This method checks the length of the returned double array and returns the same if it is equal to the size of the
  /// [DoubleArrayList], otherwise, it copies the elements to a new double array and returns it.
  ///
  /// Use this method only if you are sure that the returned double array will not be modified.
  ///
  /// Otherwise, use [DoubleArrayList#toDoubleArray()].
  ///
  /// @param doubleArrayList Input [DoubleArrayList]
  /// @return Best effort extracted double array without copying the elements
  public static double[] toDoubleArray(DoubleArrayList doubleArrayList) {
    double[] doubleArrayListElements = doubleArrayList.elements();
    return doubleArrayListElements.length == doubleArrayList.size() ? doubleArrayListElements
        : doubleArrayList.toDoubleArray();
  }
}
