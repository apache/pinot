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
import it.unimi.dsi.fastutil.floats.FloatArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.math.BigDecimal;
import org.apache.pinot.spi.utils.ByteArray;


/// Utility class for [IntArrayList], [LongArrayList], [FloatArrayList], [DoubleArrayList],
/// [ObjectArrayList].
public class ArrayListUtils {
  private ArrayListUtils() {
  }

  /// Best effort extract the given [IntArrayList] to an int array without copying the elements.
  /// The [IntArrayList#elements()] returned int array may be longer than the actual size of the
  /// [IntArrayList], and the actual size of the [IntArrayList] can be retrieved using
  /// [IntArrayList#size()].
  /// This method checks the length of the returned int array and returns the same if it is equal to the size of the
  /// [IntArrayList], otherwise, it copies the elements to a new int array and returns it.
  ///
  /// Use this method only if you are sure that the returned int array will not be modified.
  ///
  /// Otherwise, use [IntArrayList#toIntArray()].
  ///
  /// @param intArrayList Input [IntArrayList]
  /// @return Best effort extracted int array without copying the elements
  public static int[] toIntArray(IntArrayList intArrayList) {
    int[] intArrayListElements = intArrayList.elements();
    return intArrayListElements.length == intArrayList.size() ? intArrayListElements : intArrayList.toIntArray();
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

  /// Best effort extract the given [FloatArrayList] to a float array without copying the elements.
  /// The [FloatArrayList#elements()] returned float array may be longer than the actual size of the
  /// [FloatArrayList], and the actual size of the [FloatArrayList] can be retrieved using
  /// [FloatArrayList#size()].
  /// This method checks the length of the returned float array and returns the same if it is equal to the size of the
  /// [FloatArrayList], otherwise, it copies the elements to a new float array and returns it.
  ///
  /// Use this method only if you are sure that the returned float array will not be modified.
  ///
  /// Otherwise, use [FloatArrayList#toFloatArray()].
  ///
  /// @param floatArrayList Input [FloatArrayList]
  /// @return Best effort extracted float array without copying the elements
  public static float[] toFloatArray(FloatArrayList floatArrayList) {
    float[] floatArrayListElements = floatArrayList.elements();
    return floatArrayListElements.length == floatArrayList.size() ? floatArrayListElements
        : floatArrayList.toFloatArray();
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

  /// Convert the given [ObjectArrayList] to a BigDecimal array. Mirrors [#toStringArray]: returns the
  /// backing array reference when possible, otherwise copies elements into a new array.
  public static BigDecimal[] toBigDecimalArray(ObjectArrayList<BigDecimal> bigDecimalArrayList) {
    Object elements = bigDecimalArrayList.elements();
    if (elements instanceof BigDecimal[]) {
      BigDecimal[] bigDecimalArrayListElements = (BigDecimal[]) elements;
      if (bigDecimalArrayListElements.length == bigDecimalArrayList.size()) {
        return bigDecimalArrayListElements;
      }
    }
    return bigDecimalArrayList.toArray(new BigDecimal[0]);
  }

  /// Convert the given [ObjectArrayList] to a string array.
  /// The method [ObjectArrayList#elements()] could return either Object\[\] or String\[\]. The casting to String\[\]
  /// is not guaranteed to work, and it may throw [ClassCastException] if the internal object is not a String
  /// array.
  ///
  ///   This method first get `elements` as Object, then check if it's instance of String\[\].
  ///   Only return the reference when the internal object is a String array and the length equals to ObjectArrayList
  ///   size.
  ///   For all the other scenarios, just copy the elements to a new string array and returns it.
  ///
  /// @param stringArrayList Input [ObjectArrayList]
  /// @return Copied string array
  public static String[] toStringArray(ObjectArrayList<String> stringArrayList) {
    Object elements = stringArrayList.elements();
    if (elements instanceof String[]) {
      String[] stringArrayListElements = (String[]) elements;
      if (stringArrayListElements.length == stringArrayList.size()) {
        return stringArrayListElements;
      }
    }
    return stringArrayList.toArray(new String[0]);
  }

  /// Convert the given [ObjectArrayList] to a ByteArray array. Mirrors [#toStringArray]: returns the
  /// backing array reference when possible, otherwise copies elements into a new array.
  public static ByteArray[] toBytesArray(ObjectArrayList<ByteArray> bytesArrayList) {
    Object elements = bytesArrayList.elements();
    if (elements instanceof ByteArray[]) {
      ByteArray[] bytesArrayListElements = (ByteArray[]) elements;
      if (bytesArrayListElements.length == bytesArrayList.size()) {
        return bytesArrayListElements;
      }
    }
    return bytesArrayList.toArray(new ByteArray[0]);
  }
}
