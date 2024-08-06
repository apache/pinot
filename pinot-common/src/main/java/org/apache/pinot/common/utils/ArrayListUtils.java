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


/**
 * Utility class for {@link IntArrayList}, {@link LongArrayList}, {@link FloatArrayList}, {@link DoubleArrayList},
 * {@link ObjectArrayList}.
 */
public class ArrayListUtils {
  private ArrayListUtils() {
  }

  /**
   * Best effort extract the given {@link IntArrayList} to an int array without copying the elements.
   * The {@link IntArrayList#elements()} returned int array may be longer than the actual size of the
   * {@link IntArrayList}, and the actual size of the {@link IntArrayList} can be retrieved using
   * {@link IntArrayList#size()}.
   * This method checks the length of the returned int array and returns the same if it is equal to the size of the
   * {@link IntArrayList}, otherwise, it copies the elements to a new int array and returns it.
   *
   * <p>Use this method only if you are sure that the returned int array will not be modified.
   * <p>Otherwise, use {@link IntArrayList#toIntArray()}.
   *
   * @param intArrayList Input {@link IntArrayList}
   * @return Best effort extracted int array without copying the elements
   */
  public static int[] toIntArray(IntArrayList intArrayList) {
    int[] intArrayListElements = intArrayList.elements();
    return intArrayListElements.length == intArrayList.size() ? intArrayListElements : intArrayList.toIntArray();
  }

  /**
   * Best effort extract the given {@link LongArrayList} to a long array without copying the elements.
   * The {@link LongArrayList#elements()} returned long array may be longer than the actual size of the
   * {@link LongArrayList}, and the actual size of the {@link LongArrayList} can be retrieved using
   * {@link LongArrayList#size()}.
   * This method checks the length of the returned long array and returns the same if it is equal to the size of the
   * {@link LongArrayList}, otherwise, it copies the elements to a new long array and returns it.
   *
   * <p>Use this method only if you are sure that the returned long array will not be modified.
   * <p>Otherwise, use {@link LongArrayList#toLongArray()}.
   *
   * @param longArrayList Input {@link LongArrayList}
   * @return Best effort extracted long array without copying the elements
   */
  public static long[] toLongArray(LongArrayList longArrayList) {
    long[] longArrayListElements = longArrayList.elements();
    return longArrayListElements.length == longArrayList.size() ? longArrayListElements : longArrayList.toLongArray();
  }

  /**
   * Best effort extract the given {@link FloatArrayList} to a float array without copying the elements.
   * The {@link FloatArrayList#elements()} returned float array may be longer than the actual size of the
   * {@link FloatArrayList}, and the actual size of the {@link FloatArrayList} can be retrieved using
   * {@link FloatArrayList#size()}.
   * This method checks the length of the returned float array and returns the same if it is equal to the size of the
   * {@link FloatArrayList}, otherwise, it copies the elements to a new float array and returns it.
   *
   * <p>Use this method only if you are sure that the returned float array will not be modified.
   * <p>Otherwise, use {@link FloatArrayList#toFloatArray()}.
   *
   * @param floatArrayList Input {@link FloatArrayList}
   * @return Best effort extracted float array without copying the elements
   */
  public static float[] toFloatArray(FloatArrayList floatArrayList) {
    float[] floatArrayListElements = floatArrayList.elements();
    return floatArrayListElements.length == floatArrayList.size() ? floatArrayListElements
        : floatArrayList.toFloatArray();
  }

  /**
   * Best effort extract the given {@link DoubleArrayList} to a double array without copying the elements.
   * The {@link DoubleArrayList#elements()} returned double array may be longer than the actual size of the
   * {@link DoubleArrayList}, and the actual size of the {@link DoubleArrayList} can be retrieved using
   * {@link DoubleArrayList#size()}.
   * This method checks the length of the returned double array and returns the same if it is equal to the size of the
   * {@link DoubleArrayList}, otherwise, it copies the elements to a new double array and returns it.
   *
   * <p>Use this method only if you are sure that the returned double array will not be modified.
   * <p>Otherwise, use {@link DoubleArrayList#toDoubleArray()}.
   *
   * @param doubleArrayList Input {@link DoubleArrayList}
   * @return Best effort extracted double array without copying the elements
   */
  public static double[] toDoubleArray(DoubleArrayList doubleArrayList) {
    double[] doubleArrayListElements = doubleArrayList.elements();
    return doubleArrayListElements.length == doubleArrayList.size() ? doubleArrayListElements
        : doubleArrayList.toDoubleArray();
  }

  /**
   * Convert the given {@link ObjectArrayList} to a string array.
   * The method {@link ObjectArrayList#elements()} could return either Object[] or String[]. The casting to String[]
   * is not guaranteed to work, and it may throw {@link ClassCastException} if the internal object is not a String
   * array.
   * <p>
   *   This method first get `elements` as Object, then check if it's instance of String[].
   *   Only return the reference when the internal object is a String array and the length equals to ObjectArrayList
   *   size.
   *   For all the other scenarios, just copy the elements to a new string array and returns it.
   * <p>
   *
   * @param stringArrayList Input {@link ObjectArrayList}
   * @return Copied string array
   */
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
}
