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
package org.apache.pinot.common.function.scalar;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntLinkedOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.objects.ObjectLinkedOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectSet;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.pinot.spi.annotations.ScalarFunction;
import org.apache.pinot.spi.utils.CommonConstants.NullValuePlaceHolder;


/**
 * Inbuilt array scalar functions. See {@link ArrayUtils} for details.
 */
public class ArrayFunctions {
  private ArrayFunctions() {
  }

  @ScalarFunction
  public static int[] arrayReverseInt(int[] values) {
    int[] clone = values.clone();
    ArrayUtils.reverse(clone);
    return clone;
  }

  @ScalarFunction
  public static String[] arrayReverseString(String[] values) {
    String[] clone = values.clone();
    ArrayUtils.reverse(clone);
    return clone;
  }

  @ScalarFunction
  public static int[] arraySortInt(int[] values) {
    int[] clone = values.clone();
    Arrays.sort(clone);
    return clone;
  }

  @ScalarFunction
  public static String[] arraySortString(String[] values) {
    String[] clone = values.clone();
    Arrays.sort(clone);
    return clone;
  }

  @ScalarFunction
  public static int arrayIndexOfInt(int[] values, int valueToFind) {
    return ArrayUtils.indexOf(values, valueToFind);
  }

  @ScalarFunction
  public static int arrayIndexOfString(String[] values, String valueToFind) {
    return ArrayUtils.indexOf(values, valueToFind);
  }

  @ScalarFunction
  public static int[] arrayIndexesOfInt(int[] value, int valueToFind) {
    return ArrayUtils.indexesOf(value, valueToFind).stream().toArray();
  }

  @ScalarFunction
  public static int[] arrayIndexesOfLong(long[] value, long valueToFind) {
    return ArrayUtils.indexesOf(value, valueToFind).stream().toArray();
  }

  @ScalarFunction
  public static int[] arrayIndexesOfFloat(float[] value, float valueToFind) {
    return ArrayUtils.indexesOf(value, valueToFind).stream().toArray();
  }

  @ScalarFunction
  public static int[] arrayIndexesOfDouble(double[] value, double valueToFind) {
    return ArrayUtils.indexesOf(value, valueToFind).stream().toArray();
  }

  @ScalarFunction
  public static int[] arrayIndexesOfString(String[] value, String valueToFind) {
    return ArrayUtils.indexesOf(value, valueToFind).stream().toArray();
  }

  /**
   * Assume values1, and values2 are monotonous increasing indices of MV cols.
   * Here is the common usage:
   * col1: ["a", "b", "a", "b"]
   * col2: ["c", "d", "d", "c"]
   * The user want to get the first index called idx, s.t. col1[idx] == "b" && col2[idx] == "d"
   * arrayElementAtInt(0, intersectIndices(arrayIndexOfAllString(col1, "b"), arrayIndexOfAllString(col2, "d")))
   */
  @ScalarFunction
  public static int[] intersectIndices(int[] values1, int[] values2) {
    // TODO: if values1.length << values2.length. Use binary search can speed up the query
    int i = 0;
    int j = 0;
    IntArrayList indices = new IntArrayList();
    while (i < values1.length && j < values2.length) {
      if (values1[i] == values2[j]) {
        indices.add(values1[i]);
        j++;
      }
      i++;
    }
    return indices.toIntArray();
  }

  @ScalarFunction
  public static boolean arrayContainsInt(int[] values, int valueToFind) {
    return ArrayUtils.contains(values, valueToFind);
  }

  @ScalarFunction
  public static boolean arrayContainsString(String[] values, String valueToFind) {
    return ArrayUtils.contains(values, valueToFind);
  }

  @ScalarFunction
  public static int[] arraySliceInt(int[] values, int start, int end) {
    return Arrays.copyOfRange(values, start, end);
  }

  @ScalarFunction
  public static String[] arraySliceString(String[] values, int start, int end) {
    return Arrays.copyOfRange(values, start, end);
  }

  @ScalarFunction
  public static int[] arrayDistinctInt(int[] values) {
    return new IntLinkedOpenHashSet(values).toIntArray();
  }

  @ScalarFunction
  public static String[] arrayDistinctString(String[] values) {
    return new ObjectLinkedOpenHashSet<>(values).toArray(new String[0]);
  }

  @ScalarFunction
  public static int[] arrayRemoveInt(int[] values, int element) {
    return ArrayUtils.removeElement(values, element);
  }

  @ScalarFunction
  public static String[] arrayRemoveString(String[] values, String element) {
    return ArrayUtils.removeElement(values, element);
  }

  @ScalarFunction
  public static int[] arrayUnionInt(int[] values1, int[] values2) {
    IntSet set = new IntLinkedOpenHashSet(values1);
    set.addAll(IntArrayList.wrap(values2));
    return set.toIntArray();
  }

  @ScalarFunction
  public static String[] arrayUnionString(String[] values1, String[] values2) {
    ObjectSet<String> set = new ObjectLinkedOpenHashSet<>(values1);
    set.addAll(Arrays.asList(values2));
    return set.toArray(new String[0]);
  }

  @ScalarFunction
  public static int[] arrayConcatInt(int[] values1, int[] values2) {
    return ArrayUtils.addAll(values1, values2);
  }

  @ScalarFunction
  public static long[] arrayConcatLong(long[] values1, long[] values2) {
    return ArrayUtils.addAll(values1, values2);
  }

  @ScalarFunction
  public static float[] arrayConcatFloat(float[] values1, float[] values2) {
    return ArrayUtils.addAll(values1, values2);
  }

  @ScalarFunction
  public static double[] arrayConcatDouble(double[] values1, double[] values2) {
    return ArrayUtils.addAll(values1, values2);
  }

  @ScalarFunction
  public static String[] arrayConcatString(String[] values1, String[] values2) {
    return ArrayUtils.addAll(values1, values2);
  }

  @ScalarFunction
  public static int[] arrayPushBackInt(int[] values, int element) {
    return ArrayUtils.add(values, element);
  }

  @ScalarFunction
  public static long[] arrayPushBackLong(long[] values, long element) {
    return ArrayUtils.add(values, element);
  }

  @ScalarFunction
  public static float[] arrayPushBackFloat(float[] values, float element) {
    return ArrayUtils.add(values, element);
  }

  @ScalarFunction
  public static double[] arrayPushBackDouble(double[] values, double element) {
    return ArrayUtils.add(values, element);
  }

  @ScalarFunction
  public static String[] arrayPushBackString(String[] values, String element) {
    return ArrayUtils.add(values, element);
  }

  @ScalarFunction
  public static int[] arrayPushFrontInt(int[] values, int element) {
    return ArrayUtils.insert(0, values, element);
  }

  @ScalarFunction
  public static long[] arrayPushFrontLong(long[] values, long element) {
    return ArrayUtils.insert(0, values, element);
  }

  @ScalarFunction
  public static float[] arrayPushFrontFloat(float[] values, float element) {
    return ArrayUtils.insert(0, values, element);
  }

  @ScalarFunction
  public static double[] arrayPushFrontDouble(double[] values, double element) {
    return ArrayUtils.insert(0, values, element);
  }

  @ScalarFunction
  public static String[] arrayPushFrontString(String[] values, String element) {
    return ArrayUtils.insert(0, values, element);
  }

  @ScalarFunction
  public static int arrayElementAtInt(int[] arr, int idx) {
    return idx > 0 && idx <= arr.length ? arr[idx - 1] : NullValuePlaceHolder.INT;
  }

  @ScalarFunction
  public static long arrayElementAtLong(long[] arr, int idx) {
    return idx > 0 && idx <= arr.length ? arr[idx - 1] : NullValuePlaceHolder.LONG;
  }

  @ScalarFunction
  public static float arrayElementAtFloat(float[] arr, int idx) {
    return idx > 0 && idx <= arr.length ? arr[idx - 1] : NullValuePlaceHolder.FLOAT;
  }

  @ScalarFunction
  public static double arrayElementAtDouble(double[] arr, int idx) {
    return idx > 0 && idx <= arr.length ? arr[idx - 1] : NullValuePlaceHolder.DOUBLE;
  }

  @ScalarFunction
  public static String arrayElementAtString(String[] arr, int idx) {
    return idx > 0 && idx <= arr.length ? arr[idx - 1] : NullValuePlaceHolder.STRING;
  }

  @ScalarFunction
  public static int arraySumInt(int[] arr) {
    int sum = 0;
    for (int value : arr) {
      sum += value;
    }
    return sum;
  }

  @ScalarFunction
  public static long arraySumLong(long[] arr) {
    long sum = 0;
    for (long value : arr) {
      sum += value;
    }
    return sum;
  }

  @ScalarFunction(names = {"array", "arrayValueConstructor"}, isVarArg = true)
  public static Object arrayValueConstructor(Object... arr) {
    if (arr == null || arr.length == 0) {
      return arr;
    }
    Class<?> clazz = arr[0].getClass();
    if (clazz == Integer.class) {
      int[] intArr = new int[arr.length];
      for (int i = 0; i < arr.length; i++) {
        intArr[i] = (Integer) arr[i];
      }
      return intArr;
    }
    if (clazz == Long.class) {
      long[] longArr = new long[arr.length];
      for (int i = 0; i < arr.length; i++) {
        longArr[i] = (Long) arr[i];
      }
      return longArr;
    }
    if (clazz == Float.class) {
      float[] floatArr = new float[arr.length];
      for (int i = 0; i < arr.length; i++) {
        floatArr[i] = (Float) arr[i];
      }
      return floatArr;
    }
    if (clazz == Double.class) {
      double[] doubleArr = new double[arr.length];
      for (int i = 0; i < arr.length; i++) {
        doubleArr[i] = (Double) arr[i];
      }
      return doubleArr;
    }
    if (clazz == Boolean.class) {
      boolean[] boolArr = new boolean[arr.length];
      for (int i = 0; i < arr.length; i++) {
        boolArr[i] = (Boolean) arr[i];
      }
      return boolArr;
    }
    if (clazz == BigDecimal.class) {
      BigDecimal[] bigDecimalArr = new BigDecimal[arr.length];
      for (int i = 0; i < arr.length; i++) {
        bigDecimalArr[i] = (BigDecimal) arr[i];
      }
      return bigDecimalArr;
    }
    if (clazz == String.class) {
      String[] strArr = new String[arr.length];
      for (int i = 0; i < arr.length; i++) {
        strArr[i] = (String) arr[i];
      }
      return strArr;
    }
    return arr;
  }

  @ScalarFunction
  public static int[] generateIntArray(int start, int end, int inc) {
    int size = (end - start) / inc + 1;
    int[] arr = new int[size];

    for (int i = 0, value = start; i < size; i++, value += inc) {
      arr[i] = value;
    }
    return arr;
  }

  @ScalarFunction
  public static long[] generateLongArray(long start, long end, long inc) {
    int size = (int) ((end - start) / inc + 1);
    long[] arr = new long[size];

    for (int i = 0; i < size; i++, start += inc) {
      arr[i] = start;
    }
    return arr;
  }

  @ScalarFunction
  public static float[] generateFloatArray(float start, float end, float inc) {
    int size = (int) ((end - start) / inc + 1);
    float[] arr = new float[size];

    for (int i = 0; i < size; i++, start += inc) {
      arr[i] = start;
    }
    return arr;
  }

  @ScalarFunction
  public static double[] generateDoubleArray(double start, double end, double inc) {
    int size = (int) ((end - start) / inc + 1);
    double[] arr = new double[size];

    for (int i = 0; i < size; i++, start += inc) {
      arr[i] = start;
    }
    return arr;
  }

  @ScalarFunction
  public static String arrayToString(String[] values, String delimiter) {
    return String.join(delimiter, values);
  }

  @ScalarFunction
  public static String arrayToString(String[] values, String delimiter, String nullString) {
    if (values == null || values.length == 0) {
      return NullValuePlaceHolder.STRING;
    }

    return String.join(
        delimiter,
        Arrays.stream(values)
            .map(s -> s == null || s.equals(NullValuePlaceHolder.STRING) ? nullString : s)
            .toArray(String[]::new));
  }

  /**
   * Returns the first element of an array. If the array is empty, returns null.
   * This is safer than using subscript operator which would fail on empty arrays.
   */
  @ScalarFunction
  public static int arrayFirstInt(int[] arr) {
    return arr == null || arr.length == 0 ? NullValuePlaceHolder.INT : arr[0];
  }

  @ScalarFunction
  public static long arrayFirstLong(long[] arr) {
    return arr == null || arr.length == 0 ? NullValuePlaceHolder.LONG : arr[0];
  }

  @ScalarFunction
  public static float arrayFirstFloat(float[] arr) {
    return arr == null || arr.length == 0 ? NullValuePlaceHolder.FLOAT : arr[0];
  }

  @ScalarFunction
  public static double arrayFirstDouble(double[] arr) {
    return arr == null || arr.length == 0 ? NullValuePlaceHolder.DOUBLE : arr[0];
  }

  @ScalarFunction
  public static String arrayFirstString(String[] arr) {
    return arr == null || arr.length == 0 ? NullValuePlaceHolder.STRING : arr[0];
  }

  /**
   * Returns the last element of an array. If the array is empty, returns null.
   * This is safer than using subscript operator which would fail on empty arrays.
   */
  @ScalarFunction
  public static int arrayLastInt(int[] arr) {
    return arr == null || arr.length == 0 ? NullValuePlaceHolder.INT : arr[arr.length - 1];
  }

  @ScalarFunction
  public static long arrayLastLong(long[] arr) {
    return arr == null || arr.length == 0 ? NullValuePlaceHolder.LONG : arr[arr.length - 1];
  }

  @ScalarFunction
  public static float arrayLastFloat(float[] arr) {
    return arr == null || arr.length == 0 ? NullValuePlaceHolder.FLOAT : arr[arr.length - 1];
  }

  @ScalarFunction
  public static double arrayLastDouble(double[] arr) {
    return arr == null || arr.length == 0 ? NullValuePlaceHolder.DOUBLE : arr[arr.length - 1];
  }

  @ScalarFunction
  public static String arrayLastString(String[] arr) {
    return arr == null || arr.length == 0 ? NullValuePlaceHolder.STRING : arr[arr.length - 1];
  }

  /**
   * Returns the position of the first occurrence of the element in array (1-based indexing).
   * Returns 0 if not found. This follows Trino's array_position function behavior.
   */
  @ScalarFunction
  public static long arrayPositionInt(int[] arr, int element) {
    if (arr == null) {
      return 0;
    }
    for (int i = 0; i < arr.length; i++) {
      if (arr[i] == element) {
        return i + 1; // 1-based indexing
      }
    }
    return 0;
  }

  @ScalarFunction
  public static long arrayPositionLong(long[] arr, long element) {
    if (arr == null) {
      return 0;
    }
    for (int i = 0; i < arr.length; i++) {
      if (arr[i] == element) {
        return i + 1; // 1-based indexing
      }
    }
    return 0;
  }

  @ScalarFunction
  public static long arrayPositionFloat(float[] arr, float element) {
    if (arr == null) {
      return 0;
    }
    for (int i = 0; i < arr.length; i++) {
      if (Float.compare(arr[i], element) == 0) {
        return i + 1; // 1-based indexing
      }
    }
    return 0;
  }

  @ScalarFunction
  public static long arrayPositionDouble(double[] arr, double element) {
    if (arr == null) {
      return 0;
    }
    for (int i = 0; i < arr.length; i++) {
      if (Double.compare(arr[i], element) == 0) {
        return i + 1; // 1-based indexing
      }
    }
    return 0;
  }

  @ScalarFunction
  public static long arrayPositionString(String[] arr, String element) {
    if (arr == null) {
      return 0;
    }
    for (int i = 0; i < arr.length; i++) {
      if (Objects.equals(arr[i], element)) {
        return i + 1; // 1-based indexing
      }
    }
    return 0;
  }

  /**
   * Returns the maximum value in the array.
   */
  @ScalarFunction
  public static int arrayMaxInt(int[] arr) {
    if (arr == null || arr.length == 0) {
      return NullValuePlaceHolder.INT;
    }
    int max = arr[0];
    for (int i = 1; i < arr.length; i++) {
      if (arr[i] > max) {
        max = arr[i];
      }
    }
    return max;
  }

  @ScalarFunction
  public static long arrayMaxLong(long[] arr) {
    if (arr == null || arr.length == 0) {
      return NullValuePlaceHolder.LONG;
    }
    long max = arr[0];
    for (int i = 1; i < arr.length; i++) {
      if (arr[i] > max) {
        max = arr[i];
      }
    }
    return max;
  }

  @ScalarFunction
  public static float arrayMaxFloat(float[] arr) {
    if (arr == null || arr.length == 0) {
      return NullValuePlaceHolder.FLOAT;
    }
    float max = arr[0];
    for (int i = 1; i < arr.length; i++) {
      if (Float.compare(arr[i], max) > 0) {
        max = arr[i];
      }
    }
    return max;
  }

  @ScalarFunction
  public static double arrayMaxDouble(double[] arr) {
    if (arr == null || arr.length == 0) {
      return NullValuePlaceHolder.DOUBLE;
    }
    double max = arr[0];
    for (int i = 1; i < arr.length; i++) {
      if (Double.compare(arr[i], max) > 0) {
        max = arr[i];
      }
    }
    return max;
  }

  @ScalarFunction
  public static String arrayMaxString(String[] arr) {
    if (arr == null || arr.length == 0) {
      return NullValuePlaceHolder.STRING;
    }
    String max = arr[0];
    for (int i = 1; i < arr.length; i++) {
      if (arr[i] != null && (max == null || arr[i].compareTo(max) > 0)) {
        max = arr[i];
      }
    }
    return max;
  }

  /**
   * Returns the minimum value in the array.
   */
  @ScalarFunction
  public static int arrayMinInt(int[] arr) {
    if (arr == null || arr.length == 0) {
      return NullValuePlaceHolder.INT;
    }
    int min = arr[0];
    for (int i = 1; i < arr.length; i++) {
      if (arr[i] < min) {
        min = arr[i];
      }
    }
    return min;
  }

  @ScalarFunction
  public static long arrayMinLong(long[] arr) {
    if (arr == null || arr.length == 0) {
      return NullValuePlaceHolder.LONG;
    }
    long min = arr[0];
    for (int i = 1; i < arr.length; i++) {
      if (arr[i] < min) {
        min = arr[i];
      }
    }
    return min;
  }

  @ScalarFunction
  public static float arrayMinFloat(float[] arr) {
    if (arr == null || arr.length == 0) {
      return NullValuePlaceHolder.FLOAT;
    }
    float min = arr[0];
    for (int i = 1; i < arr.length; i++) {
      if (Float.compare(arr[i], min) < 0) {
        min = arr[i];
      }
    }
    return min;
  }

  @ScalarFunction
  public static double arrayMinDouble(double[] arr) {
    if (arr == null || arr.length == 0) {
      return NullValuePlaceHolder.DOUBLE;
    }
    double min = arr[0];
    for (int i = 1; i < arr.length; i++) {
      if (Double.compare(arr[i], min) < 0) {
        min = arr[i];
      }
    }
    return min;
  }

  @ScalarFunction
  public static String arrayMinString(String[] arr) {
    if (arr == null || arr.length == 0) {
      return NullValuePlaceHolder.STRING;
    }
    String min = arr[0];
    for (int i = 1; i < arr.length; i++) {
      if (arr[i] != null && (min == null || arr[i].compareTo(min) < 0)) {
        min = arr[i];
      }
    }
    return min;
  }


  /**
   * Concatenates the elements of the given array using the delimiter. Null elements are omitted.
   * This follows Trino's array_join function behavior.
   */
  @ScalarFunction(names = {"arrayJoinInt", "arrayToStringInt"})
  public static String arrayJoinInt(int[] arr, String delimiter) {
    if (arr == null || arr.length == 0) {
      return NullValuePlaceHolder.STRING;
    }
    return Arrays.stream(arr)
        .filter(x -> x != NullValuePlaceHolder.INT)
        .mapToObj(String::valueOf)
        .collect(Collectors.joining(delimiter));
  }

  @ScalarFunction(names = {"arrayJoinLong", "arrayToStringLong"})
  public static String arrayJoinLong(long[] arr, String delimiter) {
    if (arr == null || arr.length == 0) {
      return NullValuePlaceHolder.STRING;
    }
    return Arrays.stream(arr)
        .filter(x -> x != NullValuePlaceHolder.LONG)
        .mapToObj(String::valueOf)
        .collect(Collectors.joining(delimiter));
  }

  @ScalarFunction(names = {"arrayJoinFloat", "arrayToStringFloat"})
  public static String arrayJoinFloat(float[] arr, String delimiter) {
    if (arr == null || arr.length == 0) {
      return NullValuePlaceHolder.STRING;
    }
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (float value : arr) {
      if (value != NullValuePlaceHolder.FLOAT) {
        if (!first) {
          sb.append(delimiter);
        }
        sb.append(value);
        first = false;
      }
    }
    return sb.toString();
  }

  @ScalarFunction(names = {"arrayJoinDouble", "arrayToStringDouble"})
  public static String arrayJoinDouble(double[] arr, String delimiter) {
    if (arr == null || arr.length == 0) {
      return NullValuePlaceHolder.STRING;
    }
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (double value : arr) {
      if (value != NullValuePlaceHolder.DOUBLE) {
        if (!first) {
          sb.append(delimiter);
        }
        sb.append(value);
        first = false;
      }
    }
    return sb.toString();
  }

  @ScalarFunction(names = {"arrayJoinString", "arrayToStringString"})
  public static String arrayJoinString(String[] arr, String delimiter) {
    if (arr == null || arr.length == 0) {
      return NullValuePlaceHolder.STRING;
    }
    return Arrays.stream(arr)
        .filter(s -> s != null && !s.equals(NullValuePlaceHolder.STRING))
        .collect(Collectors.joining(delimiter));
  }

  /**
   * Concatenates the elements of the given array using the delimiter and null replacement.
   * This is the overloaded version of array_join that handles nulls explicitly.
   */
  @ScalarFunction
  public static String arrayJoinInt(int[] arr, String delimiter, String nullReplacement) {
    if (arr == null || arr.length == 0) {
      return NullValuePlaceHolder.STRING;
    }
    return Arrays.stream(arr)
        .mapToObj(x -> x == NullValuePlaceHolder.INT ? nullReplacement : String.valueOf(x))
        .collect(Collectors.joining(delimiter));
  }

  @ScalarFunction
  public static String arrayJoinLong(long[] arr, String delimiter, String nullReplacement) {
    if (arr == null || arr.length == 0) {
      return NullValuePlaceHolder.STRING;
    }
    return Arrays.stream(arr)
        .mapToObj(x -> x == NullValuePlaceHolder.LONG ? nullReplacement : String.valueOf(x))
        .collect(Collectors.joining(delimiter));
  }

  @ScalarFunction
  public static String arrayJoinString(String[] arr, String delimiter, String nullReplacement) {
    if (arr == null || arr.length == 0) {
      return NullValuePlaceHolder.STRING;
    }
    return Arrays.stream(arr)
        .map(s -> s == null || s.equals(NullValuePlaceHolder.STRING) ? nullReplacement : s)
        .collect(Collectors.joining(delimiter));
  }

  /**
   * Standard contains function - same as arrayContains but with standard SQL name.
   */
  @ScalarFunction
  public static boolean containsInt(int[] arr, int element) {
    return arrayContainsInt(arr, element);
  }

  @ScalarFunction
  public static boolean containsLong(long[] arr, long element) {
    return ArrayUtils.contains(arr, element);
  }

  @ScalarFunction
  public static boolean containsFloat(float[] arr, float element) {
    return ArrayUtils.contains(arr, element);
  }

  @ScalarFunction
  public static boolean containsDouble(double[] arr, double element) {
    return ArrayUtils.contains(arr, element);
  }

  @ScalarFunction
  public static boolean containsString(String[] arr, String element) {
    return arrayContainsString(arr, element);
  }

  /**
   * Returns element of array at given index. If index > 0, provides same functionality as
   * subscript operator, but returns null for out-of-bounds instead of failing.
   * If index < 0, accesses elements from the last to the first.
   */
  @ScalarFunction(names = {"elementAtInt", "arrayElementAtInt"})
  public static int elementAtInt(int[] arr, int index) {
    if (arr == null || arr.length == 0) {
      return NullValuePlaceHolder.INT;
    }
    int actualIndex;
    if (index > 0) {
      actualIndex = index - 1; // Convert to 0-based
    } else if (index < 0) {
      actualIndex = arr.length + index; // Negative indexing from end
    } else {
      return NullValuePlaceHolder.INT; // index 0 is invalid
    }

    if (actualIndex < 0 || actualIndex >= arr.length) {
      return NullValuePlaceHolder.INT;
    }
    return arr[actualIndex];
  }

  @ScalarFunction(names = {"elementAtLong", "arrayElementAtLong"})
  public static long elementAtLong(long[] arr, int index) {
    if (arr == null || arr.length == 0) {
      return NullValuePlaceHolder.LONG;
    }
    int actualIndex;
    if (index > 0) {
      actualIndex = index - 1; // Convert to 0-based
    } else if (index < 0) {
      actualIndex = arr.length + index; // Negative indexing from end
    } else {
      return NullValuePlaceHolder.LONG; // index 0 is invalid
    }

    if (actualIndex < 0 || actualIndex >= arr.length) {
      return NullValuePlaceHolder.LONG;
    }
    return arr[actualIndex];
  }

  @ScalarFunction(names = {"elementAtFloat", "arrayElementAtFloat"})
  public static float elementAtFloat(float[] arr, int index) {
    if (arr == null || arr.length == 0) {
      return NullValuePlaceHolder.FLOAT;
    }
    int actualIndex;
    if (index > 0) {
      actualIndex = index - 1; // Convert to 0-based
    } else if (index < 0) {
      actualIndex = arr.length + index; // Negative indexing from end
    } else {
      return NullValuePlaceHolder.FLOAT; // index 0 is invalid
    }

    if (actualIndex < 0 || actualIndex >= arr.length) {
      return NullValuePlaceHolder.FLOAT;
    }
    return arr[actualIndex];
  }

  @ScalarFunction(names = {"elementAtDouble", "arrayElementAtDouble"})
  public static double elementAtDouble(double[] arr, int index) {
    if (arr == null || arr.length == 0) {
      return NullValuePlaceHolder.DOUBLE;
    }
    int actualIndex;
    if (index > 0) {
      actualIndex = index - 1; // Convert to 0-based
    } else if (index < 0) {
      actualIndex = arr.length + index; // Negative indexing from end
    } else {
      return NullValuePlaceHolder.DOUBLE; // index 0 is invalid
    }

    if (actualIndex < 0 || actualIndex >= arr.length) {
      return NullValuePlaceHolder.DOUBLE;
    }
    return arr[actualIndex];
  }

  @ScalarFunction(names = {"elementAtString", "arrayElementAtString"})
  public static String elementAtString(String[] arr, int index) {
    if (arr == null || arr.length == 0) {
      return NullValuePlaceHolder.STRING;
    }
    int actualIndex;
    if (index > 0) {
      actualIndex = index - 1; // Convert to 0-based
    } else if (index < 0) {
      actualIndex = arr.length + index; // Negative indexing from end
    } else {
      return NullValuePlaceHolder.STRING; // index 0 is invalid
    }

    if (actualIndex < 0 || actualIndex >= arr.length) {
      return NullValuePlaceHolder.STRING;
    }
    return arr[actualIndex];
  }

  /**
   * Standard reverse function - same as arrayReverse but with standard SQL name.
   */
  @ScalarFunction
  public static int[] reverseInt(int[] arr) {
    return arrayReverseInt(arr);
  }

  @ScalarFunction
  public static long[] reverseLong(long[] arr) {
    if (arr == null) {
      return null;
    }
    long[] clone = arr.clone();
    ArrayUtils.reverse(clone);
    return clone;
  }

  @ScalarFunction
  public static float[] reverseFloat(float[] arr) {
    if (arr == null) {
      return null;
    }
    float[] clone = arr.clone();
    ArrayUtils.reverse(clone);
    return clone;
  }

  @ScalarFunction
  public static double[] reverseDouble(double[] arr) {
    if (arr == null) {
      return null;
    }
    double[] clone = arr.clone();
    ArrayUtils.reverse(clone);
    return clone;
  }

  @ScalarFunction
  public static String[] reverseString(String[] arr) {
    return arrayReverseString(arr);
  }
}
