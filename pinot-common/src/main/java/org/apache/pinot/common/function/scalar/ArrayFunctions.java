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

  @ScalarFunction(names = {"array", "arrayValueConstructor"}, isVarArg = true)
  public static Object arrayValueConstructor(Object... arr) {
    if (arr == null || arr.length == 0 || arr[0] == null) {
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
}
