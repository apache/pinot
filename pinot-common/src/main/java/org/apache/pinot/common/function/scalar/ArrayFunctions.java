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

import com.google.common.base.Preconditions;
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


/// Inbuilt array scalar functions. See [ArrayUtils] for details.
public class ArrayFunctions {
  /// Maximum number of elements the `generate*Array` functions may produce. Calls on literal arguments are folded
  /// into the query plan as an array literal, so an unbounded range would exhaust broker memory rather than fail
  /// cleanly. 100000 covers a minute resolution grid over 69 days, which is well past any practical time grid.
  private static final int MAX_GENERATED_ARRAY_LENGTH = 100_000;

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

  /// Assume values1, and values2 are monotonous increasing indices of MV cols.
  /// Here is the common usage:
  /// col1: \["a", "b", "a", "b"\]
  /// col2: \["c", "d", "d", "c"\]
  /// The user want to get the first index called idx, s.t. col1\[idx\] == "b" && col2\[idx\] == "d"
  /// arrayElementAtInt(0, intersectIndices(arrayIndexOfAllString(col1, "b"), arrayIndexOfAllString(col2, "d")))
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
  public static long[] arraySliceLong(long[] values, int start, int end) {
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
    if (clazz == byte[].class) {
      byte[][] bytesArr = new byte[arr.length][];
      for (int i = 0; i < arr.length; i++) {
        bytesArr[i] = (byte[]) arr[i];
      }
      return bytesArr;
    }
    return arr;
  }

  /// Generates the sequence `start, start + inc, ...`, stopping at the last value that does not pass `end`. Both
  /// bounds are inclusive when `end` lands exactly on a step, and `inc` may be negative to count down.
  ///
  /// The main use is building a dense time grid to `UNNEST` and left join a sparse time series onto, which is how gap
  /// filling is expressed in the multi-stage engine:
  /// ```sql
  /// SELECT ts FROM UNNEST(generateArray(1633078800000, 1633089600000, 1800000)) AS grid(ts)
  /// ```
  /// `generateArray` selects the variant to call from its argument types; see
  /// [org.apache.pinot.common.function.scalar.array.GenerateArrayScalarFunction].
  ///
  /// @throws IllegalArgumentException if `inc` is zero, if its sign does not lead from `start` to `end`, or if the
  ///         sequence would hold more than [#MAX_GENERATED_ARRAY_LENGTH] elements
  @ScalarFunction
  public static long[] generateLongArray(long start, long end, long inc) {
    long[] arr = new long[generatedArrayLength(start, end, inc)];
    for (int i = 0; i < arr.length; i++) {
      arr[i] = start + i * inc;
    }
    return arr;
  }

  /// Same as [#generateLongArray(long, long, long)] with a step of `1`, or `-1` when `end` is below `start`.
  @ScalarFunction
  public static long[] generateLongArray(long start, long end) {
    return generateLongArray(start, end, end < start ? -1 : 1);
  }

  @ScalarFunction
  public static int[] generateIntArray(int start, int end, int inc) {
    int[] arr = new int[generatedArrayLength(start, end, inc)];
    for (int i = 0; i < arr.length; i++) {
      arr[i] = start + i * inc;
    }
    return arr;
  }

  @ScalarFunction
  public static int[] generateIntArray(int start, int end) {
    return generateIntArray(start, end, end < start ? -1 : 1);
  }

  @ScalarFunction
  public static float[] generateFloatArray(float start, float end, float inc) {
    float[] arr = new float[generatedArrayLength(start, end, inc)];
    for (int i = 0; i < arr.length; i++) {
      arr[i] = start + i * inc;
    }
    return arr;
  }

  @ScalarFunction
  public static float[] generateFloatArray(float start, float end) {
    return generateFloatArray(start, end, end < start ? -1 : 1);
  }

  @ScalarFunction
  public static double[] generateDoubleArray(double start, double end, double inc) {
    double[] arr = new double[generatedArrayLength(start, end, inc)];
    for (int i = 0; i < arr.length; i++) {
      arr[i] = start + i * inc;
    }
    return arr;
  }

  @ScalarFunction
  public static double[] generateDoubleArray(double start, double end) {
    return generateDoubleArray(start, end, end < start ? -1 : 1);
  }

  /// Length of the integral sequence `start, start + inc, ...` bounded by `end`, inclusive.
  private static int generatedArrayLength(long start, long end, long inc) {
    Preconditions.checkArgument(inc != 0, "Increment must not be zero");
    long span;
    try {
      span = Math.subtractExact(end, start);
    } catch (ArithmeticException e) {
      // The span does not fit in a long, so the sequence is far past the length limit whatever the increment is.
      throw new IllegalArgumentException(
          String.format("Range from %s to %s is too wide to generate an array from", start, end));
    }
    checkIncrementLeadsToEnd(Long.signum(span), Long.signum(inc), start, end, inc);
    return checkGeneratedArrayLength(span / inc);
  }

  /// Length of the floating point sequence `start, start + inc, ...` bounded by `end`, inclusive.
  private static int generatedArrayLength(double start, double end, double inc) {
    Preconditions.checkArgument(inc != 0 && !Double.isNaN(inc), "Increment must not be zero or NaN, got: %s", inc);
    Preconditions.checkArgument(Double.isFinite(start) && Double.isFinite(end),
        "Range from %s to %s must be finite", start, end);
    double span = end - start;
    checkIncrementLeadsToEnd((int) Math.signum(span), (int) Math.signum(inc), start, end, inc);
    // A span too wide for a long saturates the cast at Long.MAX_VALUE, which the length check below rejects.
    return checkGeneratedArrayLength((long) Math.floor(span / inc));
  }

  private static void checkIncrementLeadsToEnd(int spanSignum, int incSignum, Number start, Number end, Number inc) {
    // A zero span yields the single element sequence [start], so any non-zero increment is fine there.
    Preconditions.checkArgument(spanSignum == 0 || spanSignum == incSignum,
        "Increment: %s does not lead from start: %s to end: %s", inc, start, end);
  }

  /// Checks the length of a sequence taking `steps` increments to reach its end, and returns it. The check is on the
  /// step count rather than on the length so that a span of `Long.MAX_VALUE` cannot overflow into a small length.
  private static int checkGeneratedArrayLength(long steps) {
    Preconditions.checkArgument(steps < MAX_GENERATED_ARRAY_LENGTH,
        "Generating more than %s elements exceeds the maximum of %s", steps, MAX_GENERATED_ARRAY_LENGTH);
    return (int) steps + 1;
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
}
