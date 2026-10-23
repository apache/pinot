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
package org.apache.pinot.query.runtime.operator.utils;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.floats.FloatArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import org.apache.pinot.common.utils.ArrayListUtils;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.spi.utils.ByteArray;


public class TypeUtils {
  private TypeUtils() {
  }

  /// Converts value to the desired stored [ColumnDataType]. This is used to convert rows generated from
  /// single-stage engine to be used in multi-stage engine.
  /// TODO: Revisit to see if we should use original type instead of stored type
  public static Object convert(Object value, ColumnDataType storedType) {
    switch (storedType) {
      case INT:
        return value instanceof Integer ? value : ((Number) value).intValue();
      case LONG:
        return value instanceof Long ? value : ((Number) value).longValue();
      case FLOAT:
        return value instanceof Float ? value : ((Number) value).floatValue();
      case DOUBLE:
        return value instanceof Double ? value : ((Number) value).doubleValue();
      case BIG_DECIMAL:
        return value instanceof BigDecimal ? value : BigDecimal.valueOf(((Number) value).doubleValue());
      // For AggregationFunctions that return serialized custom object, e.g. DistinctCountRawHLLAggregationFunction
      case STRING:
        return value.toString();
      case BYTES:
        assert value instanceof ByteArray;
        return value;
      case INT_ARRAY:
        if (value instanceof IntArrayList) {
          // For ArrayAggregationFunction
          return ArrayListUtils.toIntArray((IntArrayList) value);
        }
        if (isOtherNumericArray(value, int[].class)) {
          return convertNumericArray(value, storedType);
        }
        assert value instanceof int[];
        return value;
      case LONG_ARRAY:
        if (value instanceof LongArrayList) {
          // For FunnelCountAggregationFunction and ArrayAggregationFunction
          return ArrayListUtils.toLongArray((LongArrayList) value);
        }
        if (isOtherNumericArray(value, long[].class)) {
          return convertNumericArray(value, storedType);
        }
        assert value instanceof long[];
        return value;
      case FLOAT_ARRAY:
        if (value instanceof FloatArrayList) {
          // For ArrayAggregationFunction
          return ArrayListUtils.toFloatArray((FloatArrayList) value);
        }
        if (value instanceof double[]) {
          // This is due to for parsing array literal value like [0.1, 0.2, 0.3].
          // The parsed value is stored as double[] in java, however the calcite type is FLOAT_ARRAY.
          double[] doubleArray = (double[]) value;
          float[] floatArray = new float[doubleArray.length];
          for (int i = 0; i < floatArray.length; i++) {
            floatArray[i] = (float) doubleArray[i];
          }
          return floatArray;
        }
        if (isOtherNumericArray(value, float[].class)) {
          return convertNumericArray(value, storedType);
        }
        assert value instanceof float[];
        return value;
      case DOUBLE_ARRAY:
        if (value instanceof DoubleArrayList) {
          // For HistogramAggregationFunction and ArrayAggregationFunction
          return ArrayListUtils.toDoubleArray((DoubleArrayList) value);
        }
        if (value instanceof BigDecimal[]) {
          // MV BIG_DECIMAL columns are reported as DOUBLE_ARRAY at the MSE boundary for backward compatibility
          // (see RelToPlanNodeConverter.resolveDecimal TODO); downcast to double[] here. Precision loss is
          // accepted until the TODO is cleared in a future release.
          BigDecimal[] bigDecimalArray = (BigDecimal[]) value;
          double[] doubleArray = new double[bigDecimalArray.length];
          for (int i = 0; i < bigDecimalArray.length; i++) {
            doubleArray[i] = bigDecimalArray[i].doubleValue();
          }
          return doubleArray;
        }
        if (value instanceof ObjectArrayList) {
          // ARRAY_AGG on BIG_DECIMAL produces ObjectArrayList<BigDecimal>; same backward-compat downcast.
          ObjectArrayList<?> list = (ObjectArrayList<?>) value;
          int size = list.size();
          double[] doubleArray = new double[size];
          for (int i = 0; i < size; i++) {
            doubleArray[i] = ((BigDecimal) list.get(i)).doubleValue();
          }
          return doubleArray;
        }
        if (isOtherNumericArray(value, double[].class)) {
          return convertNumericArray(value, storedType);
        }
        assert value instanceof double[];
        return value;
      case BIG_DECIMAL_ARRAY:
        if (value instanceof ObjectArrayList) {
          // For ArrayAggregationFunction
          return ArrayListUtils.toBigDecimalArray((ObjectArrayList<BigDecimal>) value);
        }
        assert value instanceof BigDecimal[];
        return value;
      case STRING_ARRAY:
        if (value instanceof ObjectArrayList) {
          // For ArrayAggregationFunction
          return ArrayListUtils.toStringArray((ObjectArrayList<String>) value);
        }
        assert value instanceof String[];
        return value;
      case BYTES_ARRAY:
        if (value instanceof ObjectArrayList) {
          // For ArrayAggregationFunction
          return ArrayListUtils.toBytesArray((ObjectArrayList<ByteArray>) value);
        }
        assert value instanceof ByteArray[];
        return value;
      // TODO: Add more conversions
      default:
        return value;
    }
  }

  /// Returns whether the value is a primitive numeric array with an element type other than the expected one.
  private static boolean isOtherNumericArray(Object value, Class<?> expectedClass) {
    return value.getClass() != expectedClass && (value instanceof int[] || value instanceof long[]
        || value instanceof float[] || value instanceof double[]);
  }

  /// Converts the elements of a primitive numeric array the way [#convert] converts a single value. The single-stage
  /// engine can compute another element type than the one the multi-stage planner inferred. For example, its `plus`
  /// returns DOUBLE, so `generateArray(intCol, intCol + 1)` generates a `double[]` where the plan has an INT_ARRAY.
  private static Object convertNumericArray(Object value, ColumnDataType storedType) {
    int length = Array.getLength(value);
    switch (storedType) {
      case INT_ARRAY:
        int[] intArray = new int[length];
        for (int i = 0; i < length; i++) {
          intArray[i] = ((Number) Array.get(value, i)).intValue();
        }
        return intArray;
      case LONG_ARRAY:
        long[] longArray = new long[length];
        for (int i = 0; i < length; i++) {
          longArray[i] = ((Number) Array.get(value, i)).longValue();
        }
        return longArray;
      case FLOAT_ARRAY:
        float[] floatArray = new float[length];
        for (int i = 0; i < length; i++) {
          floatArray[i] = ((Number) Array.get(value, i)).floatValue();
        }
        return floatArray;
      case DOUBLE_ARRAY:
        double[] doubleArray = new double[length];
        for (int i = 0; i < length; i++) {
          doubleArray[i] = ((Number) Array.get(value, i)).doubleValue();
        }
        return doubleArray;
      default:
        throw new IllegalStateException("Not a numeric array type: " + storedType);
    }
  }

  /// Converts row to the desired stored [ColumnDataType]s in-place. This is used to convert rows generated from
  /// single-stage engine to be used in multi-stage engine.
  public static void convertRow(Object[] row, ColumnDataType[] outputStoredTypes) {
    int numColumns = row.length;
    for (int colId = 0; colId < numColumns; colId++) {
      Object value = row[colId];
      if (value != null) {
        row[colId] = convert(value, outputStoredTypes[colId]);
      }
    }
  }
}
