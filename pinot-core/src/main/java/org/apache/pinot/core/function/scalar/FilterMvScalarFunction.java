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
package org.apache.pinot.core.function.scalar;

import java.util.EnumMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.function.FunctionInfo;
import org.apache.pinot.common.function.PinotScalarFunction;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.operator.transform.function.FilterMvPredicateEvaluator;
import org.apache.pinot.spi.annotations.ScalarFunction;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * Scalar wrapper for filterMv so FunctionRegistry can expose type signatures for query planning and execution paths
 * that resolve scalar functions.
 *
 * <p>Each FunctionInvoker creates a new instance of this class, so instance fields are safe to use without
 * synchronization. The evaluator is cached per instance to avoid repeated creation for the same predicate.</p>
 */
@ScalarFunction
public class FilterMvScalarFunction implements PinotScalarFunction {
  private static final Map<ColumnDataType, FunctionInfo> TYPE_FUNCTION_INFO_MAP =
      new EnumMap<>(ColumnDataType.class);

  static {
    try {
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.INT_ARRAY,
          new FunctionInfo(FilterMvScalarFunction.class.getMethod("filterMv", int[].class, String.class),
              FilterMvScalarFunction.class, false));
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.LONG_ARRAY,
          new FunctionInfo(FilterMvScalarFunction.class.getMethod("filterMv", long[].class, String.class),
              FilterMvScalarFunction.class, false));
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.FLOAT_ARRAY,
          new FunctionInfo(FilterMvScalarFunction.class.getMethod("filterMv", float[].class, String.class),
              FilterMvScalarFunction.class, false));
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.DOUBLE_ARRAY,
          new FunctionInfo(FilterMvScalarFunction.class.getMethod("filterMv", double[].class, String.class),
              FilterMvScalarFunction.class, false));
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.STRING_ARRAY,
          new FunctionInfo(FilterMvScalarFunction.class.getMethod("filterMv", String[].class, String.class),
              FilterMvScalarFunction.class, false));
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.BYTES_ARRAY,
          new FunctionInfo(FilterMvScalarFunction.class.getMethod("filterMv", byte[][].class, String.class),
              FilterMvScalarFunction.class, false));
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(e);
    }
  }

  private String _predicate;
  private DataType _dataType;
  private FilterMvPredicateEvaluator _evaluator;

  @Override
  public String getName() {
    return "filterMv";
  }

  @Nullable
  @Override
  public FunctionInfo getFunctionInfo(ColumnDataType[] argumentTypes) {
    if (argumentTypes.length != 2) {
      return null;
    }
    if (argumentTypes[1] != ColumnDataType.STRING) {
      return null;
    }
    return TYPE_FUNCTION_INFO_MAP.get(argumentTypes[0].getStoredType());
  }

  @Nullable
  @Override
  public FunctionInfo getFunctionInfo(int numArguments) {
    if (numArguments != 2) {
      return null;
    }
    // Fall back to string
    return getFunctionInfo(new ColumnDataType[]{ColumnDataType.STRING_ARRAY, ColumnDataType.STRING});
  }

  public int[] filterMv(int[] values, String predicate) {
    FilterMvPredicateEvaluator evaluator = evaluatorFor(predicate, DataType.INT);
    int numValues = values.length;
    int count = 0;
    for (int value : values) {
      if (evaluator.matchesInt(value)) {
        count++;
      }
    }
    if (count == numValues) {
      return values;
    }
    int[] filtered = new int[count];
    int idx = 0;
    for (int value : values) {
      if (evaluator.matchesInt(value)) {
        filtered[idx++] = value;
      }
    }
    return filtered;
  }

  public long[] filterMv(long[] values, String predicate) {
    FilterMvPredicateEvaluator evaluator = evaluatorFor(predicate, DataType.LONG);
    int numValues = values.length;
    int count = 0;
    for (long value : values) {
      if (evaluator.matchesLong(value)) {
        count++;
      }
    }
    if (count == numValues) {
      return values;
    }
    long[] filtered = new long[count];
    int idx = 0;
    for (long value : values) {
      if (evaluator.matchesLong(value)) {
        filtered[idx++] = value;
      }
    }
    return filtered;
  }

  public float[] filterMv(float[] values, String predicate) {
    FilterMvPredicateEvaluator evaluator = evaluatorFor(predicate, DataType.FLOAT);
    int numValues = values.length;
    int count = 0;
    for (float value : values) {
      if (evaluator.matchesFloat(value)) {
        count++;
      }
    }
    if (count == numValues) {
      return values;
    }
    float[] filtered = new float[count];
    int idx = 0;
    for (float value : values) {
      if (evaluator.matchesFloat(value)) {
        filtered[idx++] = value;
      }
    }
    return filtered;
  }

  public double[] filterMv(double[] values, String predicate) {
    FilterMvPredicateEvaluator evaluator = evaluatorFor(predicate, DataType.DOUBLE);
    int numValues = values.length;
    int count = 0;
    for (double value : values) {
      if (evaluator.matchesDouble(value)) {
        count++;
      }
    }
    if (count == numValues) {
      return values;
    }
    double[] filtered = new double[count];
    int idx = 0;
    for (double value : values) {
      if (evaluator.matchesDouble(value)) {
        filtered[idx++] = value;
      }
    }
    return filtered;
  }

  public String[] filterMv(String[] values, String predicate) {
    FilterMvPredicateEvaluator evaluator = evaluatorFor(predicate, DataType.STRING);
    int numValues = values.length;
    int count = 0;
    for (String value : values) {
      if (evaluator.matchesString(value)) {
        count++;
      }
    }
    if (count == numValues) {
      return values;
    }
    String[] filtered = new String[count];
    int idx = 0;
    for (String value : values) {
      if (evaluator.matchesString(value)) {
        filtered[idx++] = value;
      }
    }
    return filtered;
  }

  public byte[][] filterMv(byte[][] values, String predicate) {
    FilterMvPredicateEvaluator evaluator = evaluatorFor(predicate, DataType.BYTES);
    int numValues = values.length;
    int count = 0;
    for (byte[] value : values) {
      if (evaluator.matchesBytes(value)) {
        count++;
      }
    }
    if (count == numValues) {
      return values;
    }
    byte[][] filtered = new byte[count][];
    int idx = 0;
    for (byte[] value : values) {
      if (evaluator.matchesBytes(value)) {
        filtered[idx++] = value;
      }
    }
    return filtered;
  }

  private FilterMvPredicateEvaluator evaluatorFor(String predicate, DataType dataType) {
    if (_evaluator == null || _dataType != dataType || !_predicate.equals(predicate)) {
      // Build the new evaluator first so cached state is only updated after successful creation
      // Scalar invocation: no dictionary is available — the evaluator builds against raw values only.
      FilterMvPredicateEvaluator newEvaluator =
          FilterMvPredicateEvaluator.forPredicate(predicate, dataType, null, null);
      _predicate = predicate;
      _dataType = dataType;
      _evaluator = newEvaluator;
    }
    return _evaluator;
  }
}
