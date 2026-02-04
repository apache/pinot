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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.annotation.Nullable;
import org.apache.pinot.common.function.FunctionInfo;
import org.apache.pinot.common.function.PinotScalarFunction;
import org.apache.pinot.common.function.sql.PinotSqlFunction;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.operator.transform.function.EvalMvPredicateEvaluator;
import org.apache.pinot.spi.annotations.ScalarFunction;
import org.apache.pinot.spi.data.FieldSpec.DataType;


@ScalarFunction(names = {"evalMv"})
public class EvalMvScalarFunction implements PinotScalarFunction {
  private static final Map<ColumnDataType, FunctionInfo> TYPE_FUNCTION_INFO_MAP =
      new EnumMap<>(ColumnDataType.class);
  private static final ConcurrentMap<CacheKey, EvalMvPredicateEvaluator> EVALUATOR_CACHE =
      new ConcurrentHashMap<>();

  static {
    try {
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.INT_ARRAY,
          new FunctionInfo(EvalMvScalarFunction.class.getMethod("evalMv", int[].class, String.class),
              EvalMvScalarFunction.class, false));
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.LONG_ARRAY,
          new FunctionInfo(EvalMvScalarFunction.class.getMethod("evalMv", long[].class, String.class),
              EvalMvScalarFunction.class, false));
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.FLOAT_ARRAY,
          new FunctionInfo(EvalMvScalarFunction.class.getMethod("evalMv", float[].class, String.class),
              EvalMvScalarFunction.class, false));
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.DOUBLE_ARRAY,
          new FunctionInfo(EvalMvScalarFunction.class.getMethod("evalMv", double[].class, String.class),
              EvalMvScalarFunction.class, false));
      TYPE_FUNCTION_INFO_MAP.put(ColumnDataType.STRING_ARRAY,
          new FunctionInfo(EvalMvScalarFunction.class.getMethod("evalMv", String[].class, String.class),
              EvalMvScalarFunction.class, false));
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String getName() {
    return "evalMv";
  }

  @Override
  public Set<String> getNames() {
    return Set.of("evalMv");
  }

  @Nullable
  @Override
  public PinotSqlFunction toPinotSqlFunction() {
    // Should already be registered in PinotOperatorTable by the transform function implementation
    return null;
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
    return TYPE_FUNCTION_INFO_MAP.get(argumentTypes[0]);
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

  public static int[] evalMv(int[] values, String predicate) {
    EvalMvPredicateEvaluator evaluator = evaluatorFor(predicate, DataType.INT);
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

  public static long[] evalMv(long[] values, String predicate) {
    EvalMvPredicateEvaluator evaluator = evaluatorFor(predicate, DataType.LONG);
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

  public static float[] evalMv(float[] values, String predicate) {
    EvalMvPredicateEvaluator evaluator = evaluatorFor(predicate, DataType.FLOAT);
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

  public static double[] evalMv(double[] values, String predicate) {
    EvalMvPredicateEvaluator evaluator = evaluatorFor(predicate, DataType.DOUBLE);
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

  public static String[] evalMv(String[] values, String predicate) {
    EvalMvPredicateEvaluator evaluator = evaluatorFor(predicate, DataType.STRING);
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

  private static EvalMvPredicateEvaluator evaluatorFor(String predicate, DataType dataType) {
    CacheKey key = new CacheKey(predicate, dataType);
    return EVALUATOR_CACHE.computeIfAbsent(key,
        cacheKey -> EvalMvPredicateEvaluator.forPredicate(cacheKey._predicate, cacheKey._dataType, null, null));
  }

  private static final class CacheKey {
    private final String _predicate;
    private final DataType _dataType;

    private CacheKey(String predicate, DataType dataType) {
      _predicate = predicate;
      _dataType = dataType;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof CacheKey)) {
        return false;
      }
      CacheKey other = (CacheKey) obj;
      return _dataType == other._dataType && _predicate.equals(other._predicate);
    }

    @Override
    public int hashCode() {
      int result = _predicate.hashCode();
      result = 31 * result + _dataType.hashCode();
      return result;
    }
  }
}
