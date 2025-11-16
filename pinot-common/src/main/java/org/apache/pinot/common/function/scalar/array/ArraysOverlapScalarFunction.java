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
package org.apache.pinot.common.function.scalar.array;

import it.unimi.dsi.fastutil.doubles.DoubleOpenHashSet;
import it.unimi.dsi.fastutil.floats.FloatOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.pinot.common.function.FunctionInfo;
import org.apache.pinot.common.function.PinotScalarFunction;
import org.apache.pinot.common.function.sql.PinotSqlFunction;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.spi.annotations.ScalarFunction;


@ScalarFunction(names = {"ARRAYS_OVERLAP", "ARRAYSOVERLAP"})
public class ArraysOverlapScalarFunction implements PinotScalarFunction {

  private static final Map<DataSchema.ColumnDataType, FunctionInfo>
      TYPE_FUNCTION_INFO_MAP = new EnumMap<>(DataSchema.ColumnDataType.class);

  static {
    try {
      TYPE_FUNCTION_INFO_MAP.put(DataSchema.ColumnDataType.INT_ARRAY,
          new FunctionInfo(ArraysOverlapScalarFunction.class.getMethod("arraysOverlap", int[].class, int[].class),
              ArraysOverlapScalarFunction.class, false));
      TYPE_FUNCTION_INFO_MAP.put(DataSchema.ColumnDataType.LONG_ARRAY,
          new FunctionInfo(ArraysOverlapScalarFunction.class.getMethod("arraysOverlap", long[].class, long[].class),
              ArraysOverlapScalarFunction.class, false));
      TYPE_FUNCTION_INFO_MAP.put(DataSchema.ColumnDataType.FLOAT_ARRAY,
          new FunctionInfo(ArraysOverlapScalarFunction.class.getMethod("arraysOverlap", float[].class, float[].class),
              ArraysOverlapScalarFunction.class, false));
      TYPE_FUNCTION_INFO_MAP.put(DataSchema.ColumnDataType.DOUBLE_ARRAY,
          new FunctionInfo(ArraysOverlapScalarFunction.class.getMethod("arraysOverlap", double[].class, double[].class),
              ArraysOverlapScalarFunction.class, false));
      TYPE_FUNCTION_INFO_MAP.put(DataSchema.ColumnDataType.STRING_ARRAY,
          new FunctionInfo(ArraysOverlapScalarFunction.class.getMethod("arraysOverlap", String[].class, String[].class),
              ArraysOverlapScalarFunction.class, false));
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String getName() {
    return "ARRAYS_OVERLAP";
  }

  @Override
  public Set<String> getNames() {
    return Set.of("ARRAYS_OVERLAP", "ARRAYSOVERLAP");
  }

  @Nullable
  @Override
  public PinotSqlFunction toPinotSqlFunction() {
    return new PinotSqlFunction("ARRAYS_OVERLAP", ReturnTypes.BOOLEAN,
        OperandTypes.family(List.of(SqlTypeFamily.ARRAY, SqlTypeFamily.ARRAY)));
  }

  @Nullable
  @Override
  public FunctionInfo getFunctionInfo(DataSchema.ColumnDataType[] argumentTypes) {
    if (argumentTypes.length != 2) {
      return null;
    }
    if (argumentTypes[0] != argumentTypes[1]) {
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
    return getFunctionInfo(new DataSchema.ColumnDataType[]{
        DataSchema.ColumnDataType.STRING_ARRAY,
        DataSchema.ColumnDataType.STRING_ARRAY
    });
  }

  private static boolean overlapInts(int[] small, int[] large) {
    IntOpenHashSet set = new IntOpenHashSet(small);
    for (int v : large) {
      if (set.contains(v)) {
        return true;
      }
    }
    return false;
  }

  private static boolean overlapLongs(long[] small, long[] large) {
    LongOpenHashSet set = new LongOpenHashSet(small);
    for (long v : large) {
      if (set.contains(v)) {
        return true;
      }
    }
    return false;
  }

  private static boolean overlapFloats(float[] small, float[] large) {
    FloatOpenHashSet set = new FloatOpenHashSet(small);
    for (float v : large) {
      if (set.contains(v)) {
        return true;
      }
    }
    return false;
  }

  private static boolean overlapDoubles(double[] small, double[] large) {
    DoubleOpenHashSet set = new DoubleOpenHashSet(small);

    for (double v : large) {
      if (set.contains(v)) {
        return true;
      }
    }
    return false;
  }

  private static boolean overlapStrings(String[] small, String[] large) {
    ObjectOpenHashSet<String> set = new ObjectOpenHashSet<>(small);
    for (String v : large) {
      if (set.contains(v)) {
        return true;
      }
    }
    return false;
  }

  public static boolean arraysOverlap(int[] array1, int[] array2) {
    return array1.length <= array2.length ? overlapInts(array1, array2) : overlapInts(array2, array1);
  }

  public static boolean arraysOverlap(long[] array1, long[] array2) {
    return array1.length <= array2.length ? overlapLongs(array1, array2) : overlapLongs(array2, array1);
  }

  public static boolean arraysOverlap(float[] array1, float[] array2) {
    return array1.length <= array2.length ? overlapFloats(array1, array2) : overlapFloats(array2, array1);
  }

  public static boolean arraysOverlap(double[] array1, double[] array2) {
    return array1.length <= array2.length ? overlapDoubles(array1, array2) : overlapDoubles(array2, array1);
  }

  public static boolean arraysOverlap(String[] array1, String[] array2) {
    return array1.length <= array2.length ? overlapStrings(array1, array2) : overlapStrings(array2, array1);
  }
}
