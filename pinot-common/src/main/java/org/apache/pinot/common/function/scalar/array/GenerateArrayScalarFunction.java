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

import java.lang.reflect.Method;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.pinot.common.function.FunctionInfo;
import org.apache.pinot.common.function.PinotScalarFunction;
import org.apache.pinot.common.function.scalar.ArrayFunctions;
import org.apache.pinot.common.function.sql.PinotSqlFunction;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.spi.annotations.FunctionVolatility;
import org.apache.pinot.spi.annotations.ScalarFunction;


/// `generateArray(start, end[, inc])` in the multi-stage engine, generating the sequence `start, start + inc, ...`
/// bounded by `end` inclusive. The single-stage engine resolves the same name through
/// `GenerateArrayTransformFunction`; both delegate to [ArrayFunctions] so the two engines agree on the sequence and
/// on which arguments are rejected.
///
/// The element type is the widest of the argument types rather than a fixed one, so `generateArray(0, 4)` generates
/// `INT`s while `generateArray(1633078800000, 1633089600000, 1800000)` generates `LONG`s and `generateArray(0, 1,
/// 0.25)` generates `DOUBLE`s. This class exists because that dispatch is on argument types, which the method level
/// [ScalarFunction] annotation cannot express -- it dispatches on argument count alone.
///
/// This class is stateless and thread safe.
@ScalarFunction
public class GenerateArrayScalarFunction implements PinotScalarFunction {
  public static final String NAME = "generateArray";

  /// The element types a sequence can be generated as, narrowest first. The widest of the argument types wins, so a
  /// single fractional or wide argument decides the whole sequence.
  private static final List<ColumnDataType> ELEMENT_TYPES =
      List.of(ColumnDataType.INT, ColumnDataType.LONG, ColumnDataType.FLOAT, ColumnDataType.DOUBLE);
  private static final List<SqlTypeName> ELEMENT_TYPE_NAMES =
      List.of(SqlTypeName.INTEGER, SqlTypeName.BIGINT, SqlTypeName.REAL, SqlTypeName.DOUBLE);

  /// Element type to the [ArrayFunctions] method generating it, by argument count.
  private static final Map<ColumnDataType, Map<Integer, FunctionInfo>> FUNCTION_INFO_MAP =
      new EnumMap<>(ColumnDataType.class);

  static {
    register(ColumnDataType.INT, "generateIntArray", int.class);
    register(ColumnDataType.LONG, "generateLongArray", long.class);
    register(ColumnDataType.FLOAT, "generateFloatArray", float.class);
    register(ColumnDataType.DOUBLE, "generateDoubleArray", double.class);
  }

  private static void register(ColumnDataType elementType, String methodName, Class<?> argumentClass) {
    try {
      FUNCTION_INFO_MAP.put(elementType, Map.of(2,
          functionInfo(ArrayFunctions.class.getMethod(methodName, argumentClass, argumentClass)), 3,
          functionInfo(ArrayFunctions.class.getMethod(methodName, argumentClass, argumentClass, argumentClass))));
    } catch (NoSuchMethodException e) {
      throw new IllegalStateException("Failed to find the " + methodName + " implementation of " + NAME, e);
    }
  }

  /// Marks the function as not deterministic so that neither engine folds a call on literal arguments into an array
  /// literal at the broker. Folding would serialize the whole sequence, up to the maximum length [ArrayFunctions]
  /// allows, into the query plan sent to every server, while evaluating it on the server only needs the three
  /// arguments. The function is still [FunctionVolatility#IMMUTABLE]: the same arguments always generate the same
  /// sequence, so it stays usable in ingestion transforms.
  private static FunctionInfo functionInfo(Method method) {
    return new FunctionInfo(method, ArrayFunctions.class, false, false, FunctionVolatility.IMMUTABLE);
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public PinotSqlFunction toPinotSqlFunction() {
    // The third operand is the increment, which defaults to 1 (or -1 when counting down) when omitted.
    return new PinotSqlFunction(NAME, GenerateArrayScalarFunction::inferReturnType,
        OperandTypes.family(List.of(SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC), i -> i == 2));
  }

  @Nullable
  @Override
  public FunctionInfo getFunctionInfo(ColumnDataType[] argumentTypes) {
    ColumnDataType elementType = getElementType(argumentTypes);
    return elementType != null ? getFunctionInfo(elementType, argumentTypes.length) : null;
  }

  /// The element type of the sequence generated from arguments of these types: `INT`, `LONG`, `FLOAT` or `DOUBLE`,
  /// whichever is the widest of the argument types. Returns `null` if a sequence cannot be generated from one of them.
  ///
  /// The single-stage `GenerateArrayTransformFunction` uses this too. It also evaluates the multi-stage leaf stage
  /// projections, so it must generate the same element type as the type the multi-stage planner inferred.
  @Nullable
  public static ColumnDataType getElementType(ColumnDataType[] argumentTypes) {
    int width = 0;
    for (ColumnDataType argumentType : argumentTypes) {
      int argumentWidth = width(argumentType);
      if (argumentWidth < 0) {
        return null;
      }
      width = Math.max(width, argumentWidth);
    }
    return ELEMENT_TYPES.get(width);
  }

  @Nullable
  @Override
  public FunctionInfo getFunctionInfo(int numArguments) {
    // Reached only when the argument types are unknown. LONG is the type the time grids this mainly generates use.
    return getFunctionInfo(ColumnDataType.LONG, numArguments);
  }

  @Nullable
  private static FunctionInfo getFunctionInfo(ColumnDataType elementType, int numArguments) {
    return FUNCTION_INFO_MAP.get(elementType).get(numArguments);
  }

  private static RelDataType inferReturnType(SqlOperatorBinding binding) {
    int width = 0;
    for (RelDataType operandType : binding.collectOperandTypes()) {
      width = Math.max(width, width(operandType.getSqlTypeName()));
    }
    RelDataTypeFactory typeFactory = binding.getTypeFactory();
    return typeFactory.createArrayType(typeFactory.createSqlType(ELEMENT_TYPE_NAMES.get(width)), -1);
  }

  /// Index into [#ELEMENT_TYPES] of the narrowest sequence this argument type can be generated as, or `-1` if no
  /// sequence can be generated from it.
  private static int width(ColumnDataType argumentType) {
    switch (argumentType) {
      case INT:
        return 0;
      case LONG:
        return 1;
      case FLOAT:
        return 2;
      case DOUBLE:
      case BIG_DECIMAL:
        return 3;
      default:
        return -1;
    }
  }

  /// Index into [#ELEMENT_TYPE_NAMES], the planning side counterpart of [#width(ColumnDataType)]. The operand type
  /// checker has already rejected non numeric arguments, so everything else is an approximate or decimal type.
  private static int width(SqlTypeName typeName) {
    switch (typeName) {
      case TINYINT:
      case SMALLINT:
      case INTEGER:
        return 0;
      case BIGINT:
        return 1;
      case REAL:
      case FLOAT:
        // Pinot reads both as its 4 byte FLOAT type.
        return 2;
      default:
        return 3;
    }
  }
}
