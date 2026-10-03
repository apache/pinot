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
package org.apache.pinot.common.function;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.function.IntFunction;
import javax.annotation.Nullable;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.ExplicitOperatorBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.pinot.common.request.context.AggregateCallBinding;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.LiteralContext;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/// Adapts schema-derived expression types to the same return-type rules used by the SQL planner.
/// All binding state is local to a call; this utility is thread-safe and never constructs runtime transforms.
public final class AggregationFunctionTypeResolver {
  private AggregationFunctionTypeResolver() {
  }

  @Nullable
  public static AggregateCallBinding bind(AggregationFunctionType type, List<ExpressionContext> arguments,
      IntFunction<ColumnDataType> argumentTypeResolver) {
    if (!type.isTypeBindingRequired(arguments.size(), index ->
        arguments.get(index).getType() == ExpressionContext.Type.LITERAL
            && arguments.get(index).getLiteral().getType() == DataType.STRING)) {
      return null;
    }
    List<ColumnDataType> argumentTypes = new ArrayList<>(arguments.size());
    try {
      for (int i = 0; i < arguments.size(); i++) {
        argumentTypes.add(argumentTypeResolver.apply(i));
      }
    } catch (TypeInferenceUnavailableException e) {
      if (type.supportsLegacyUnboundCalls()) {
        return null;
      }
      throw e;
    }
    ColumnDataType resultType =
        inferReturnType(type.getName(), type.getReturnTypeInference(), arguments, argumentTypes);
    return new AggregateCallBinding(argumentTypes, resultType);
  }

  /// Signals missing schema-only metadata, distinct from invalid expressions or unsupported aggregate types.
  /// Legacy overloads can retain their original execution path; new inferred overloads require this metadata.
  public static class TypeInferenceUnavailableException extends IllegalArgumentException {
    private static final long serialVersionUID = 1L;

    public TypeInferenceUnavailableException(String functionName) {
      super("No schema-only type inference for native transform: " + functionName);
    }
  }

  public static ColumnDataType inferReturnType(String functionName, SqlReturnTypeInference inference,
      List<ExpressionContext> arguments, List<ColumnDataType> argumentTypes) {
    RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
    List<RelDataType> operandTypes = new ArrayList<>(argumentTypes.size());
    for (ColumnDataType argumentType : argumentTypes) {
      operandTypes.add(argumentType.toType(typeFactory));
    }
    SqlOperator operator = new SqlFunction(functionName, SqlKind.OTHER_FUNCTION, inference, null, null,
        SqlFunctionCategory.USER_DEFINED_FUNCTION);
    RelDataType resultType = inference.inferReturnType(new LiteralOperatorBinding(typeFactory, operator,
        operandTypes, arguments));
    if (resultType == null) {
      throw new IllegalArgumentException("Cannot infer result type for " + functionName + argumentTypes);
    }
    return toColumnDataType(resultType);
  }

  /// Converts a logical planner type without replacing BOOLEAN or TIMESTAMP by its storage representation.
  public static ColumnDataType toColumnDataType(RelDataType type) {
    switch (type.getSqlTypeName()) {
      case TINYINT:
      case SMALLINT:
      case INTEGER:
        return ColumnDataType.INT;
      case BIGINT:
        return ColumnDataType.LONG;
      case REAL:
      case FLOAT:
        return ColumnDataType.FLOAT;
      case DOUBLE:
        return ColumnDataType.DOUBLE;
      case DECIMAL:
        return ColumnDataType.BIG_DECIMAL;
      case BOOLEAN:
        return ColumnDataType.BOOLEAN;
      case TIMESTAMP:
        return ColumnDataType.TIMESTAMP;
      case CHAR:
      case VARCHAR:
        return ColumnDataType.STRING;
      case BINARY:
      case VARBINARY:
        return ColumnDataType.BYTES;
      case UUID:
        return ColumnDataType.UUID;
      case ARRAY:
        return ColumnDataType.valueOf(toColumnDataType(type.getComponentType()).name() + "_ARRAY");
      case NULL:
      case UNKNOWN:
        return ColumnDataType.UNKNOWN;
      case ANY:
      case OTHER:
        return ColumnDataType.OBJECT;
      default:
        throw new IllegalArgumentException("Unsupported expression type: " + type);
    }
  }

  /// Supplies literal options to planner inference without requiring a parsed or validated SQL tree.
  private static final class LiteralOperatorBinding extends ExplicitOperatorBinding {
    private final List<ExpressionContext> _arguments;

    private LiteralOperatorBinding(RelDataTypeFactory typeFactory, SqlOperator operator,
        List<RelDataType> operandTypes, List<ExpressionContext> arguments) {
      super(typeFactory, operator, operandTypes);
      _arguments = arguments;
    }

    @Override
    public boolean isOperandLiteral(int ordinal, boolean allowCast) {
      return _arguments.get(ordinal).getType() == ExpressionContext.Type.LITERAL;
    }

    @Override
    public boolean isOperandNull(int ordinal, boolean allowCast) {
      return isOperandLiteral(ordinal, allowCast) && _arguments.get(ordinal).getLiteral().getValue() == null;
    }

    @Nullable
    @Override
    public <T> T getOperandLiteralValue(int ordinal, Class<T> clazz) {
      if (!isOperandLiteral(ordinal, false)) {
        throw new IllegalArgumentException("Argument " + ordinal + " to " + getOperator().getName()
            + " must be a literal");
      }
      LiteralContext literal = _arguments.get(ordinal).getLiteral();
      Object value = literal.getValue();
      if (value == null) {
        return null;
      }
      if (clazz == String.class) {
        return clazz.cast(literal.getStringValue());
      }
      if (clazz == BigDecimal.class && value instanceof Number) {
        return clazz.cast(new BigDecimal(value.toString()));
      }
      return clazz.cast(value);
    }

    @Override
    public boolean hasEmptyGroup() {
      // An aggregate must have a defined logical type even when every input row is absent.
      return true;
    }
  }
}
