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
package org.apache.pinot.common.calcite.function;

import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.runtime.CalciteException;
import org.apache.calcite.runtime.Resources;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.pinot.common.calcite.type.TypeFactory;
import org.apache.pinot.common.function.FunctionRegistry;
import org.apache.pinot.common.function.scalar.DataTypeConversionFunctions;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.exception.PinotRuntimeException;


/**
 * Return type inference for v1 {@link Expression}s using Pinot's Calcite operator return type logic.
 */
public final class SseExpressionTypeInference {

  private SseExpressionTypeInference() {
  }

  /**
   * Infer the return type for an {@link Expression} based on the following rules:
   * <ul>
   *   <li>If the expression is an identifier, return the type of the column from the schema.</li>
   *   <li>If the expression is a literal, return the type of the literal.</li>
   *   <li>If the expression is a function, use Calcite's operator return type inference logic through
   *   {@link PinotOperatorTable} and recursing on the function operands as needed.</li>
   * </ul>
   */
  public static DataSchema.ColumnDataType inferReturnRelType(Expression expression, Schema schema) {
    switch (expression.getType()) {
      case IDENTIFIER: {
        FieldSpec fieldSpec = schema.getFieldSpecFor(expression.getIdentifier().getName());
        if (fieldSpec == null) {
          return DataSchema.ColumnDataType.UNKNOWN;
        }
        return DataSchema.ColumnDataType.fromDataType(fieldSpec.getDataType(), fieldSpec.isSingleValueField());
      }
      case LITERAL: {
        Literal literal = expression.getLiteral();
        return RequestUtils.getLiteralTypeAndValue(literal).getLeft();
      }
      case FUNCTION: {
        Function fn = expression.getFunctionCall();
        String canonicalName = FunctionRegistry.canonicalize(fn.getOperator());

        if ("cast".equals(canonicalName)) {
          return DataTypeConversionFunctions.getReturnType(fn.getOperands().get(1).getLiteral().getStringValue());
        }

        PinotOperatorTable opTable = PinotOperatorTable.instance(true);
        SqlOperator operator = opTable.getOperator(canonicalName);

        if (operator == null) {
          throw new IllegalArgumentException("Unknown function: " + fn.getOperator());
        }

        RelDataTypeFactory typeFactory = TypeFactory.INSTANCE;
        SqlOperatorBinding binding = new ExpressionOperatorBinding(operator, fn.getOperands(), typeFactory, schema);
        RelDataType relType = operator.inferReturnType(binding);
        return DataSchema.ColumnDataType.fromRelDataType(relType);
      }
      default:
        throw new IllegalArgumentException("Unsupported expression type: " + expression.getType());
    }
  }

  private static final class ExpressionOperatorBinding extends SqlOperatorBinding {
    private final List<Expression> _operands;
    private final RelDataTypeFactory _typeFactory;
    private final Schema _schema;

    ExpressionOperatorBinding(SqlOperator operator, List<Expression> operands, RelDataTypeFactory typeFactory,
        Schema schema) {
      super(typeFactory, operator);
      _operands = operands;
      _typeFactory = typeFactory;
      _schema = schema;
    }

    @Override
    public int getOperandCount() {
      return _operands.size();
    }

    @Override
    public RelDataType getOperandType(int ordinal) {
      Expression operand = _operands.get(ordinal);
      switch (operand.getType()) {
        case IDENTIFIER:
          FieldSpec fieldSpec = _schema.getFieldSpecFor(operand.getIdentifier().getName());
          if (fieldSpec != null) {
            return DataSchema.ColumnDataType.fromDataType(fieldSpec.getDataType(), fieldSpec.isSingleValueField())
                .toType(_typeFactory);
          } else {
            return _typeFactory.createSqlType(SqlTypeName.ANY);
          }
        case LITERAL:
          return RequestUtils.getLiteralTypeAndValue(operand.getLiteral()).getLeft().toType(_typeFactory);
        case FUNCTION:
          RelDataType nested = inferReturnRelType(operand, _schema).toType(_typeFactory);
          return nested != null ? nested : _typeFactory.createSqlType(SqlTypeName.ANY);
        default:
          return _typeFactory.createSqlType(SqlTypeName.ANY);
      }
    }

    @Override
    public CalciteException newError(Resources.ExInst<SqlValidatorException> e) {
      throw new PinotRuntimeException(e.ex());
    }

    @Override
    public boolean isOperandNull(int ordinal, boolean allowCast) {
      Expression operand = _operands.get(ordinal);
      return operand.getType() == ExpressionType.LITERAL && operand.getLiteral().isSetNullValue();
    }

    @Override
    public boolean isOperandLiteral(int ordinal, boolean allowCast) {
      return _operands.get(ordinal).getType() == ExpressionType.LITERAL;
    }

    @Override
    public <T> T getOperandLiteralValue(int ordinal, Class<T> clazz) {
      Expression operand = _operands.get(ordinal);
      if (operand.getType() != ExpressionType.LITERAL) {
        return null;
      }
      Object value = RequestUtils.getLiteralTypeAndValue(operand.getLiteral()).getRight();
      if (value == null) {
        return null;
      }
      if (clazz.isInstance(value)) {
        return clazz.cast(value);
      }
      if (value instanceof Number && Number.class.isAssignableFrom(clazz)) {
        Number number = (Number) value;
        if (clazz == Integer.class) {
          return clazz.cast(number.intValue());
        } else if (clazz == Long.class) {
          return clazz.cast(number.longValue());
        } else if (clazz == Double.class) {
          return clazz.cast(number.doubleValue());
        } else if (clazz == Float.class) {
          return clazz.cast(number.floatValue());
        }
      }
      return null;
    }
  }
}
