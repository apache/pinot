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
package org.apache.pinot.query.runtime.operator.operands;

import com.google.common.base.Preconditions;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.common.utils.VariantUtils;
import org.apache.pinot.common.utils.VariantUtils.ResultType;
import org.apache.pinot.common.utils.VariantUtils.ReusableResult;
import org.apache.pinot.common.utils.VariantUtils.VariantPath;
import org.apache.pinot.query.planner.logical.RexExpression;


/// Query-local multi-stage operand for Variant scalar operations.
///
/// <p>Literal paths and target types are compiled once at construction. Values enter and leave the operand in
/// {@link DataSchema}'s internal representation, which {@link VariantUtils.ReusableResult} materializes directly after
/// extraction. The reusable extraction result makes instances not thread-safe.
final class VariantOperand implements TransformOperand {
  private static final VariantPath ROOT_PATH = VariantUtils.compilePath("$");
  private static final String VARIANT_GET = "variantget";
  private static final String TRY_VARIANT_GET = "tryvariantget";
  private static final String VARIANT_EXISTS = "variantexists";
  private static final String IS_VARIANT_NULL = "isvariantnull";
  private static final String VARIANT_TYPE_OF = "varianttypeof";
  private static final String VARIANT_TO_JSON = "varianttojson";

  private final ColumnDataType _resultType;
  private final TransformOperand _variantOperand;
  private final Operation _operation;
  private final VariantPath _path;
  @Nullable
  private final ResultType _targetType;
  private final ReusableResult _reusableResult = new ReusableResult();

  VariantOperand(RexExpression.FunctionCall functionCall, DataSchema dataSchema, String canonicalName) {
    _resultType = functionCall.getDataType();
    List<RexExpression> operands = functionCall.getFunctionOperands();
    _operation = operation(canonicalName);
    validateArgumentCount(_operation, operands.size(), functionCall.getFunctionName());
    _variantOperand = TransformOperandFactory.getTransformOperand(operands.get(0), dataSchema);
    ColumnDataType inputType = _variantOperand.getResultType();
    Preconditions.checkArgument(
        inputType == ColumnDataType.VARIANT || inputType == ColumnDataType.BYTES || inputType == ColumnDataType.UNKNOWN,
        "%s first argument must be a VARIANT", functionCall.getFunctionName());

    switch (_operation) {
      case GET:
      case TRY_GET:
        _path = compilePathLiteral(operands.get(1), functionCall.getFunctionName());
        _targetType = operands.size() == 2 ? ResultType.VARIANT
            : parseTypeLiteral(operands.get(2), functionCall.getFunctionName());
        break;
      case EXISTS:
        _path = compilePathLiteral(operands.get(1), functionCall.getFunctionName());
        _targetType = null;
        break;
      case IS_NULL:
      case TYPE_OF:
        _path =
            operands.size() == 2 ? compilePathLiteral(operands.get(1), functionCall.getFunctionName()) : ROOT_PATH;
        _targetType = null;
        break;
      case TO_JSON:
        _path = ROOT_PATH;
        _targetType = null;
        break;
      default:
        throw new IllegalStateException("Unhandled Variant operation: " + _operation);
    }
  }

  static boolean isSupported(String canonicalName) {
    switch (canonicalName) {
      case VARIANT_GET:
      case TRY_VARIANT_GET:
      case VARIANT_EXISTS:
      case IS_VARIANT_NULL:
      case VARIANT_TYPE_OF:
      case VARIANT_TO_JSON:
        return true;
      default:
        return false;
    }
  }

  @Override
  public ColumnDataType getResultType() {
    return _resultType;
  }

  @Nullable
  @Override
  public Object apply(List<Object> row) {
    Object internalVariant = _variantOperand.apply(row);
    byte[] variant = internalVariant != null
        ? (byte[]) _variantOperand.getResultType().toExternal(internalVariant) : null;
    if (_operation == Operation.GET) {
      return extractInternal(variant, false);
    }
    if (_operation == Operation.TRY_GET) {
      return extractInternal(variant, true);
    }
    Object externalResult;
    switch (_operation) {
      case EXISTS:
        externalResult = VariantUtils.variantExists(variant, _path, _reusableResult);
        break;
      case IS_NULL:
        externalResult = VariantUtils.isVariantNull(variant, _path, _reusableResult);
        break;
      case TYPE_OF:
        externalResult = VariantUtils.variantTypeOf(variant, _path, _reusableResult);
        break;
      case TO_JSON:
        externalResult = VariantUtils.variantToJson(variant, _reusableResult);
        break;
      default:
        throw new IllegalStateException("Unhandled Variant operation: " + _operation);
    }
    return externalResult != null ? _resultType.toInternal(externalResult) : null;
  }

  @Nullable
  private Object extractInternal(@Nullable byte[] variant, boolean tolerant) {
    ResultType targetType = Preconditions.checkNotNull(_targetType, "Variant target type must be planned");
    boolean present = tolerant
        ? VariantUtils.tryExtractInto(variant, _path, targetType, _reusableResult)
        : VariantUtils.extractInto(variant, _path, targetType, _reusableResult);
    return present ? _reusableResult.getInternalValue(targetType) : null;
  }

  private static Operation operation(String canonicalName) {
    switch (canonicalName) {
      case VARIANT_GET:
        return Operation.GET;
      case TRY_VARIANT_GET:
        return Operation.TRY_GET;
      case VARIANT_EXISTS:
        return Operation.EXISTS;
      case IS_VARIANT_NULL:
        return Operation.IS_NULL;
      case VARIANT_TYPE_OF:
        return Operation.TYPE_OF;
      case VARIANT_TO_JSON:
        return Operation.TO_JSON;
      default:
        throw new IllegalArgumentException("Unsupported Variant function: " + canonicalName);
    }
  }

  private static void validateArgumentCount(Operation operation, int numOperands, String functionName) {
    boolean valid;
    switch (operation) {
      case GET:
      case TRY_GET:
        valid = numOperands == 2 || numOperands == 3;
        break;
      case EXISTS:
        valid = numOperands == 2;
        break;
      case IS_NULL:
      case TYPE_OF:
        valid = numOperands == 1 || numOperands == 2;
        break;
      case TO_JSON:
        valid = numOperands == 1;
        break;
      default:
        throw new IllegalStateException("Unhandled Variant operation: " + operation);
    }
    Preconditions.checkArgument(valid, "Invalid number of arguments for %s: %s", functionName, numOperands);
  }

  private static VariantPath compilePathLiteral(RexExpression operand, String functionName) {
    return VariantUtils.compilePath(stringLiteral(operand, functionName, "path"));
  }

  private static ResultType parseTypeLiteral(RexExpression operand, String functionName) {
    return VariantUtils.parseResultType(stringLiteral(operand, functionName, "target type"));
  }

  private static String stringLiteral(RexExpression operand, String functionName, String role) {
    Preconditions.checkArgument(operand instanceof RexExpression.Literal,
        "%s %s must be a string literal", functionName, role);
    RexExpression.Literal literal = (RexExpression.Literal) operand;
    Preconditions.checkArgument(literal.getDataType() == ColumnDataType.STRING && literal.getValue() instanceof String,
        "%s %s must be a string literal", functionName, role);
    return (String) literal.getValue();
  }

  private enum Operation {
    GET,
    TRY_GET,
    EXISTS,
    IS_NULL,
    TYPE_OF,
    TO_JSON
  }
}
