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
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import javax.annotation.Nullable;
import org.apache.pinot.common.function.FunctionInfo;
import org.apache.pinot.common.function.FunctionRegistry;
import org.apache.pinot.common.function.FunctionUtils;
import org.apache.pinot.common.function.QueryFunctionInvoker;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.runtime.operator.utils.TypeUtils;
import org.apache.pinot.spi.utils.PinotDataType;


/*
 * FunctionOperands are generated from {@link RexExpression}s.
 */
public class FunctionOperand implements TransformOperand {
  private final ColumnDataType _resultType;
  private final QueryFunctionInvoker _functionInvoker;
  private final ColumnDataType _functionInvokerResultType;
  private final boolean _needsConversion;
  private final List<TransformOperand> _operands;
  private final Object[] _reusableOperandHolder;
  private final boolean _replaceNullJsonOperands;

  public FunctionOperand(RexExpression.FunctionCall functionCall, DataSchema dataSchema) {
    this(functionCall, dataSchema, false);
  }

  public FunctionOperand(RexExpression.FunctionCall functionCall, DataSchema dataSchema,
      boolean nullHandlingEnabled) {
    _resultType = functionCall.getDataType();
    List<RexExpression> operands = functionCall.getFunctionOperands();
    int numOperands = operands.size();
    ColumnDataType[] argumentTypes = new ColumnDataType[numOperands];
    for (int i = 0; i < numOperands; i++) {
      RexExpression operand = operands.get(i);
      ColumnDataType argumentType;
      if (operand instanceof RexExpression.InputRef) {
        argumentType = dataSchema.getColumnDataType(((RexExpression.InputRef) operand).getIndex());
      } else if (operand instanceof RexExpression.Literal) {
        argumentType = ((RexExpression.Literal) operand).getDataType();
      } else {
        assert operand instanceof RexExpression.FunctionCall;
        argumentType = ((RexExpression.FunctionCall) operand).getDataType();
      }
      argumentTypes[i] = argumentType;
    }
    String functionName = functionCall.getFunctionName();
    String canonicalName = FunctionRegistry.canonicalize(functionName);
    _replaceNullJsonOperands = !nullHandlingEnabled && isJsonExtractScalar(canonicalName);
    FunctionInfo functionInfo = FunctionRegistry.lookupFunctionInfo(canonicalName, argumentTypes);
    if (functionInfo == null) {
      if (FunctionRegistry.contains(canonicalName)) {
        throw new IllegalArgumentException(
            String.format("Unsupported function: %s with argument types: %s", functionName,
                Arrays.toString(argumentTypes)));
      } else {
        throw new IllegalArgumentException(String.format("Unsupported function: %s", functionName));
      }
    }
    _functionInvoker = new QueryFunctionInvoker(functionInfo);
    if (!_functionInvoker.getMethod().isVarArgs()) {
      Class<?>[] parameterClasses = _functionInvoker.getParameterClasses();
      PinotDataType[] parameterTypes = _functionInvoker.getParameterTypes();
      boolean needsConversion = false;
      for (int i = 0; i < numOperands; i++) {
        Preconditions.checkState(parameterTypes[i] != null, "Unsupported parameter class: %s for method: %s",
            parameterClasses[i], functionInfo.getMethod());
        if (!needsConversion) {
          // For array-typed parameters, always require conversion: the runtime Java class may
          // differ from the canonical stored type (e.g. Double[] vs double[] after DataBlock
          // deserialization in the multi-stage engine), and Method.invoke does not autobox arrays.
          ColumnDataType parameterColumnType = FunctionUtils.getColumnDataType(parameterClasses[i]);
          if (parameterColumnType == null || argumentTypes[i] != parameterColumnType
              || parameterClasses[i].isArray()) {
            needsConversion = true;
          }
        }
      }
      _needsConversion = needsConversion;
    } else {
      _needsConversion = false;
    }
    ColumnDataType functionInvokerResultType = FunctionUtils.getColumnDataType(_functionInvoker.getResultClass());
    // Handle unrecognized result class with STRING
    _functionInvokerResultType = functionInvokerResultType != null ? functionInvokerResultType : ColumnDataType.STRING;
    _operands = new ArrayList<>(numOperands);
    for (RexExpression operand : operands) {
      _operands.add(TransformOperandFactory.getTransformOperand(operand, dataSchema, nullHandlingEnabled));
    }
    _reusableOperandHolder = new Object[numOperands];
  }

  @Override
  public ColumnDataType getResultType() {
    return _resultType;
  }

  @Nullable
  @Override
  public Object apply(List<Object> row) {
    for (int i = 0; i < _operands.size(); i++) {
      TransformOperand operand = _operands.get(i);
      Object value = operand.apply(row);
      _reusableOperandHolder[i] = value != null ? operand.getResultType().toExternal(value) : null;
    }
    if (_replaceNullJsonOperands) {
      if (_reusableOperandHolder[0] == null) {
        ColumnDataType inputType = _operands.get(0).getResultType();
        Object nullPlaceholder = inputType.getNullPlaceholder();
        // An untyped SQL NULL has UNKNOWN type and therefore no generic placeholder. The leaf transform reads every
        // non-BYTES JSON input through transformToStringValuesSV(), whose NULL literal value is the empty string.
        _reusableOperandHolder[0] = nullPlaceholder != null ? inputType.toExternal(nullPlaceholder) : "";
      }
      if (_reusableOperandHolder.length == 4 && _reusableOperandHolder[3] == null
          && _reusableOperandHolder[2] != null) {
        _reusableOperandHolder[3] = getJsonNullDefault(_reusableOperandHolder[2].toString());
      }
    }
    Object result;
    if (_functionInvoker.getMethod().isVarArgs()) {
      result = _functionInvoker.invoke(new Object[]{_reusableOperandHolder});
    } else {
      if (_needsConversion) {
        _functionInvoker.convertTypes(_reusableOperandHolder);
      }
      result = _functionInvoker.invoke(_reusableOperandHolder);
    }
    return result != null ? TypeUtils.convert(_functionInvokerResultType.toInternal(result),
        _resultType.getStoredType()) : null;
  }

  private static boolean isJsonExtractScalar(String canonicalName) {
    return canonicalName.equals("jsonextractscalar") || canonicalName.equals("jsonextractscalarfast")
        || canonicalName.equals("jsonextractscalarfirstmatch") || canonicalName.equals("jsonextractscalarfory");
  }

  @Nullable
  private static Object getJsonNullDefault(String resultsType) {
    String baseType = resultsType.toUpperCase(Locale.ROOT);
    if (baseType.endsWith("_ARRAY")) {
      baseType = baseType.substring(0, baseType.length() - 6);
    }
    switch (baseType) {
      case "INT":
      case "BOOLEAN":
        return 0;
      case "LONG":
      case "TIMESTAMP":
        return 0L;
      case "FLOAT":
        return 0F;
      case "DOUBLE":
        return 0D;
      case "BIG_DECIMAL":
        return BigDecimal.ZERO;
      case "STRING":
      case "JSON":
        return "";
      case "BYTES":
        return new byte[0];
      default:
        return null;
    }
  }
}
