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
import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.common.function.FunctionInfo;
import org.apache.pinot.common.function.FunctionInvoker;
import org.apache.pinot.common.function.FunctionRegistry;
import org.apache.pinot.common.function.FunctionUtils;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.runtime.operator.utils.OperatorUtils;

/*
 * FunctionOperands are generated from {@link RexExpression}s.
 */
public class FunctionOperand extends TransformOperand {
  private final List<TransformOperand> _childOperandList;
  private final FunctionInvoker _functionInvoker;
  private final Object[] _reusableOperandHolder;

  public FunctionOperand(RexExpression.FunctionCall functionCall, DataSchema dataSchema) {
    // iteratively resolve child operands.
    List<RexExpression> operandExpressions = functionCall.getFunctionOperands();
    _childOperandList = new ArrayList<>(operandExpressions.size());
    for (RexExpression childRexExpression : operandExpressions) {
      _childOperandList.add(toTransformOperand(childRexExpression, dataSchema));
    }
    FunctionInfo functionInfo =
        FunctionRegistry.getFunctionInfo(OperatorUtils.canonicalizeFunctionName(functionCall.getFunctionName()),
            operandExpressions.size());
    Preconditions.checkNotNull(functionInfo, "Cannot find function with Name: " + functionCall.getFunctionName());
    _functionInvoker = new FunctionInvoker(functionInfo);
    _resultName = computeColumnName(functionCall.getFunctionName(), _childOperandList);
    _resultType = FunctionUtils.getColumnDataType(_functionInvoker.getResultClass());
    if (functionCall.getDataType() != FunctionUtils.getDataType(_functionInvoker.getResultClass())) {
      _resultType = DataSchema.ColumnDataType.fromDataType(functionCall.getDataType(), true);
    }
    _reusableOperandHolder = new Object[operandExpressions.size()];
  }

  @Override
  public Object apply(Object[] row) {
    for (int i = 0; i < _childOperandList.size(); i++) {
      _reusableOperandHolder[i] = _childOperandList.get(i).apply(row);
    }
    return _functionInvoker.invoke(_reusableOperandHolder);
  }

  private static String computeColumnName(String functionName, List<TransformOperand> childOperands) {
    StringBuilder sb = new StringBuilder();
    sb.append(functionName);
    sb.append("(");
    for (TransformOperand operands : childOperands) {
      sb.append(operands.getResultName());
      sb.append(",");
    }
    sb.append(")");
    return sb.toString();
  }
}
