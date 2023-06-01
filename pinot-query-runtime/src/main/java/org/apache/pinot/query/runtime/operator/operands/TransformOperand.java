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
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.runtime.operator.utils.OperatorUtils;


public abstract class TransformOperand {
  protected String _resultName;
  protected DataSchema.ColumnDataType _resultType;

  public static TransformOperand toTransformOperand(RexExpression rexExpression, DataSchema inputDataSchema) {
    if (rexExpression instanceof RexExpression.InputRef) {
      return new ReferenceOperand((RexExpression.InputRef) rexExpression, inputDataSchema);
    } else if (rexExpression instanceof RexExpression.FunctionCall) {
      return toTransformOperand((RexExpression.FunctionCall) rexExpression, inputDataSchema);
    } else if (rexExpression instanceof RexExpression.Literal) {
      return new LiteralOperand((RexExpression.Literal) rexExpression);
    } else {
      throw new UnsupportedOperationException("Unsupported RexExpression: " + rexExpression);
    }
  }

  private static TransformOperand toTransformOperand(RexExpression.FunctionCall functionCall,
      DataSchema inputDataSchema) {
    final List<RexExpression> functionOperands = functionCall.getFunctionOperands();
    int operandSize = functionOperands.size();
    switch (OperatorUtils.canonicalizeFunctionName(functionCall.getFunctionName())) {
      case "AND":
        Preconditions.checkState(operandSize >= 2, "AND takes >=2 argument, passed in argument size:" + operandSize);
        return new FilterOperand.And(functionOperands, inputDataSchema);
      case "OR":
        Preconditions.checkState(operandSize >= 2, "OR takes >=2 argument, passed in argument size:" + operandSize);
        return new FilterOperand.Or(functionOperands, inputDataSchema);
      case "ISNOTTRUE":
      case "NOT":
        Preconditions.checkState(operandSize == 1,
            "NOT / IS_NOT_TRUE takes one argument, passed in argument size:" + operandSize);
        return new FilterOperand.Not(functionOperands.get(0), inputDataSchema);
      case "ISTRUE":
        Preconditions.checkState(operandSize == 1,
            "BOOL / IS_TRUE takes one argument, passed in argument size:" + operandSize);
        return new FilterOperand.True(functionOperands.get(0), inputDataSchema);
      case "equals":
        return new FilterOperand.Predicate(functionOperands, inputDataSchema, v -> v == 0);
      case "notEquals":
        return new FilterOperand.Predicate(functionOperands, inputDataSchema, v -> v != 0);
      case "greaterThan":
        return new FilterOperand.Predicate(functionOperands, inputDataSchema, v -> v > 0);
      case "greaterThanOrEqual":
        return new FilterOperand.Predicate(functionOperands, inputDataSchema, v -> v >= 0);
      case "lessThan":
        return new FilterOperand.Predicate(functionOperands, inputDataSchema, v -> v < 0);
      case "lessThanOrEqual":
        return new FilterOperand.Predicate(functionOperands, inputDataSchema, v -> v <= 0);
      default:
        return new FunctionOperand(functionCall, inputDataSchema);
    }
  }

  public String getResultName() {
    return _resultName;
  }

  public DataSchema.ColumnDataType getResultType() {
    return _resultType;
  }

  @Nullable
  public abstract Object apply(Object[] row);
}
