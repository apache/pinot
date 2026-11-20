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
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.logical.RexExpression;


public class TransformOperandFactory {
  private TransformOperandFactory() {
  }

  public static TransformOperand getTransformOperand(RexExpression rexExpression, DataSchema dataSchema) {
    return getTransformOperand(rexExpression, dataSchema, false);
  }

  public static TransformOperand getTransformOperand(RexExpression rexExpression, DataSchema dataSchema,
      boolean nullHandlingEnabled) {
    if (rexExpression instanceof RexExpression.FunctionCall) {
      return getTransformOperand((RexExpression.FunctionCall) rexExpression, dataSchema, nullHandlingEnabled);
    } else if (rexExpression instanceof RexExpression.InputRef) {
      return new ReferenceOperand(((RexExpression.InputRef) rexExpression).getIndex(), dataSchema);
    } else if (rexExpression instanceof RexExpression.Literal) {
      return new LiteralOperand((RexExpression.Literal) rexExpression);
    } else {
      throw new UnsupportedOperationException("Unsupported RexExpression: " + rexExpression);
    }
  }

  private static TransformOperand getTransformOperand(RexExpression.FunctionCall functionCall, DataSchema dataSchema,
      boolean nullHandlingEnabled) {
    List<RexExpression> operands = functionCall.getFunctionOperands();
    int numOperands = operands.size();
    switch (functionCall.getFunctionName()) {
      case "AND":
        Preconditions.checkState(numOperands >= 2, "AND takes >=2 arguments, got: %s", numOperands);
        return new FilterOperand.And(operands, dataSchema, nullHandlingEnabled);
      case "OR":
        Preconditions.checkState(numOperands >= 2, "OR takes >=2 arguments, got: %s", numOperands);
        return new FilterOperand.Or(operands, dataSchema, nullHandlingEnabled);
      case "NOT":
        Preconditions.checkState(numOperands == 1, "NOT takes one argument, got: %s", numOperands);
        return new FilterOperand.Not(operands.get(0), dataSchema, nullHandlingEnabled);
      case "IN":
        Preconditions.checkState(numOperands >= 2, "IN takes >=2 arguments, got: %s", numOperands);
        return new FilterOperand.In(operands, dataSchema, false, nullHandlingEnabled);
      case "NOT_IN":
        Preconditions.checkState(numOperands >= 2, "NOT_IN takes >=2 arguments, got: %s", numOperands);
        return new FilterOperand.In(operands, dataSchema, true, nullHandlingEnabled);
      case "IS_TRUE":
        Preconditions.checkState(numOperands == 1, "IS_TRUE takes one argument, got: %s", numOperands);
        return new FilterOperand.IsTrue(operands.get(0), dataSchema, nullHandlingEnabled);
      case "IS_NOT_TRUE":
        Preconditions.checkState(numOperands == 1, "IS_NOT_TRUE takes one argument, got: %s", numOperands);
        return new FilterOperand.IsNotTrue(operands.get(0), dataSchema, nullHandlingEnabled);
      case "EQUALS":
        return new FilterOperand.Predicate(operands, dataSchema, v -> v == 0, nullHandlingEnabled);
      case "NOT_EQUALS":
        return new FilterOperand.Predicate(operands, dataSchema, v -> v != 0, nullHandlingEnabled);
      case "GREATER_THAN":
        return new FilterOperand.Predicate(operands, dataSchema, v -> v > 0, nullHandlingEnabled);
      case "GREATER_THAN_OR_EQUAL":
        return new FilterOperand.Predicate(operands, dataSchema, v -> v >= 0, nullHandlingEnabled);
      case "LESS_THAN":
        return new FilterOperand.Predicate(operands, dataSchema, v -> v < 0, nullHandlingEnabled);
      case "LESS_THAN_OR_EQUAL":
        return new FilterOperand.Predicate(operands, dataSchema, v -> v <= 0, nullHandlingEnabled);
      default:
        return new FunctionOperand(functionCall, dataSchema, nullHandlingEnabled);
    }
  }
}
