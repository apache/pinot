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
package org.apache.pinot.sql.parsers.rewriter;

import com.google.common.annotations.VisibleForTesting;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.function.FunctionInfo;
import org.apache.pinot.common.function.FunctionRegistry;
import org.apache.pinot.common.function.QueryFunctionInvoker;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.sql.parsers.SqlCompilationException;


public class CompileTimeFunctionsInvoker implements QueryRewriter {

  @Override
  public PinotQuery rewrite(PinotQuery pinotQuery) {
    for (int i = 0; i < pinotQuery.getSelectListSize(); i++) {
      Expression expression = invokeCompileTimeFunctionExpression(pinotQuery.getSelectList().get(i));
      pinotQuery.getSelectList().set(i, expression);
    }
    for (int i = 0; i < pinotQuery.getGroupByListSize(); i++) {
      Expression expression = invokeCompileTimeFunctionExpression(pinotQuery.getGroupByList().get(i));
      pinotQuery.getGroupByList().set(i, expression);
    }
    for (int i = 0; i < pinotQuery.getOrderByListSize(); i++) {
      Expression expression = invokeCompileTimeFunctionExpression(pinotQuery.getOrderByList().get(i));
      pinotQuery.getOrderByList().set(i, expression);
    }
    Expression filterExpression = invokeCompileTimeFunctionExpression(pinotQuery.getFilterExpression());
    pinotQuery.setFilterExpression(filterExpression);
    Expression havingExpression = invokeCompileTimeFunctionExpression(pinotQuery.getHavingExpression());
    pinotQuery.setHavingExpression(havingExpression);
    return pinotQuery;
  }

  @VisibleForTesting
  public static Expression invokeCompileTimeFunctionExpression(@Nullable Expression expression) {
    if (expression == null || expression.getFunctionCall() == null) {
      return expression;
    }
    Function function = expression.getFunctionCall();
    String canonicalName = FunctionRegistry.canonicalize(function.getOperator());
    boolean isArrayConstructor = canonicalName.equals("arrayvalueconstructor") || canonicalName.equals("array");

    List<Expression> operands = function.getOperands();
    int numOperands = operands.size();
    boolean compilable = true;
    ColumnDataType[] argumentTypes = new ColumnDataType[numOperands];
    Object[] arguments = new Object[numOperands];
    for (int i = 0; i < numOperands; i++) {
      Expression operand = operands.get(i);
      // For array constructors, avoid folding CAST to TIMESTAMP or UUID to prevent lowering to LONG or BYTES literal
      if (isArrayConstructor && isCastToTimestampOrUuid(operand)) {
        operands.set(i, compileCastOperand(operand));
        compilable = false;
        continue;
      }
      operand = invokeCompileTimeFunctionExpression(operand);
      operands.set(i, operand);
      Literal literal = operand.getLiteral();
      if (compilable && literal != null) {
        Pair<ColumnDataType, Object> typeAndValue = RequestUtils.getLiteralTypeAndValue(literal);
        argumentTypes[i] = typeAndValue.getLeft();
        arguments[i] = typeAndValue.getRight();
      } else {
        // NOTE: Do not directly 'return expression;' here because we want to compile all operands even if the current
        //       expression is not compilable.
        compilable = false;
      }
    }
    if (!compilable) {
      return expression;
    }
    if (isArrayConstructor) {
      for (ColumnDataType argumentType : argumentTypes) {
        if (!canFoldArrayType(argumentType)) {
          return expression;
        }
      }
    }
    FunctionInfo functionInfo = FunctionRegistry.lookupFunctionInfo(canonicalName, argumentTypes);
    if (functionInfo == null || !functionInfo.isDeterministic()) {
      return expression;
    }
    try {
      QueryFunctionInvoker invoker = new QueryFunctionInvoker(functionInfo);
      Object result;
      if (invoker.getMethod().isVarArgs()) {
        result = invoker.invoke(new Object[]{arguments});
      } else {
        invoker.convertTypes(arguments);
        result = invoker.invoke(arguments);
      }
      return RequestUtils.getLiteralExpression(result);
    } catch (Exception e) {
      throw new SqlCompilationException(
          "Caught exception while invoking method: " + functionInfo.getMethod().getName() + " with arguments: "
              + Arrays.toString(arguments) + ": " + e.getMessage(), e);
    }
  }

  private static boolean isCastToTimestampOrUuid(Expression expression) {
    if (expression.getFunctionCall() == null) {
      return false;
    }
    Function function = expression.getFunctionCall();
    if (!FunctionRegistry.canonicalize(function.getOperator()).equals("cast")) {
      return false;
    }
    List<Expression> operands = function.getOperands();
    if (operands == null || operands.size() != 2) {
      return false;
    }
    Literal targetTypeLiteral = operands.get(1).getLiteral();
    if (targetTypeLiteral == null || !targetTypeLiteral.isSetStringValue()) {
      return false;
    }
    String targetType = targetTypeLiteral.getStringValue().toUpperCase();
    return targetType.equals("TIMESTAMP") || targetType.equals("UUID");
  }

  private static Expression compileCastOperand(Expression castExpression) {
    Function function = castExpression.getFunctionCall();
    List<Expression> operands = function.getOperands();
    operands.set(0, invokeCompileTimeFunctionExpression(operands.get(0)));
    return castExpression;
  }

  private static boolean canFoldArrayType(ColumnDataType type) {
    switch (type) {
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case STRING:
      case BYTES:
        return true;
      default:
        return false;
    }
  }
}
