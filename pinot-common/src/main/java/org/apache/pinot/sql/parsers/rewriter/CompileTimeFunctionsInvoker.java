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

import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.function.FunctionInfo;
import org.apache.pinot.common.function.FunctionInvoker;
import org.apache.pinot.common.function.FunctionRegistry;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.common.request.PinotQuery;
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

  protected static Expression invokeCompileTimeFunctionExpression(@Nullable Expression expression) {
    if (expression == null || expression.getFunctionCall() == null) {
      return expression;
    }
    Function function = expression.getFunctionCall();
    List<Expression> operands = function.getOperands();
    int numOperands = operands.size();
    boolean compilable = true;
    for (int i = 0; i < numOperands; i++) {
      Expression operand = invokeCompileTimeFunctionExpression(operands.get(i));
      if (operand.getLiteral() == null) {
        compilable = false;
      }
      operands.set(i, operand);
    }
    String functionName = function.getOperator();
    if (compilable) {
      FunctionInfo functionInfo = FunctionRegistry.getFunctionInfo(functionName, numOperands);
      if (functionInfo != null) {
        Object[] arguments = new Object[numOperands];
        for (int i = 0; i < numOperands; i++) {
          Literal literal = function.getOperands().get(i).getLiteral();
          if (literal.isSetNullValue()) {
            arguments[i] = null;
          } else {
            arguments[i] = function.getOperands().get(i).getLiteral().getFieldValue();
          }
        }
        try {
          FunctionInvoker invoker = new FunctionInvoker(functionInfo);
          invoker.convertTypes(arguments);
          Object result = invoker.invoke(arguments);
          return RequestUtils.getLiteralExpression(result);
        } catch (Exception e) {
          throw new SqlCompilationException(
              "Caught exception while invoking method: " + functionInfo.getMethod() + " with arguments: "
                  + Arrays.toString(arguments), e);
        }
      }
    }
    return expression;
  }
}
