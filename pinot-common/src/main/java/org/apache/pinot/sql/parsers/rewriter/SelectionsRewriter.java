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

import java.util.List;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.PinotQuery;


public class SelectionsRewriter implements QueryRewriter {
  @Override
  public PinotQuery rewrite(PinotQuery pinotQuery) {
    for (Expression expression : pinotQuery.getSelectList()) {
      // Rewrite aggregation
      tryToRewriteArrayFunction(expression);
    }
    return pinotQuery;
  }

  private static void tryToRewriteArrayFunction(Expression expression) {
    Function function = expression.getFunctionCall();
    if (function == null) {
      return;
    }
    String functionName = function.getOperator();
    List<Expression> operands = function.getOperands();
    switch (functionName) {
      case "sum":
        if (operands.size() != 1) {
          return;
        }
        Function innerFunction = operands.get(0).getFunctionCall();
        if (innerFunction != null && innerFunction.getOperator().equals("arraysum")) {
          Function sumMvFunc = new Function("summv");
          sumMvFunc.setOperands(innerFunction.getOperands());
          expression.setFunctionCall(sumMvFunc);
        }
        return;
      case "min":
        if (operands.size() != 1) {
          return;
        }
        innerFunction = operands.get(0).getFunctionCall();
        if (innerFunction != null && innerFunction.getOperator().equals("arraymin")) {
          Function minMvFunc = new Function("minmv");
          minMvFunc.setOperands(innerFunction.getOperands());
          expression.setFunctionCall(minMvFunc);
        }
        return;
      case "max":
        if (operands.size() != 1) {
          return;
        }
        innerFunction = operands.get(0).getFunctionCall();
        if (innerFunction != null && innerFunction.getOperator().equals("arraymax")) {
          Function maxMvFunc = new Function("maxmv");
          maxMvFunc.setOperands(innerFunction.getOperands());
          expression.setFunctionCall(maxMvFunc);
        }
        return;
      default:
        break;
    }
    for (Expression operand : operands) {
      tryToRewriteArrayFunction(operand);
    }
  }
}
