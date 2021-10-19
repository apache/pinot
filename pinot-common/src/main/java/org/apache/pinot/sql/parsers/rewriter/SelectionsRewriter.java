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

import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.sql.parsers.CalciteSqlParser;


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
    if (!expression.isSetFunctionCall()) {
      return;
    }
    Function functionCall = expression.getFunctionCall();
    switch (CalciteSqlParser.canonicalize(functionCall.getOperator())) {
      case "sum":
        if (functionCall.getOperands().size() != 1) {
          return;
        }
        if (functionCall.getOperands().get(0).isSetFunctionCall()) {
          Function innerFunction = functionCall.getOperands().get(0).getFunctionCall();
          if (CalciteSqlParser.isSameFunction(innerFunction.getOperator(), TransformFunctionType.ARRAYSUM.getName())) {
            Function sumMvFunc = new Function(AggregationFunctionType.SUMMV.getName());
            sumMvFunc.setOperands(innerFunction.getOperands());
            expression.setFunctionCall(sumMvFunc);
          }
        }
        return;
      case "min":
        if (functionCall.getOperands().size() != 1) {
          return;
        }
        if (functionCall.getOperands().get(0).isSetFunctionCall()) {
          Function innerFunction = functionCall.getOperands().get(0).getFunctionCall();
          if (CalciteSqlParser.isSameFunction(innerFunction.getOperator(), TransformFunctionType.ARRAYMIN.getName())) {
            Function sumMvFunc = new Function(AggregationFunctionType.MINMV.getName());
            sumMvFunc.setOperands(innerFunction.getOperands());
            expression.setFunctionCall(sumMvFunc);
          }
        }
        return;
      case "max":
        if (functionCall.getOperands().size() != 1) {
          return;
        }
        if (functionCall.getOperands().get(0).isSetFunctionCall()) {
          Function innerFunction = functionCall.getOperands().get(0).getFunctionCall();
          if (CalciteSqlParser.isSameFunction(innerFunction.getOperator(), TransformFunctionType.ARRAYMAX.getName())) {
            Function sumMvFunc = new Function(AggregationFunctionType.MAXMV.getName());
            sumMvFunc.setOperands(innerFunction.getOperands());
            expression.setFunctionCall(sumMvFunc);
          }
        }
        return;
      default:
        break;
    }
    for (Expression operand : functionCall.getOperands()) {
      tryToRewriteArrayFunction(operand);
    }
  }
}
