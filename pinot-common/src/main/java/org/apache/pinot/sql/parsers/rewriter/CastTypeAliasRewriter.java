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

import com.google.common.base.Preconditions;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.PinotQuery;


///
/// Rewrites CAST function type aliases from Calcite type names to Pinot type names:
/// - BIGINT -> LONG
/// - VARCHAR -> STRING
/// - VARBINARY -> BYTES
///
/// Required for backward compatibility.
public class CastTypeAliasRewriter implements QueryRewriter {

  @Override
  public PinotQuery rewrite(PinotQuery pinotQuery) {
    for (int i = 0; i < pinotQuery.getSelectListSize(); i++) {
      Expression expression = rewriteCastTypeAlias(pinotQuery.getSelectList().get(i));
      pinotQuery.getSelectList().set(i, expression);
    }
    for (int i = 0; i < pinotQuery.getGroupByListSize(); i++) {
      Expression expression = rewriteCastTypeAlias(pinotQuery.getGroupByList().get(i));
      pinotQuery.getGroupByList().set(i, expression);
    }
    for (int i = 0; i < pinotQuery.getOrderByListSize(); i++) {
      Expression expression = rewriteCastTypeAlias(pinotQuery.getOrderByList().get(i));
      pinotQuery.getOrderByList().set(i, expression);
    }
    Expression filterExpression = rewriteCastTypeAlias(pinotQuery.getFilterExpression());
    pinotQuery.setFilterExpression(filterExpression);
    Expression havingExpression = rewriteCastTypeAlias(pinotQuery.getHavingExpression());
    pinotQuery.setHavingExpression(havingExpression);
    return pinotQuery;
  }

  public static Expression rewriteCastTypeAlias(Expression expression) {
    if (expression == null || expression.getFunctionCall() == null) {
      return expression;
    }

    if (expression.getFunctionCall().getOperator().equalsIgnoreCase("CAST")) {
      Preconditions.checkState(expression.getFunctionCall().getOperandsSize() == 2,
          "CAST function must have exactly 2 operands");
      Expression castType = expression.getFunctionCall().getOperands().get(1);
      Preconditions.checkState(castType.isSetLiteral(), "CAST function's 2nd operand must be a literal");
      switch (castType.getLiteral().getStringValue().toUpperCase()) {
        case "BIGINT":
          castType.getLiteral().setStringValue("LONG");
          break;
        case "VARCHAR":
          castType.getLiteral().setStringValue("STRING");
          break;
        case "VARBINARY":
          castType.getLiteral().setStringValue("BYTES");
          break;
        default:
      }
    } else {
      for (int i = 0; i < expression.getFunctionCall().getOperandsSize(); i++) {
        Expression operand = rewriteCastTypeAlias(expression.getFunctionCall().getOperands().get(i));
        expression.getFunctionCall().getOperands().set(i, operand);
      }
    }
    return expression;
  }
}
