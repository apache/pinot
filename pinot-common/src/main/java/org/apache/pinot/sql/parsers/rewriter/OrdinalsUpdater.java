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

import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.spi.auth.request.Expression;
import org.apache.pinot.spi.auth.request.Function;
import org.apache.pinot.spi.auth.request.Literal;
import org.apache.pinot.spi.auth.request.PinotQuery;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.sql.parsers.SqlCompilationException;


public class OrdinalsUpdater implements QueryRewriter {
  @Override
  public PinotQuery rewrite(PinotQuery pinotQuery) {
    // handle GROUP BY clause
    for (int i = 0; i < pinotQuery.getGroupByListSize(); i++) {
      Expression groupByExpr = pinotQuery.getGroupByList().get(i);
      Literal literal = groupByExpr.getLiteral();
      if (literal != null && literal.isSetIntValue()) {
        int ordinal = literal.getIntValue();
        pinotQuery.getGroupByList().set(i, getExpressionFromOrdinal(pinotQuery.getSelectList(), ordinal));
      }
    }

    // handle ORDER BY clause
    for (int i = 0; i < pinotQuery.getOrderByListSize(); i++) {
      Expression orderByExpr = pinotQuery.getOrderByList().get(i);
      Literal literal = CalciteSqlParser.removeOrderByFunctions(orderByExpr).getLiteral();
      if (literal != null && literal.isSetIntValue()) {
        int ordinal = literal.getIntValue();
        Function functionToSet = orderByExpr.getFunctionCall();
        if (CalciteSqlParser.isNullsLast(orderByExpr) != null) {
          functionToSet = functionToSet.getOperands().get(0).getFunctionCall();
        }
        // NOTE: Create an ArrayList because we might need to modify the list later
        List<Expression> newOperands = new ArrayList<>(1);
        newOperands.add(getExpressionFromOrdinal(pinotQuery.getSelectList(), ordinal));
        functionToSet.setOperands(newOperands);
      }
    }
    return pinotQuery;
  }

  private static Expression getExpressionFromOrdinal(List<Expression> selectList, int ordinal) {
    if (ordinal > 0 && ordinal <= selectList.size()) {
      final Expression expression = selectList.get(ordinal - 1);
      // If the expression has AS, return the left operand.
      if (expression.isSetFunctionCall() && expression.getFunctionCall().getOperator().equals("as")) {
        return expression.getFunctionCall().getOperands().get(0);
      }
      return expression;
    } else {
      throw new SqlCompilationException(
          String.format("Expected Ordinal value to be between 1 and %d.", selectList.size()));
    }
  }
}
