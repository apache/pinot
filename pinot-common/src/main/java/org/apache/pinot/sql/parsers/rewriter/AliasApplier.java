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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.sql.SqlKind;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.Identifier;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.sql.parsers.SqlCompilationException;


public class AliasApplier implements QueryRewriter {
  @Override
  public PinotQuery rewrite(PinotQuery pinotQuery) {

    // Update alias
    Map<Identifier, Expression> aliasMap = extractAlias(pinotQuery.getSelectList());
    applyAlias(aliasMap, pinotQuery);

    // Validate
    validateSelectionClause(aliasMap, pinotQuery);
    return pinotQuery;
  }

  private static Map<Identifier, Expression> extractAlias(List<Expression> expressions) {
    Map<Identifier, Expression> aliasMap = new HashMap<>();
    for (Expression expression : expressions) {
      Function functionCall = expression.getFunctionCall();
      if (functionCall == null) {
        continue;
      }
      if (functionCall.getOperator().equalsIgnoreCase(SqlKind.AS.toString())) {
        Expression identifierExpr = functionCall.getOperands().get(1);
        aliasMap.put(identifierExpr.getIdentifier(), functionCall.getOperands().get(0));
      }
    }
    return aliasMap;
  }

  private static void applyAlias(Map<Identifier, Expression> aliasMap, PinotQuery pinotQuery) {
    Expression filterExpression = pinotQuery.getFilterExpression();
    if (filterExpression != null) {
      applyAlias(aliasMap, filterExpression);
    }
    List<Expression> groupByList = pinotQuery.getGroupByList();
    if (groupByList != null) {
      for (Expression expression : groupByList) {
        applyAlias(aliasMap, expression);
      }
    }
    Expression havingExpression = pinotQuery.getHavingExpression();
    if (havingExpression != null) {
      applyAlias(aliasMap, havingExpression);
    }
    List<Expression> orderByList = pinotQuery.getOrderByList();
    if (orderByList != null) {
      for (Expression expression : orderByList) {
        applyAlias(aliasMap, expression);
      }
    }
  }

  private static void applyAlias(Map<Identifier, Expression> aliasMap, Expression expression) {
    Identifier identifierKey = expression.getIdentifier();
    if (identifierKey != null) {
      Expression aliasExpression = aliasMap.get(identifierKey);
      if (aliasExpression != null) {
        expression.setType(aliasExpression.getType());
        expression.setIdentifier(aliasExpression.getIdentifier());
        expression.setFunctionCall(aliasExpression.getFunctionCall());
        expression.setLiteral(aliasExpression.getLiteral());
      }
      return;
    }
    Function function = expression.getFunctionCall();
    if (function != null) {
      for (Expression operand : function.getOperands()) {
        applyAlias(aliasMap, operand);
      }
    }
  }

  private static void validateSelectionClause(Map<Identifier, Expression> aliasMap, PinotQuery pinotQuery)
      throws SqlCompilationException {
    // Sanity check on selection expression shouldn't use alias reference.
    Set<String> aliasKeys = new HashSet<>();
    for (Identifier identifier : aliasMap.keySet()) {
      String aliasName = identifier.getName().toLowerCase();
      if (!aliasKeys.add(aliasName)) {
        throw new SqlCompilationException("Duplicated alias name found.");
      }
    }
  }
}
