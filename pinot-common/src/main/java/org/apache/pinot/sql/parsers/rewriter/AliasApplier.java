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
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.Identifier;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.sql.parsers.SqlCompilationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AliasApplier implements QueryRewriter {
  private static final Logger LOGGER = LoggerFactory.getLogger(AliasApplier.class);

  @Override
  public PinotQuery rewrite(PinotQuery pinotQuery) {
    Map<String, Expression> aliasMap = extractAlias(pinotQuery.getSelectList());
    applyAlias(aliasMap, pinotQuery);
    return pinotQuery;
  }

  private static Map<String, Expression> extractAlias(List<Expression> selectExpressions) {
    Map<String, Expression> aliasMap = new HashMap<>();
    for (Expression expression : selectExpressions) {
      Function function = expression.getFunctionCall();
      if (function == null || !function.getOperator().equals("as")) {
        continue;
      }
      String alias = function.getOperands().get(1).getIdentifier().getName();
      if (aliasMap.put(alias, function.getOperands().get(0)) != null) {
        throw new SqlCompilationException("Find duplicate alias: " + alias);
      }
    }
    return aliasMap;
  }

  private static void applyAlias(Map<String, Expression> aliasMap, PinotQuery pinotQuery) {
    Expression filterExpression = pinotQuery.getFilterExpression();
    if (filterExpression != null) {
      applyAliasForFilter(aliasMap, filterExpression);
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

  private static void applyAlias(Map<String, Expression> aliasMap, Expression expression) {
    Identifier identifier = expression.getIdentifier();
    if (identifier != null) {
      Expression aliasExpression = aliasMap.get(identifier.getName());
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

  private static void applyAliasForFilter(Map<String, Expression> aliasMap, Expression expression) {
    Identifier identifier = expression.getIdentifier();
    if (identifier != null) {
      Expression aliasExpression = aliasMap.get(identifier.getName());
      if (aliasExpression != null) {
        LOGGER.error("LEGACY QUERY: Cannot apply alias for filter: {}", expression);
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
        applyAliasForFilter(aliasMap, operand);
      }
    }
  }
}
