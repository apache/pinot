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
package org.apache.pinot.core.query.request.context.utils;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.collections.CollectionUtils;
import org.apache.pinot.common.request.DataSource;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.Join;
import org.apache.pinot.common.request.JoinType;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.core.query.request.context.DataSourceContext;
import org.apache.pinot.core.query.request.context.JoinContext;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.sql.parsers.CalciteSqlParser;


public class QueryContextConverterUtils {
  private QueryContextConverterUtils() {
  }

  /**
   * Converts the given query into a {@link QueryContext}.
   */
  public static QueryContext getQueryContext(String query) {
    return getQueryContext(CalciteSqlParser.compileToPinotQuery(query));
  }

  /**
   * Converts the given {@link PinotQuery} into a {@link QueryContext}.
   */
  public static QueryContext getQueryContext(PinotQuery pinotQuery) {
    // FROM
    DataSourceContext dataSource = getDataSourceContext(pinotQuery.getDataSource());

    // SELECT
    List<ExpressionContext> selectExpressions;
    List<Expression> selectList = pinotQuery.getSelectList();
    List<String> aliasList = new ArrayList<>(selectList.size());
    selectExpressions = new ArrayList<>(selectList.size());
    for (Expression thriftExpression : selectList) {
      // Handle alias
      Expression expressionWithoutAlias = thriftExpression;
      if (thriftExpression.getType() == ExpressionType.FUNCTION) {
        Function function = thriftExpression.getFunctionCall();
        List<Expression> operands = function.getOperands();
        switch (function.getOperator().toUpperCase()) {
          case "AS":
            expressionWithoutAlias = operands.get(0);
            aliasList.add(operands.get(1).getIdentifier().getName());
            break;
          case "DISTINCT":
            int numOperands = operands.size();
            for (int i = 0; i < numOperands; i++) {
              Expression operand = operands.get(i);
              Function operandFunction = operand.getFunctionCall();
              if (operandFunction != null && operandFunction.getOperator().equalsIgnoreCase("AS")) {
                operands.set(i, operandFunction.getOperands().get(0));
                aliasList.add(operandFunction.getOperands().get(1).getIdentifier().getName());
              } else {
                aliasList.add(null);
              }
            }
            break;
          default:
            // Add null as a placeholder for alias.
            aliasList.add(null);
            break;
        }
      } else {
        // Add null as a placeholder for alias.
        aliasList.add(null);
      }
      selectExpressions.add(RequestContextUtils.getExpression(expressionWithoutAlias));
    }

    // WHERE
    FilterContext filter = null;
    Expression filterExpression = pinotQuery.getFilterExpression();
    if (filterExpression != null) {
      JoinContext join = dataSource.getJoin();
      String columnPrefix = join != null ? join.getRawLeftTableName() + "." : null;
      filter = RequestContextUtils.getFilter(filterExpression, columnPrefix);
    }

    // GROUP BY
    List<ExpressionContext> groupByExpressions = null;
    List<Expression> groupByList = pinotQuery.getGroupByList();
    if (CollectionUtils.isNotEmpty(groupByList)) {
      groupByExpressions = new ArrayList<>(groupByList.size());
      for (Expression thriftExpression : groupByList) {
        groupByExpressions.add(RequestContextUtils.getExpression(thriftExpression));
      }
    }

    // ORDER BY
    List<OrderByExpressionContext> orderByExpressions = null;
    List<Expression> orderByList = pinotQuery.getOrderByList();
    if (CollectionUtils.isNotEmpty(orderByList)) {
      // Deduplicate the order-by expressions
      orderByExpressions = new ArrayList<>(orderByList.size());
      Set<ExpressionContext> expressionSet = new HashSet<>();
      for (Expression orderBy : orderByList) {
        // NOTE: Order-by is always a Function with the ordering of the Expression
        Function thriftFunction = orderBy.getFunctionCall();
        ExpressionContext expression = RequestContextUtils.getExpression(thriftFunction.getOperands().get(0));
        if (expressionSet.add(expression)) {
          boolean isAsc = thriftFunction.getOperator().equalsIgnoreCase("ASC");
          orderByExpressions.add(new OrderByExpressionContext(expression, isAsc));
        }
      }
    }

    // HAVING
    FilterContext havingFilter = null;
    Expression havingExpression = pinotQuery.getHavingExpression();
    if (havingExpression != null) {
      havingFilter = RequestContextUtils.getFilter(havingExpression);
    }

    // EXPRESSION OVERRIDE HINTS
    Map<ExpressionContext, ExpressionContext> expressionContextOverrideHints = new HashMap<>();
    Map<Expression, Expression> expressionOverrideHints = pinotQuery.getExpressionOverrideHints();
    if (expressionOverrideHints != null) {
      for (Map.Entry<Expression, Expression> entry : expressionOverrideHints.entrySet()) {
        expressionContextOverrideHints.put(RequestContextUtils.getExpression(entry.getKey()),
            RequestContextUtils.getExpression(entry.getValue()));
      }
    }

    return new QueryContext.Builder().setDataSource(dataSource).setSelectExpressions(selectExpressions)
        .setAliasList(aliasList).setFilter(filter).setGroupByExpressions(groupByExpressions)
        .setOrderByExpressions(orderByExpressions).setHavingFilter(havingFilter).setLimit(pinotQuery.getLimit())
        .setOffset(pinotQuery.getOffset()).setQueryOptions(pinotQuery.getQueryOptions())
        .setExpressionOverrideHints(expressionContextOverrideHints).setExplain(pinotQuery.isExplain()).build();
  }

  private static DataSourceContext getDataSourceContext(DataSource dataSource) {
    String tableName = dataSource.getTableName();
    QueryContext subquery = null;
    if (dataSource.getSubquery() != null) {
      subquery = getQueryContext(dataSource.getSubquery());
    }
    JoinContext joinContext = null;
    if (dataSource.getJoin() != null) {
      Join join = dataSource.getJoin();
      Preconditions.checkArgument(join.getType() == JoinType.INNER, "Only INNER JOIN is supported");

      DataSourceContext leftDataSource = getDataSourceContext(join.getLeft());
      Preconditions.checkArgument(isSimpleTable(leftDataSource), "Left side of JOIN must be a table, got: %s",
          leftDataSource);
      String leftTableName = leftDataSource.getTableName();
      String leftTableColumnPrefix = leftTableName + ".";

      DataSourceContext rightDataSource = getDataSourceContext(join.getRight());
      Preconditions.checkArgument(isSimpleTable(rightDataSource), "Right side of JOIN must be a table, got: %s",
          rightDataSource);
      String rightTableName = rightDataSource.getTableName();
      String rightTableColumnPrefix = rightTableName + ".";

      Expression condition = join.getCondition();
      Preconditions.checkArgument(condition != null, "JOIN condition must be specified");
      Function function = condition.getFunctionCall();
      Preconditions.checkArgument(function != null && function.getOperator().equals("EQUALS"),
          "Only EQ JOIN condition is supported, got: %s", condition);
      List<Expression> operands = function.getOperands();
      ExpressionContext firstJoinKey = RequestContextUtils.getExpression(operands.get(0));
      ExpressionContext secondJoinKey = RequestContextUtils.getExpression(operands.get(1));
      ExpressionContext leftJoinKey;
      String rightJoinKey;
      if (isRightTableColumn(secondJoinKey, rightTableColumnPrefix)) {
        leftJoinKey = getLeftJoinKey(firstJoinKey, leftTableColumnPrefix);
        rightJoinKey = getRightJoinKey(secondJoinKey, rightTableColumnPrefix);
      } else {
        Preconditions.checkArgument(isRightTableColumn(firstJoinKey, rightTableColumnPrefix),
            "Failed to find JOIN key for right table");
        leftJoinKey = getLeftJoinKey(secondJoinKey, leftTableColumnPrefix);
        rightJoinKey = getRightJoinKey(firstJoinKey, rightTableColumnPrefix);
      }
      joinContext = new JoinContext(leftTableName, rightTableName, leftJoinKey, rightJoinKey);
    }
    return new DataSourceContext(tableName, subquery, joinContext);
  }

  private static boolean isSimpleTable(DataSourceContext dataSource) {
    return dataSource.getTableName() != null && dataSource.getSubquery() == null && dataSource.getJoin() == null;
  }

  private static boolean isRightTableColumn(ExpressionContext expression, String rightTableColumnPrefix) {
    return expression.getIdentifier() != null && expression.getIdentifier().startsWith(rightTableColumnPrefix);
  }

  private static ExpressionContext getLeftJoinKey(ExpressionContext expression, String leftTableColumnPrefix) {
    switch (expression.getType()) {
      case LITERAL:
        return expression;
      case IDENTIFIER:
        String identifier = expression.getIdentifier();
        if (identifier.equals("*")) {
          return expression;
        } else {
          Preconditions.checkArgument(identifier.startsWith(leftTableColumnPrefix),
              "Column: %s does not have left table prefix: %s", identifier, leftTableColumnPrefix);
          return ExpressionContext.forIdentifier(identifier.substring(leftTableColumnPrefix.length()));
        }
      case FUNCTION:
        FunctionContext function = expression.getFunction();
        function.getArguments().replaceAll(argument -> getLeftJoinKey(argument, leftTableColumnPrefix));
        return expression;
      default:
        throw new IllegalStateException();
    }
  }

  private static String getRightJoinKey(ExpressionContext expressionContext, String rightTableColumnPrefix) {
    return expressionContext.getIdentifier().substring(rightTableColumnPrefix.length());
  }
}
