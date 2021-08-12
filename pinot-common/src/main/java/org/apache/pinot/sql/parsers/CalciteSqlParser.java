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
package org.apache.pinot.sql.parsers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSelectKeyword;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.babel.SqlBabelParserImpl;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.function.FunctionDefinitionRegistry;
import org.apache.pinot.common.function.FunctionInfo;
import org.apache.pinot.common.function.FunctionInvoker;
import org.apache.pinot.common.function.FunctionRegistry;
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.common.request.DataSource;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.Identifier;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.pql.parsers.pql2.ast.FilterKind;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CalciteSqlParser {

  private static final Logger LOGGER = LoggerFactory.getLogger(CalciteSqlParser.class);

  /** Lexical policy similar to MySQL with ANSI_QUOTES option enabled. (To be
   * precise: MySQL on Windows; MySQL on Linux uses case-sensitive matching,
   * like the Linux file system.) The case of identifiers is preserved whether
   * or not they quoted; after which, identifiers are matched
   * case-insensitively. Double quotes allow identifiers to contain
   * non-alphanumeric characters. */
  private static final Lex PINOT_LEX = Lex.MYSQL_ANSI;

  // BABEL is a very liberal conformance value that allows anything supported by any dialect
  private static final SqlParser.Config PARSER_CONFIG =
      SqlParser.configBuilder().setLex(PINOT_LEX).setConformance(SqlConformanceEnum.BABEL)
          .setParserFactory(SqlBabelParserImpl.FACTORY).build();

  // To Keep the backward compatibility with 'OPTION' Functionality in PQL, which is used to
  // provide more hints for query processing.
  //
  // PQL syntax is: `OPTION (<key> = <value>)`
  //
  // Multiple OPTIONs is also supported by:
  // either
  //   `OPTION (<k1> = <v1>, <k2> = <v2>, <k3> = <v3>)`
  // or
  //   `OPTION (<k1> = <v1>) OPTION (<k2> = <v2>) OPTION (<k3> = <v3>)`
  private static final Pattern OPTIONS_REGEX_PATTEN =
      Pattern.compile("option\\s*\\(([^\\)]+)\\)", Pattern.CASE_INSENSITIVE);

  public static PinotQuery compileToPinotQuery(String sql)
      throws SqlCompilationException {
    // Extract OPTION statements from sql as Calcite Parser doesn't parse it.
    List<String> options = extractOptionsFromSql(sql);
    if (!options.isEmpty()) {
      sql = removeOptionsFromSql(sql);
    }
    // Compile Sql without OPTION statements.
    PinotQuery pinotQuery = compileCalciteSqlToPinotQuery(sql);

    // Set Option statements to PinotQuery.
    setOptions(pinotQuery, options);
    return pinotQuery;
  }

  static void validate(Map<Identifier, Expression> aliasMap, PinotQuery pinotQuery)
      throws SqlCompilationException {
    validateSelectionClause(aliasMap, pinotQuery);
    validateGroupByClause(pinotQuery);
    validateDistinctQuery(pinotQuery);
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
    for (Expression selectExpr : pinotQuery.getSelectList()) {
      matchIdentifierInAliasMap(selectExpr, aliasKeys);
    }
  }

  private static void matchIdentifierInAliasMap(Expression selectExpr, Set<String> aliasKeys)
      throws SqlCompilationException {
    Function functionCall = selectExpr.getFunctionCall();
    if (functionCall != null) {
      if (functionCall.getOperator().equalsIgnoreCase(SqlKind.AS.toString())) {
        matchIdentifierInAliasMap(functionCall.getOperands().get(0), aliasKeys);
      } else {
        if (functionCall.getOperandsSize() > 0) {
          for (Expression operand : functionCall.getOperands()) {
            matchIdentifierInAliasMap(operand, aliasKeys);
          }
        }
      }
    }
    if (selectExpr.getIdentifier() != null) {
      if (aliasKeys.contains(selectExpr.getIdentifier().getName().toLowerCase())) {
        throw new SqlCompilationException(
            "Alias " + selectExpr.getIdentifier().getName() + " cannot be referred in SELECT Clause");
      }
    }
  }

  private static void validateGroupByClause(PinotQuery pinotQuery)
      throws SqlCompilationException {
    if (pinotQuery.getGroupByList() == null) {
      return;
    }
    // Sanity check group by query: All non-aggregate expression in selection list should be also included in group by list.
    Set<Expression> groupByExprs = new HashSet<>(pinotQuery.getGroupByList());
    for (Expression selectExpression : pinotQuery.getSelectList()) {
      if (!isAggregateExpression(selectExpression) && expressionOutsideGroupByList(selectExpression, groupByExprs)) {
        throw new SqlCompilationException(
            "'" + RequestUtils.prettyPrint(selectExpression) + "' should appear in GROUP BY clause.");
      }
    }
    // Sanity check on group by clause shouldn't contain aggregate expression.
    for (Expression groupByExpression : pinotQuery.getGroupByList()) {
      if (isAggregateExpression(groupByExpression)) {
        throw new SqlCompilationException("Aggregate expression '" + RequestUtils.prettyPrint(groupByExpression)
            + "' is not allowed in GROUP BY clause.");
      }
    }
  }

  /*
   * Validate DISTINCT queries:
   * - No GROUP-BY clause
   * - LIMIT must be positive
   * - ORDER-BY columns (if exist) should be included in the DISTINCT columns
   */
  private static void validateDistinctQuery(PinotQuery pinotQuery)
      throws SqlCompilationException {
    List<Expression> selectList = pinotQuery.getSelectList();
    if (selectList.size() == 1) {
      Function function = selectList.get(0).getFunctionCall();
      if (function != null && function.getOperator().equalsIgnoreCase(AggregationFunctionType.DISTINCT.getName())) {
        if (CollectionUtils.isNotEmpty(pinotQuery.getGroupByList())) {
          // TODO: Explore if DISTINCT should be supported with GROUP BY
          throw new IllegalStateException("DISTINCT with GROUP BY is currently not supported");
        }
        if (pinotQuery.getLimit() <= 0) {
          // TODO: Consider changing it to SELECTION query for LIMIT 0
          throw new IllegalStateException("DISTINCT must have positive LIMIT");
        }
        List<Expression> orderByList = pinotQuery.getOrderByList();
        if (orderByList != null) {
          List<Expression> distinctExpressions = function.getOperands();
          for (Expression orderByExpression : orderByList) {
            // NOTE: Order-by is always a Function with the ordering of the Expression
            if (!distinctExpressions.contains(orderByExpression.getFunctionCall().getOperands().get(0))) {
              throw new IllegalStateException("ORDER-BY columns should be included in the DISTINCT columns");
            }
          }
        }
      }
    }
  }

  /**
   * Check recursively if an expression contains any reference not appearing in the GROUP BY clause.
   */
  private static boolean expressionOutsideGroupByList(Expression expr, Set<Expression> groupByExprs) {
    // return early for Literal, Aggregate and if we have an exact match
    if (expr.getType() == ExpressionType.LITERAL || isAggregateExpression(expr) || groupByExprs.contains(expr)) {
      return false;
    }

    final Function funcExpr = expr.getFunctionCall();
    // function expression
    if (funcExpr != null) {
      // for Alias function, check the actual value
      if (funcExpr.getOperator().equalsIgnoreCase(SqlKind.AS.toString())) {
        return expressionOutsideGroupByList(funcExpr.getOperands().get(0), groupByExprs);
      }
      // Expression is invalid if any of its children is invalid
      return funcExpr.getOperands().stream().anyMatch(e -> expressionOutsideGroupByList(e, groupByExprs));
    }
    return true;
  }

  private static boolean isAggregateExpression(Expression expression) {
    Function functionCall = expression.getFunctionCall();
    if (functionCall != null) {
      String operator = functionCall.getOperator();
      try {
        AggregationFunctionType.getAggregationFunctionType(operator);
        return true;
      } catch (IllegalArgumentException e) {
      }
      if (functionCall.getOperandsSize() > 0) {
        for (Expression operand : functionCall.getOperands()) {
          if (isAggregateExpression(operand)) {
            return true;
          }
        }
      }
    }
    return false;
  }

  /**
   * Extract all the identifiers from given expressions.
   *
   * @param expressions
   * @param excludeAs if true, ignores the right side identifier for AS function.
   * @return all the identifier names.
   */
  public static Set<String> extractIdentifiers(List<Expression> expressions, boolean excludeAs) {
    Set<String> identifiers = new HashSet<>();
    for (Expression expression : expressions) {
      if (expression.getIdentifier() != null) {
        identifiers.add(expression.getIdentifier().getName());
      } else if (expression.getFunctionCall() != null) {
        if (excludeAs && expression.getFunctionCall().getOperator().equalsIgnoreCase("AS")) {
          identifiers
              .addAll(extractIdentifiers(Arrays.asList(expression.getFunctionCall().getOperands().get(0)), true));
          continue;
        } else {
          identifiers.addAll(extractIdentifiers(expression.getFunctionCall().getOperands(), excludeAs));
        }
      }
    }
    return identifiers;
  }

  /**
   * Compiles a String expression into {@link Expression}.
   *
   * @param expression String expression.
   * @return {@link Expression} equivalent of the string.
   *
   * @throws SqlCompilationException if String is not a valid expression.
   */
  public static Expression compileToExpression(String expression) {
    SqlParser sqlParser = SqlParser.create(expression, PARSER_CONFIG);
    SqlNode sqlNode;
    try {
      sqlNode = sqlParser.parseExpression();
    } catch (SqlParseException e) {
      throw new SqlCompilationException("Caught exception while parsing expression: " + expression, e);
    }
    return toExpression(sqlNode);
  }

  private static void setOptions(PinotQuery pinotQuery, List<String> optionsStatements) {
    if (optionsStatements.isEmpty()) {
      return;
    }
    Map<String, String> options = new HashMap<>();
    for (String optionsStatement : optionsStatements) {
      for (String option : optionsStatement.split(",")) {
        final String[] splits = option.split("=");
        if (splits.length != 2) {
          throw new SqlCompilationException("OPTION statement requires two parts separated by '='");
        }
        options.put(splits[0].trim(), splits[1].trim());
      }
    }
    pinotQuery.setQueryOptions(options);
  }

  private static PinotQuery compileCalciteSqlToPinotQuery(String sql) {
    SqlParser sqlParser = SqlParser.create(sql, PARSER_CONFIG);
    SqlNode sqlNode;
    try {
      sqlNode = sqlParser.parseQuery();
    } catch (SqlParseException e) {
      throw new SqlCompilationException("Caught exception while parsing query: " + sql, e);
    }

    SqlSelect selectNode;
    if (sqlNode instanceof SqlOrderBy) {
      // Store order-by info into the select sql node
      SqlOrderBy orderByNode = (SqlOrderBy) sqlNode;
      selectNode = (SqlSelect) orderByNode.query;
      selectNode.setOrderBy(orderByNode.orderList);
      selectNode.setFetch(orderByNode.fetch);
      selectNode.setOffset(orderByNode.offset);
    } else {
      selectNode = (SqlSelect) sqlNode;
    }

    PinotQuery pinotQuery = new PinotQuery();
    // SELECT
    if (selectNode.getModifierNode(SqlSelectKeyword.DISTINCT) != null) {
      // SELECT DISTINCT
      if (selectNode.getGroup() != null) {
        // TODO: explore support for GROUP BY with DISTINCT
        throw new SqlCompilationException("DISTINCT with GROUP BY is not supported");
      }
      pinotQuery.setSelectList(convertDistinctSelectList(selectNode.getSelectList()));
    } else {
      pinotQuery.setSelectList(convertSelectList(selectNode.getSelectList()));
    }
    // FROM
    SqlNode fromNode = selectNode.getFrom();
    if (fromNode != null) {
      DataSource dataSource = new DataSource();
      dataSource.setTableName(fromNode.toString());
      pinotQuery.setDataSource(dataSource);
    }
    // WHERE
    SqlNode whereNode = selectNode.getWhere();
    if (whereNode != null) {
      pinotQuery.setFilterExpression(toExpression(whereNode));
    }
    // GROUP-BY
    SqlNodeList groupByNodeList = selectNode.getGroup();
    if (groupByNodeList != null) {
      pinotQuery.setGroupByList(convertSelectList(groupByNodeList));
    }
    // HAVING
    SqlNode havingNode = selectNode.getHaving();
    if (havingNode != null) {
      pinotQuery.setHavingExpression(toExpression(havingNode));
    }
    // ORDER-BY
    SqlNodeList orderByNodeList = selectNode.getOrderList();
    if (orderByNodeList != null) {
      pinotQuery.setOrderByList(convertOrderByList(orderByNodeList));
    }
    // LIMIT
    SqlNode limitNode = selectNode.getFetch();
    if (limitNode != null) {
      pinotQuery.setLimit(((SqlNumericLiteral) limitNode).intValue(false));
    }
    // OFFSET
    SqlNode offsetNode = selectNode.getOffset();
    if (offsetNode != null) {
      pinotQuery.setOffset(((SqlNumericLiteral) offsetNode).intValue(false));
    }

    queryRewrite(pinotQuery);
    return pinotQuery;
  }

  private static void queryRewrite(PinotQuery pinotQuery) {
    // Invoke compilation time functions
    invokeCompileTimeFunctions(pinotQuery);

    // Rewrite Selection list
    rewriteSelections(pinotQuery.getSelectList());

    // Update Predicate Comparison
    Expression filterExpression = pinotQuery.getFilterExpression();
    if (filterExpression != null) {
      pinotQuery.setFilterExpression(updateComparisonPredicate(filterExpression));
    }
    Expression havingExpression = pinotQuery.getHavingExpression();
    if (havingExpression != null) {
      pinotQuery.setHavingExpression(updateComparisonPredicate(havingExpression));
    }

    // Update Ordinals
    applyOrdinals(pinotQuery);

    // Rewrite GroupBy to Distinct
    rewriteNonAggregationGroupByToDistinct(pinotQuery);

    // Update alias
    Map<Identifier, Expression> aliasMap = extractAlias(pinotQuery.getSelectList());
    applyAlias(aliasMap, pinotQuery);

    // Validate
    validate(aliasMap, pinotQuery);
  }

  private static void applyOrdinals(PinotQuery pinotQuery) {
    // handle GROUP BY clause
    for (int i = 0; i < pinotQuery.getGroupByListSize(); i++) {
      final Expression groupByExpr = pinotQuery.getGroupByList().get(i);
      if (groupByExpr.isSetLiteral() && groupByExpr.getLiteral().isSetLongValue()) {
        final int ordinal = (int) groupByExpr.getLiteral().getLongValue();
        pinotQuery.getGroupByList().set(i, getExpressionFromOrdinal(pinotQuery.getSelectList(), ordinal));
      }
    }

    // handle ORDER BY clause
    for (int i = 0; i < pinotQuery.getOrderByListSize(); i++) {
      final Expression orderByExpr = pinotQuery.getOrderByList().get(i).getFunctionCall().getOperands().get(0);
      if (orderByExpr.isSetLiteral() && orderByExpr.getLiteral().isSetLongValue()) {
        final int ordinal = (int) orderByExpr.getLiteral().getLongValue();
        pinotQuery.getOrderByList().get(i).getFunctionCall()
            .setOperands(Arrays.asList(getExpressionFromOrdinal(pinotQuery.getSelectList(), ordinal)));
      }
    }
  }

  private static Expression getExpressionFromOrdinal(List<Expression> selectList, int ordinal) {
    if (ordinal > 0 && ordinal <= selectList.size()) {
      final Expression expression = selectList.get(ordinal - 1);
      // If the expression has AS, return the left operand.
      if (expression.isSetFunctionCall() && expression.getFunctionCall().getOperator().equals(SqlKind.AS.name())) {
        return expression.getFunctionCall().getOperands().get(0);
      }
      return expression;
    } else {
      throw new SqlCompilationException(
          String.format("Expected Ordinal value to be between 1 and %d.", selectList.size()));
    }
  }

  private static void rewriteSelections(List<Expression> selectList) {
    for (Expression expression : selectList) {
      // Rewrite aggregation
      tryToRewriteArrayFunction(expression);
    }
  }

  private static void tryToRewriteArrayFunction(Expression expression) {
    if (!expression.isSetFunctionCall()) {
      return;
    }
    Function functionCall = expression.getFunctionCall();
    switch (canonicalize(functionCall.getOperator())) {
      case "sum":
        if (functionCall.getOperands().size() != 1) {
          return;
        }
        if (functionCall.getOperands().get(0).isSetFunctionCall()) {
          Function innerFunction = functionCall.getOperands().get(0).getFunctionCall();
          if (isSameFunction(innerFunction.getOperator(), TransformFunctionType.ARRAYSUM.getName())) {
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
          if (isSameFunction(innerFunction.getOperator(), TransformFunctionType.ARRAYMIN.getName())) {
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
          if (isSameFunction(innerFunction.getOperator(), TransformFunctionType.ARRAYMAX.getName())) {
            Function sumMvFunc = new Function(AggregationFunctionType.MAXMV.getName());
            sumMvFunc.setOperands(innerFunction.getOperands());
            expression.setFunctionCall(sumMvFunc);
          }
        }
        return;
    }
    for (Expression operand : functionCall.getOperands()) {
      tryToRewriteArrayFunction(operand);
    }
  }

  private static String canonicalize(String functionName) {
    return StringUtils.remove(functionName, '_').toLowerCase();
  }

  private static boolean isSameFunction(String function1, String function2) {
    return canonicalize(function1).equals(canonicalize(function2));
  }

  /**
   * Rewrite non-aggregate group by query to distinct query.
   * E.g.
   * ```
   *   SELECT col1+col2*5 FROM foo GROUP BY col1, col2 => SELECT distinct col1+col2*5 FROM foo
   *   SELECT col1, col2 FROM foo GROUP BY col1, col2 => SELECT distinct col1, col2 FROM foo
   * ```
   * @param pinotQuery
   */
  private static void rewriteNonAggregationGroupByToDistinct(PinotQuery pinotQuery) {
    boolean hasAggregation = false;
    for (Expression select : pinotQuery.getSelectList()) {
      if (isAggregateExpression(select)) {
        hasAggregation = true;
      }
    }
    if (pinotQuery.getOrderByList() != null) {
      for (Expression orderBy : pinotQuery.getOrderByList()) {
        if (isAggregateExpression(orderBy)) {
          hasAggregation = true;
        }
      }
    }
    if (!hasAggregation && pinotQuery.getGroupByListSize() > 0) {
      Set<String> selectIdentifiers = extractIdentifiers(pinotQuery.getSelectList(), true);
      Set<String> groupByIdentifiers = extractIdentifiers(pinotQuery.getGroupByList(), true);
      if (groupByIdentifiers.containsAll(selectIdentifiers)) {
        Expression distinctExpression = RequestUtils.getFunctionExpression("DISTINCT");
        for (Expression select : pinotQuery.getSelectList()) {
          if (isAsFunction(select)) {
            Function asFunc = select.getFunctionCall();
            distinctExpression.getFunctionCall().addToOperands(asFunc.getOperands().get(0));
          } else {
            distinctExpression.getFunctionCall().addToOperands(select);
          }
        }
        pinotQuery.setSelectList(Arrays.asList(distinctExpression));
        pinotQuery.setGroupByList(Collections.emptyList());
      } else {
        selectIdentifiers.removeAll(groupByIdentifiers);
        throw new SqlCompilationException(String.format(
            "For non-aggregation group by query, all the identifiers in select clause should be in groupBys. Found identifier: %s",
            Arrays.toString(selectIdentifiers.toArray(new String[0]))));
      }
    }
  }

  private static boolean isAsFunction(Expression expression) {
    return expression.getFunctionCall() != null && expression.getFunctionCall().getOperator().equalsIgnoreCase("AS");
  }

  private static void invokeCompileTimeFunctions(PinotQuery pinotQuery) {
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
  }

  // This method converts a predicate expression to the what Pinot could evaluate.
  // For comparison expression, left operand could be any expression, but right operand only
  // supports literal.
  // E.g. 'WHERE a > b' will be updated to 'WHERE a - b > 0'
  private static Expression updateComparisonPredicate(Expression expression) {
    Function function = expression.getFunctionCall();
    if (function != null) {
      String operator = function.getOperator().toUpperCase();
      FilterKind filterKind;
      try {
        filterKind = FilterKind.valueOf(operator);
      } catch (Exception e) {
        throw new SqlCompilationException("Unsupported filter kind: " + operator);
      }
      List<Expression> operands = function.getOperands();
      switch (filterKind) {
        case AND:
        case OR:
          operands.replaceAll(CalciteSqlParser::updateComparisonPredicate);
          break;
        case EQUALS:
        case NOT_EQUALS:
        case GREATER_THAN:
        case GREATER_THAN_OR_EQUAL:
        case LESS_THAN:
        case LESS_THAN_OR_EQUAL:
          Expression firstOperand = operands.get(0);
          Expression secondOperand = operands.get(1);

          // Handle predicate like '10 = a' -> 'a = 10'
          if (firstOperand.isSetLiteral()) {
            if (!secondOperand.isSetLiteral()) {
              function.setOperator(getOppositeOperator(filterKind).name());
              operands.set(0, secondOperand);
              operands.set(1, firstOperand);
            }
            break;
          }

          // Handle predicate like 'a > b' -> 'a - b > 0'
          if (!secondOperand.isSetLiteral()) {
            Expression minusExpression = RequestUtils.getFunctionExpression(SqlKind.MINUS.name());
            minusExpression.getFunctionCall().setOperands(Arrays.asList(firstOperand, secondOperand));
            operands.set(0, minusExpression);
            operands.set(1, RequestUtils.getLiteralExpression(0));
            break;
          }

          break;
        default:
          int numOperands = operands.size();
          for (int i = 1; i < numOperands; i++) {
            if (!operands.get(i).isSetLiteral()) {
              throw new SqlCompilationException(String
                  .format("For %s predicate, the operands except for the first one must be literal, got: %s",
                      filterKind, expression));
            }
          }
      }
    }
    return expression;
  }

  /**
   * The purpose of this method is to convert expression "0 < columnA" to "columnA > 0".
   * The conversion would be:
   *  from ">" to "<",
   *  from "<" to ">",
   *  from ">=" to "<=",
   *  from "<=" to ">=".
   */
  private static FilterKind getOppositeOperator(FilterKind filterKind) {
    switch (filterKind) {
      case GREATER_THAN:
        return FilterKind.LESS_THAN;
      case GREATER_THAN_OR_EQUAL:
        return FilterKind.LESS_THAN_OR_EQUAL;
      case LESS_THAN:
        return FilterKind.GREATER_THAN;
      case LESS_THAN_OR_EQUAL:
        return FilterKind.GREATER_THAN_OR_EQUAL;
      default:
        // Do nothing
        return filterKind;
    }
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

  private static List<String> extractOptionsFromSql(String sql) {
    List<String> results = new ArrayList<>();
    Matcher matcher = OPTIONS_REGEX_PATTEN.matcher(sql);
    while (matcher.find()) {
      results.add(matcher.group(1));
    }
    return results;
  }

  private static String removeOptionsFromSql(String sql) {
    Matcher matcher = OPTIONS_REGEX_PATTEN.matcher(sql);
    return matcher.replaceAll("");
  }

  private static List<Expression> convertDistinctSelectList(SqlNodeList selectList) {
    List<Expression> selectExpr = new ArrayList<>();
    selectExpr.add(convertDistinctAndSelectListToFunctionExpression(selectList));
    return selectExpr;
  }

  private static List<Expression> convertSelectList(SqlNodeList selectList) {
    List<Expression> selectExpr = new ArrayList<>();

    final Iterator<SqlNode> iterator = selectList.iterator();
    while (iterator.hasNext()) {
      final SqlNode next = iterator.next();
      selectExpr.add(toExpression(next));
    }

    return selectExpr;
  }

  private static List<Expression> convertOrderByList(SqlNodeList orderList) {
    List<Expression> orderByExpr = new ArrayList<>();
    final Iterator<SqlNode> iterator = orderList.iterator();
    while (iterator.hasNext()) {
      final SqlNode next = iterator.next();
      orderByExpr.add(convertOrderBy(next));
    }
    return orderByExpr;
  }

  private static Expression convertOrderBy(SqlNode node) {
    final SqlKind kind = node.getKind();
    Expression expression;
    switch (kind) {
      case DESCENDING:
        SqlBasicCall basicCall = (SqlBasicCall) node;
        expression = RequestUtils.getFunctionExpression("DESC");
        expression.getFunctionCall().addToOperands(toExpression(basicCall.getOperands()[0]));
        break;
      case IDENTIFIER:
      default:
        expression = RequestUtils.getFunctionExpression("ASC");
        expression.getFunctionCall().addToOperands(toExpression(node));
        break;
    }
    return expression;
  }

  /**
   * DISTINCT is implemented as an aggregation function so need to take the select list items
   * and convert them into a single function expression for handing over to execution engine
   * either as a PinotQuery or BrokerRequest via conversion
   * @param selectList select list items
   * @return DISTINCT function expression
   */
  private static Expression convertDistinctAndSelectListToFunctionExpression(SqlNodeList selectList) {
    String functionName = AggregationFunctionType.DISTINCT.getName();
    Expression functionExpression = RequestUtils.getFunctionExpression(functionName);
    for (SqlNode node : selectList) {
      Expression columnExpression = toExpression(node);
      if (columnExpression.getType() == ExpressionType.IDENTIFIER && columnExpression.getIdentifier().getName()
          .equals("*")) {
        throw new SqlCompilationException(
            "Syntax error: Pinot currently does not support DISTINCT with *. Please specify each column name after DISTINCT keyword");
      } else if (columnExpression.getType() == ExpressionType.FUNCTION) {
        Function functionCall = columnExpression.getFunctionCall();
        String function = functionCall.getOperator();
        if (FunctionDefinitionRegistry.isAggFunc(function)) {
          throw new SqlCompilationException(
              "Syntax error: Use of DISTINCT with aggregation functions is not supported");
        }
      }
      functionExpression.getFunctionCall().addToOperands(columnExpression);
    }
    return functionExpression;
  }

  private static Expression toExpression(SqlNode node) {
    LOGGER.debug("Current processing SqlNode: {}, node.getKind(): {}", node, node.getKind());
    switch (node.getKind()) {
      case IDENTIFIER:
        if (((SqlIdentifier) node).isStar()) {
          return RequestUtils.getIdentifierExpression("*");
        }
        if (((SqlIdentifier) node).isSimple()) {
          return RequestUtils.getIdentifierExpression(((SqlIdentifier) node).getSimple());
        }
        return RequestUtils.getIdentifierExpression(node.toString());
      case LITERAL:
        return RequestUtils.getLiteralExpression((SqlLiteral) node);
      case AS:
        SqlBasicCall asFuncSqlNode = (SqlBasicCall) node;
        Expression leftExpr = toExpression(asFuncSqlNode.getOperands()[0]);
        SqlNode aliasSqlNode = asFuncSqlNode.getOperands()[1];
        String aliasName;
        switch (aliasSqlNode.getKind()) {
          case IDENTIFIER:
            aliasName = ((SqlIdentifier) aliasSqlNode).getSimple();
            break;
          case LITERAL:
            aliasName = ((SqlLiteral) aliasSqlNode).toValue();
            break;
          default:
            throw new SqlCompilationException("Unsupported Alias sql node - " + aliasSqlNode);
        }
        Expression rightExpr = RequestUtils.getIdentifierExpression(aliasName);
        // Just return left identifier if both sides are the same identifier.
        if (leftExpr.isSetIdentifier() && rightExpr.isSetIdentifier()) {
          if (leftExpr.getIdentifier().getName().equals(rightExpr.getIdentifier().getName())) {
            return leftExpr;
          }
        }
        final Expression asFuncExpr = RequestUtils.getFunctionExpression(SqlKind.AS.toString());
        asFuncExpr.getFunctionCall().addToOperands(leftExpr);
        asFuncExpr.getFunctionCall().addToOperands(rightExpr);
        return asFuncExpr;
      case CASE:
        // CASE WHEN Statement is model as a function with variable length parameters.
        // Assume N is number of WHEN Statements, total number of parameters is (2 * N + 1).
        // - N: Convert each WHEN Statement into a function Expression;
        // - N: Convert each THEN Statement into an Expression;
        // - 1: Convert ELSE Statement into an Expression.
        SqlCase caseSqlNode = (SqlCase) node;
        SqlNodeList whenOperands = caseSqlNode.getWhenOperands();
        SqlNodeList thenOperands = caseSqlNode.getThenOperands();
        SqlNode elseOperand = caseSqlNode.getElseOperand();
        Expression caseFuncExpr = RequestUtils.getFunctionExpression(SqlKind.CASE.name());
        for (SqlNode whenSqlNode : whenOperands.getList()) {
          Expression whenExpression = toExpression(whenSqlNode);
          if (isAggregateExpression(whenExpression)) {
            throw new SqlCompilationException(
                "Aggregation functions inside WHEN Clause is not supported - " + whenSqlNode);
          }
          caseFuncExpr.getFunctionCall().addToOperands(whenExpression);
        }
        for (SqlNode thenSqlNode : thenOperands.getList()) {
          Expression thenExpression = toExpression(thenSqlNode);
          if (isAggregateExpression(thenExpression)) {
            throw new SqlCompilationException(
                "Aggregation functions inside THEN Clause is not supported - " + thenSqlNode);
          }
          caseFuncExpr.getFunctionCall().addToOperands(thenExpression);
        }
        Expression elseExpression = toExpression(elseOperand);
        if (isAggregateExpression(elseExpression)) {
          throw new SqlCompilationException(
              "Aggregation functions inside ELSE Clause is not supported - " + elseExpression);
        }
        caseFuncExpr.getFunctionCall().addToOperands(elseExpression);
        return caseFuncExpr;
      default:
        if (node instanceof SqlDataTypeSpec) {
          // This is to handle expression like: CAST(col AS INT)
          return RequestUtils.getLiteralExpression(((SqlDataTypeSpec) node).getTypeName().getSimple());
        } else {
          return compileFunctionExpression((SqlBasicCall) node);
        }
    }
  }

  private static Expression compileFunctionExpression(SqlBasicCall functionNode) {
    SqlKind functionKind = functionNode.getKind();
    String functionName;
    switch (functionKind) {
      case AND:
        return compileAndExpression(functionNode);
      case OR:
        return compileOrExpression(functionNode);
      case COUNT:
        SqlLiteral functionQuantifier = functionNode.getFunctionQuantifier();
        if (functionQuantifier != null && functionQuantifier.toValue().equalsIgnoreCase("DISTINCT")) {
          functionName = AggregationFunctionType.DISTINCTCOUNT.name();
        } else {
          functionName = AggregationFunctionType.COUNT.name();
        }
        break;
      case OTHER:
      case OTHER_FUNCTION:
      case DOT:
        functionName = functionNode.getOperator().getName().toUpperCase();
        if (functionName.equals("ITEM") || functionName.equals("DOT")) {
          // Calcite parses path expression such as "data[0][1].a.b[0]" into a chain of ITEM and/or DOT
          // functions. Collapse this chain into an identifier.
          StringBuffer path = new StringBuffer();
          compilePathExpression(functionName, functionNode, path);
          return RequestUtils.getIdentifierExpression(path.toString());
        }
        break;
      default:
        functionName = functionKind.name();
    }
    // When there is no argument, set an empty list as the operands
    SqlNode[] childNodes = functionNode.getOperands();
    List<Expression> operands = new ArrayList<>(childNodes.length);
    for (SqlNode childNode : childNodes) {
      if (childNode instanceof SqlNodeList) {
        for (SqlNode node : (SqlNodeList) childNode) {
          operands.add(toExpression(node));
        }
      } else {
        operands.add(toExpression(childNode));
      }
    }
    validateFunction(functionName, operands);
    Expression functionExpression = RequestUtils.getFunctionExpression(functionName);
    functionExpression.getFunctionCall().setOperands(operands);
    return functionExpression;
  }

  /**
   * Convert Calcite operator tree made up of ITEM and DOT functions to an identifier. For example, the operator tree
   * shown below will be converted to IDENTIFIER "jsoncolumn.data[0][1].a.b[0]".
   *
   * ├── ITEM(jsoncolumn.data[0][1].a.b[0])
   *      ├── LITERAL (0)
   *      └── DOT (jsoncolumn.daa[0][1].a.b)
   *            ├── IDENTIFIER (b)
   *            └── DOT (jsoncolumn.data[0][1].a)
   *                  ├── IDENTIFIER (a)
   *                  └── ITEM (jsoncolumn.data[0][1])
   *                        ├── LITERAL (1)
   *                        └── ITEM (jsoncolumn.data[0])
   *                              ├── LITERAL (1)
   *                              └── IDENTIFIER (jsoncolumn.data)
   *
   * @param functionName Name of the function ("DOT" or "ITEM")
   * @param functionNode Root node of the DOT and/or ITEM operator function chain.
   * @param path String representation of path represented by DOT and/or ITEM function chain.
   */
  private static void compilePathExpression(String functionName, SqlBasicCall functionNode, StringBuffer path) {
    SqlNode[] operands = functionNode.getOperands();

    // Compile first operand of the function (either an identifier or another DOT and/or ITEM function).
    SqlKind kind0 = operands[0].getKind();
    if (kind0 == SqlKind.IDENTIFIER) {
      path.append(((SqlIdentifier) operands[0]).toString());
    } else if (kind0 == SqlKind.DOT || kind0 == SqlKind.OTHER_FUNCTION) {
      SqlBasicCall function0 = (SqlBasicCall) operands[0];
      String name0 = function0.getOperator().getName();
      if (name0.equals("ITEM") || name0.equals("DOT")) {
        compilePathExpression(name0, function0, path);
      } else {
        throw new SqlCompilationException("SELECT list item has bad path expression.");
      }
    } else {
      throw new SqlCompilationException("SELECT list item has bad path expression.");
    }

    // Compile second operand of the function (either an identifier or literal).
    SqlKind kind1 = operands[1].getKind();
    if (kind1 == SqlKind.IDENTIFIER) {
      path.append(".").append(((SqlIdentifier) operands[1]).getSimple());
    } else if (kind1 == SqlKind.LITERAL) {
      path.append("[").append(((SqlLiteral) operands[1]).toValue()).append("]");
    } else {
      throw new SqlCompilationException("SELECT list item has bad path expression.");
    }
  }

  private static void validateFunction(String functionName, List<Expression> operands) {
    switch (canonicalize(functionName)) {
      case "jsonextractscalar":
        validateJsonExtractScalarFunction(operands);
        break;
      case "jsonextractkey":
        validateJsonExtractKeyFunction(operands);
        break;
    }
  }

  private static void validateJsonExtractScalarFunction(List<Expression> operands) {
    int numOperands = operands.size();

    // Check that there are exactly 3 or 4 arguments
    if (numOperands != 3 && numOperands != 4) {
      throw new SqlCompilationException(
          "Expect 3 or 4 arguments for transform function: jsonExtractScalar(jsonFieldName, 'jsonPath', 'resultsType', ['defaultValue'])");
    }
    if (!operands.get(1).isSetLiteral() || !operands.get(2).isSetLiteral() || (numOperands == 4 && !operands.get(3)
        .isSetLiteral())) {
      throw new SqlCompilationException(
          "Expect the 2nd/3rd/4th argument of transform function: jsonExtractScalar(jsonFieldName, 'jsonPath', 'resultsType', ['defaultValue']) to be a single-quoted literal value.");
    }
  }

  private static void validateJsonExtractKeyFunction(List<Expression> operands) {
    // Check that there are exactly 2 arguments
    if (operands.size() != 2) {
      throw new SqlCompilationException(
          "Expect 2 arguments are required for transform function: jsonExtractKey(jsonFieldName, 'jsonPath')");
    }
    if (!operands.get(1).isSetLiteral()) {
      throw new SqlCompilationException(
          "Expect the 2nd argument for transform function: jsonExtractKey(jsonFieldName, 'jsonPath') to be a single-quoted literal value.");
    }
  }

  /**
   * Helper method to flatten the operands for the AND expression.
   */
  private static Expression compileAndExpression(SqlBasicCall andNode) {
    List<Expression> operands = new ArrayList<>();
    for (SqlNode childNode : andNode.getOperands()) {
      if (childNode.getKind() == SqlKind.AND) {
        Expression childAndExpression = compileAndExpression((SqlBasicCall) childNode);
        operands.addAll(childAndExpression.getFunctionCall().getOperands());
      } else {
        operands.add(compileFunctionExpression((SqlBasicCall) childNode));
      }
    }
    Expression andExpression = RequestUtils.getFunctionExpression(SqlKind.AND.name());
    andExpression.getFunctionCall().setOperands(operands);
    return andExpression;
  }

  /**
   * Helper method to flatten the operands for the OR expression.
   */
  private static Expression compileOrExpression(SqlBasicCall orNode) {
    List<Expression> operands = new ArrayList<>();
    for (SqlNode childNode : orNode.getOperands()) {
      if (childNode.getKind() == SqlKind.OR) {
        Expression childAndExpression = compileOrExpression((SqlBasicCall) childNode);
        operands.addAll(childAndExpression.getFunctionCall().getOperands());
      } else {
        operands.add(compileFunctionExpression((SqlBasicCall) childNode));
      }
    }
    Expression andExpression = RequestUtils.getFunctionExpression(SqlKind.OR.name());
    andExpression.getFunctionCall().setOperands(operands);
    return andExpression;
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
          arguments[i] = function.getOperands().get(i).getLiteral().getFieldValue();
        }
        try {
          FunctionInvoker invoker = new FunctionInvoker(functionInfo);
          invoker.convertTypes(arguments);
          Object result = invoker.invoke(arguments);
          return RequestUtils.getLiteralExpression(result);
        } catch (Exception e) {
          throw new SqlCompilationException(
              "Caught exception while invoking method: " + functionInfo.getMethod() + " with arguments: " + Arrays
                  .toString(arguments), e);
        }
      }
    }
    return expression;
  }

  public static boolean isLiteralOnlyExpression(Expression e) {
    if (e.getType() == ExpressionType.LITERAL) {
      return true;
    }
    if (e.getType() == ExpressionType.FUNCTION) {
      Function functionCall = e.getFunctionCall();
      if (functionCall.getOperator().equalsIgnoreCase(SqlKind.AS.toString())) {
        return isLiteralOnlyExpression(functionCall.getOperands().get(0));
      }
      return false;
    }
    return false;
  }
}
