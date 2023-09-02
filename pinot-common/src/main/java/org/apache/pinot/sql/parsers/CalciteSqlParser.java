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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import java.io.StringReader;
import java.util.ArrayList;
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
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlExplain;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSelectKeyword;
import org.apache.calcite.sql.SqlSetOption;
import org.apache.calcite.sql.fun.SqlBetweenOperator;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.fun.SqlLikeOperator;
import org.apache.calcite.sql.parser.SqlAbstractParserImpl;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.commons.collections.CollectionUtils;
import org.apache.pinot.common.request.DataSource;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.Identifier;
import org.apache.pinot.common.request.Join;
import org.apache.pinot.common.request.JoinType;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.sql.FilterKind;
import org.apache.pinot.sql.parsers.parser.SqlInsertFromFile;
import org.apache.pinot.sql.parsers.parser.SqlParserImpl;
import org.apache.pinot.sql.parsers.rewriter.QueryRewriter;
import org.apache.pinot.sql.parsers.rewriter.QueryRewriterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CalciteSqlParser {
  private CalciteSqlParser() {
  }

  public static final String ASC = "asc";
  public static final String DESC = "desc";
  public static final String NULLS_LAST = "nullslast";
  public static final String NULLS_FIRST = "nullsfirst";
  public static final ImmutableSet<String> ORDER_BY_FUNCTIONS = ImmutableSet.of(ASC, DESC, NULLS_LAST, NULLS_FIRST);
  public static final List<QueryRewriter> QUERY_REWRITERS = new ArrayList<>(QueryRewriterFactory.getQueryRewriters());
  private static final Logger LOGGER = LoggerFactory.getLogger(CalciteSqlParser.class);

  // To Keep the backward compatibility with 'OPTION' Functionality in PQL, which is used to
  // provide more hints for query processing.
  //
  // PQL syntax is: `OPTION (<key> = <value>)`
  //
  // Multiple OPTIONs is also supported by:
  // `OPTION (<k1> = <v1>, <k2> = <v2>, <k3> = <v3>)`
  private static final Pattern OPTIONS_REGEX_PATTEN =
      Pattern.compile("\\s*option\\s*\\(([^\\)]+)\\)\\s*\\Z", Pattern.CASE_INSENSITIVE);

  /**
   * Checks for the presence of semicolon in the sql query and modifies the query accordingly
   *
   * @param sql sql query
   * @return sql query without semicolons
   *
   */
  private static String removeTerminatingSemicolon(String sql) {
    // trim all the leading and trailing whitespaces
    sql = sql.trim();
    int sqlLength = sql.length();

    // Terminate the semicolon only if the last character of the query is semicolon
    if (sql.charAt(sqlLength - 1) == ';') {
      return sql.substring(0, sqlLength - 1);
    }
    return sql;
  }

  public static SqlNodeAndOptions compileToSqlNodeAndOptions(String sql)
      throws SqlCompilationException {
    long parseStartTimeNs = System.nanoTime();

    // Remove the terminating semicolon from the query
    sql = removeTerminatingSemicolon(sql);

    // extract and remove OPTIONS string
    List<String> options = extractOptionsFromSql(sql);
    if (!options.isEmpty()) {
      sql = removeOptionsFromSql(sql);
    }

    try (StringReader inStream = new StringReader(sql)) {
      SqlParserImpl sqlParser = newSqlParser(inStream);
      SqlNodeList sqlNodeList = sqlParser.SqlStmtsEof();
      // Extract OPTION statements from sql.
      SqlNodeAndOptions sqlNodeAndOptions = extractSqlNodeAndOptions(sql, sqlNodeList);
      // add legacy OPTIONS keyword-based options
      if (options.size() > 0) {
        sqlNodeAndOptions.setExtraOptions(extractOptionsMap(options));
      }
      sqlNodeAndOptions.setParseTimeNs(System.nanoTime() - parseStartTimeNs);
      return sqlNodeAndOptions;
    } catch (Throwable e) {
      throw new SqlCompilationException("Caught exception while parsing query: " + sql, e);
    }
  }

  @VisibleForTesting
  static SqlNodeAndOptions extractSqlNodeAndOptions(String sql, SqlNodeList sqlNodeList) {
    PinotSqlType sqlType = null;
    SqlNode statementNode = null;
    Map<String, String> options = new HashMap<>();
    for (SqlNode sqlNode : sqlNodeList) {
      if (sqlNode instanceof SqlInsertFromFile) {
        // extract insert statement (execution statement)
        if (sqlType == null) {
          sqlType = PinotSqlType.DML;
          statementNode = sqlNode;
        } else {
          throw new SqlCompilationException("SqlNode with executable statement already exist with type: " + sqlType);
        }
      } else if (sqlNode instanceof SqlSetOption) {
        // extract options, these are non-execution statements
        List<SqlNode> operandList = ((SqlSetOption) sqlNode).getOperandList();
        SqlIdentifier key = (SqlIdentifier) operandList.get(1);
        SqlLiteral value = (SqlLiteral) operandList.get(2);
        options.put(key.getSimple(), value.toValue());
      } else {
        // default extract query statement (execution statement)
        if (sqlType == null) {
          sqlType = PinotSqlType.DQL;
          statementNode = sqlNode;
        } else {
          throw new SqlCompilationException("SqlNode with executable statement already exist with type: " + sqlType);
        }
      }
    }
    if (sqlType == null) {
      throw new SqlCompilationException("SqlNode with executable statement not found!");
    }
    return new SqlNodeAndOptions(statementNode, sqlType, QueryOptionsUtils.resolveCaseInsensitiveOptions(options));
  }

  public static PinotQuery compileToPinotQuery(String sql)
      throws SqlCompilationException {
    return compileToPinotQuery(compileToSqlNodeAndOptions(sql));
  }

  public static PinotQuery compileToPinotQuery(SqlNodeAndOptions sqlNodeAndOptions) {
    // Compile Sql without OPTION statements.
    PinotQuery pinotQuery = compileSqlNodeToPinotQuery(sqlNodeAndOptions.getSqlNode());

    // Set Option statements to PinotQuery.
    Map<String, String> options = sqlNodeAndOptions.getOptions();
    if (!options.isEmpty()) {
      pinotQuery.setQueryOptions(options);
    }
    return pinotQuery;
  }

  static void validate(PinotQuery pinotQuery)
      throws SqlCompilationException {
    validateGroupByClause(pinotQuery);
    validateDistinctQuery(pinotQuery);
    if (pinotQuery.isSetFilterExpression()) {
      validateFilter(pinotQuery.getFilterExpression());
    }
  }

  private static void validateGroupByClause(PinotQuery pinotQuery)
      throws SqlCompilationException {
    boolean hasGroupByClause = pinotQuery.getGroupByList() != null;
    Set<Expression> groupByExprs = hasGroupByClause ? new HashSet<>(pinotQuery.getGroupByList()) : null;
    int aggregateExprCount = 0;
    for (Expression selectExpression : pinotQuery.getSelectList()) {
      if (isAggregateExpression(selectExpression)) {
        aggregateExprCount++;
      } else if (hasGroupByClause && expressionOutsideGroupByList(selectExpression, groupByExprs)) {
        throw new SqlCompilationException(
            "'" + RequestUtils.prettyPrint(selectExpression) + "' should appear in GROUP BY clause.");
      }
    }

    // block mixture of aggregate and non-aggregate expression when group by is absent
    int nonAggregateExprCount = pinotQuery.getSelectListSize() - aggregateExprCount;
    if (!hasGroupByClause && aggregateExprCount > 0 && nonAggregateExprCount > 0) {
      throw new SqlCompilationException("Columns and Aggregate functions can't co-exist without GROUP BY clause");
    }
    // Sanity check on group by clause shouldn't contain aggregate expression.
    if (hasGroupByClause) {
      for (Expression groupByExpression : pinotQuery.getGroupByList()) {
        if (isAggregateExpression(groupByExpression)) {
          throw new SqlCompilationException("Aggregate expression '" + RequestUtils.prettyPrint(groupByExpression)
              + "' is not allowed in GROUP BY clause.");
        }
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
      if (function != null && function.getOperator().equals("distinct")) {
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
          List<Expression> distinctExpressions = getAliasLeftExpressionsFromDistinctExpression(function);
          for (Expression orderByExpression : orderByList) {
            // NOTE: Order-by is always a Function with the ordering of the Expression
            if (!distinctExpressions.contains(removeOrderByFunctions(orderByExpression))) {
              throw new IllegalStateException("ORDER-BY columns should be included in the DISTINCT columns");
            }
          }
        }
      }
    }
  }

  /*
   * Throws an exception if the filter's rhs has NULL because:
   * - Predicate evaluator and pruning do not have NULL support.
   * - It is not useful to have NULL in the filter's rhs.
   *   - For most of the filters (e.g. EQUALS, GREATER_THAN, LIKE), the rhs being NULL leads to no record matched.
   *   - For IN, adding NULL to the rhs list does not change the matched records.
   *   - For NOT IN, adding NULL to the rhs list leads to no record matched.
   */
  private static void validateFilter(Expression filterExpression) {
    if (!filterExpression.isSetFunctionCall()) {
      return;
    }
    String operator = filterExpression.getFunctionCall().getOperator();
    if (operator.equals(FilterKind.AND.name()) || operator.equals(FilterKind.OR.name()) || operator.equals(
        FilterKind.NOT.name())) {
      for (Expression filter : filterExpression.getFunctionCall().getOperands()) {
        validateFilter(filter);
      }
    } else {
      List<Expression> operands = filterExpression.getFunctionCall().getOperands();
      for (int i = 1; i < operands.size(); i++) {
        if (operands.get(i).getLiteral().isSetNullValue()) {
          throw new IllegalStateException(String.format("Using NULL in %s filter is not supported", operator));
        }
      }
    }
  }

  private static List<Expression> getAliasLeftExpressionsFromDistinctExpression(Function function) {
    List<Expression> operands = function.getOperands();
    List<Expression> expressions = new ArrayList<>(operands.size());
    for (Expression operand : operands) {
      if (isAsFunction(operand)) {
        expressions.add(operand.getFunctionCall().getOperands().get(0));
      } else {
        expressions.add(operand);
      }
    }
    return expressions;
  }

  /**
   * Check recursively if an expression contains any reference not appearing in the GROUP BY clause.
   */
  private static boolean expressionOutsideGroupByList(Expression expr, Set<Expression> groupByExprs) {
    // return early for Literal, Aggregate and if we have an exact match
    if (expr.getType() == ExpressionType.LITERAL || isAggregateExpression(expr) || groupByExprs.contains(expr)) {
      return false;
    }
    Function function = expr.getFunctionCall();
    // function expression
    if (function != null) {
      // for Alias function, check the actual value
      if (function.getOperator().equals("as")) {
        return expressionOutsideGroupByList(function.getOperands().get(0), groupByExprs);
      }
      // Expression is invalid if any of its children is invalid
      return function.getOperands().stream().anyMatch(e -> expressionOutsideGroupByList(e, groupByExprs));
    }
    return true;
  }

  public static boolean isAggregateExpression(Expression expression) {
    Function functionCall = expression.getFunctionCall();
    if (functionCall != null) {
      String operator = functionCall.getOperator();
      if (AggregationFunctionType.isAggregationFunction(operator)) {
        return true;
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

  public static boolean isAsFunction(Expression expression) {
    Function function = expression.getFunctionCall();
    return function != null && function.getOperator().equals("as");
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
      Identifier identifier = expression.getIdentifier();
      if (identifier != null) {
        identifiers.add(identifier.getName());
        continue;
      }
      Function function = expression.getFunctionCall();
      if (function != null) {
        if (excludeAs && function.getOperator().equals("as")) {
          identifiers.addAll(
              extractIdentifiers(new ArrayList<>(Collections.singletonList(function.getOperands().get(0))), true));
        } else {
          identifiers.addAll(extractIdentifiers(function.getOperands(), excludeAs));
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
    SqlNode sqlNode;
    try (StringReader inStream = new StringReader(expression)) {
      SqlParserImpl sqlParser = newSqlParser(inStream);
      sqlNode = sqlParser.parseSqlExpressionEof();
    } catch (Throwable e) {
      throw new SqlCompilationException("Caught exception while parsing expression: " + expression, e);
    }
    return toExpression(sqlNode);
  }

  @VisibleForTesting
  static SqlParserImpl newSqlParser(StringReader inStream) {
    SqlParserImpl sqlParser = new SqlParserImpl(inStream);
    sqlParser.switchTo(SqlAbstractParserImpl.LexicalState.DQID);
    // TODO: convert to MySQL conformance once we retired most of the un-tested BABEL tokens
    sqlParser.setConformance(SqlConformanceEnum.BABEL);
    sqlParser.setTabSize(1);
    sqlParser.setQuotedCasing(Casing.UNCHANGED);
    sqlParser.setUnquotedCasing(Casing.UNCHANGED);
    sqlParser.setIdentifierMaxLength(SqlParser.DEFAULT_IDENTIFIER_MAX_LENGTH);
    return sqlParser;
  }

  public static PinotQuery compileSqlNodeToPinotQuery(SqlNode sqlNode) {
    PinotQuery pinotQuery = new PinotQuery();
    if (sqlNode instanceof SqlExplain) {
      // Extract sql node for the query
      sqlNode = ((SqlExplain) sqlNode).getExplicandum();
      pinotQuery.setExplain(true);
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
      pinotQuery.setDataSource(compileToDataSource(fromNode));
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

  private static DataSource compileToDataSource(SqlNode sqlNode) {
    DataSource dataSource = new DataSource();
    switch (sqlNode.getKind()) {
      case IDENTIFIER:
        dataSource.setTableName(sqlNode.toString());
        break;
      case AS:
        List<SqlNode> operandList = ((SqlBasicCall) sqlNode).getOperandList();
        dataSource.setSubquery(compileSqlNodeToPinotQuery(operandList.get(0)));
        dataSource.setTableName(operandList.get(1).toString());
        break;
      case SELECT:
      case ORDER_BY:
        dataSource.setSubquery(compileSqlNodeToPinotQuery(sqlNode));
        break;
      case JOIN:
        dataSource.setJoin(compileToJoin((SqlJoin) sqlNode));
        break;
      default:
        throw new IllegalStateException("Unsupported SQL node kind as DataSource: " + sqlNode.getKind());
    }
    return dataSource;
  }

  private static Join compileToJoin(SqlJoin sqlJoin) {
    Join join = new Join();
    switch (sqlJoin.getJoinType()) {
      case COMMA:
      case INNER:
        join.setType(JoinType.INNER);
        break;
      case LEFT:
        join.setType(JoinType.LEFT);
        break;
      case RIGHT:
        join.setType(JoinType.RIGHT);
        break;
      case FULL:
        join.setType(JoinType.FULL);
        break;
      default:
        throw new IllegalStateException("Unsupported join type: " + sqlJoin.getJoinType());
    }
    join.setLeft(compileToDataSource(sqlJoin.getLeft()));
    join.setRight(compileToDataSource(sqlJoin.getRight()));
    switch (sqlJoin.getConditionType()) {
      case ON:
        join.setCondition(toExpression(sqlJoin.getCondition()));
        break;
      case NONE:
        break;
      default:
        throw new IllegalStateException("Unsupported join condition type: " + sqlJoin.getConditionType());
    }
    return join;
  }

  private static void queryRewrite(PinotQuery pinotQuery) {
    for (QueryRewriter queryRewriter : QUERY_REWRITERS) {
      pinotQuery = queryRewriter.rewrite(pinotQuery);
    }
    // Validate
    validate(pinotQuery);
  }

  @Deprecated
  private static List<String> extractOptionsFromSql(String sql) {
    List<String> results = new ArrayList<>();
    Matcher matcher = OPTIONS_REGEX_PATTEN.matcher(sql);
    while (matcher.find()) {
      results.add(matcher.group(1));
    }
    return results;
  }

  @Deprecated
  private static String removeOptionsFromSql(String sql) {
    Matcher matcher = OPTIONS_REGEX_PATTEN.matcher(sql);
    return matcher.replaceAll("");
  }

  @Deprecated
  private static Map<String, String> extractOptionsMap(List<String> optionsStatements) {
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
    return options;
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
    for (SqlNode sqlNode : orderList) {
      orderByExpr.add(convertOrderBy(sqlNode, true));
    }
    return orderByExpr;
  }

  private static Expression convertOrderBy(SqlNode node, boolean createAscExpression) {
    // If the order is ASC, the SqlNode will not have an ASC operator. In this case we need to create an ASC function in
    // the expression.
    // The SqlNode puts the NULLS FIRST/LAST operator in an outer level of the DESC operator.
    Expression expression;
    if (node.getKind() == SqlKind.NULLS_LAST) {
      SqlBasicCall basicCall = (SqlBasicCall) node;
      expression = RequestUtils.getFunctionExpression(NULLS_LAST);
      expression.getFunctionCall().addToOperands(convertOrderBy(basicCall.getOperandList().get(0), true));
    } else if (node.getKind() == SqlKind.NULLS_FIRST) {
      SqlBasicCall basicCall = (SqlBasicCall) node;
      expression = RequestUtils.getFunctionExpression(NULLS_FIRST);
      expression.getFunctionCall().addToOperands(convertOrderBy(basicCall.getOperandList().get(0), true));
    } else if (node.getKind() == SqlKind.DESCENDING) {
      SqlBasicCall basicCall = (SqlBasicCall) node;
      expression = RequestUtils.getFunctionExpression(DESC);
      expression.getFunctionCall().addToOperands(convertOrderBy(basicCall.getOperandList().get(0), false));
    } else if (createAscExpression) {
      expression = RequestUtils.getFunctionExpression(ASC);
      expression.getFunctionCall().addToOperands(toExpression(node));
    } else {
      return toExpression(node);
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
    Expression functionExpression = RequestUtils.getFunctionExpression("distinct");
    for (SqlNode node : selectList) {
      Expression columnExpression = toExpression(node);
      if (columnExpression.getType() == ExpressionType.IDENTIFIER && columnExpression.getIdentifier().getName()
          .equals("*")) {
        throw new SqlCompilationException(
            "Syntax error: Pinot currently does not support DISTINCT with *. Please specify each column name after "
                + "DISTINCT keyword");
      } else if (columnExpression.getType() == ExpressionType.FUNCTION) {
        if (AggregationFunctionType.isAggregationFunction(columnExpression.getFunctionCall().getOperator())) {
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
      case INTERVAL_QUALIFIER:
        return RequestUtils.getLiteralExpression(node.toString());
      case LITERAL:
        return RequestUtils.getLiteralExpression((SqlLiteral) node);
      case AS:
        SqlBasicCall asFuncSqlNode = (SqlBasicCall) node;
        List<SqlNode> operands = asFuncSqlNode.getOperandList();
        Expression leftExpr = toExpression(operands.get(0));
        SqlNode aliasSqlNode = operands.get(1);
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
        Expression asFuncExpr = RequestUtils.getFunctionExpression("as");
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
        Expression caseFuncExpr = RequestUtils.getFunctionExpression("case");
        Preconditions.checkState(whenOperands.size() == thenOperands.size());
        for (int i = 0; i < whenOperands.size(); i++) {
          SqlNode whenSqlNode = whenOperands.get(i);
          Expression whenExpression = toExpression(whenSqlNode);
          if (isAggregateExpression(whenExpression)) {
            throw new SqlCompilationException(
                "Aggregation functions inside WHEN Clause is not supported - " + whenSqlNode);
          }
          caseFuncExpr.getFunctionCall().addToOperands(whenExpression);

          SqlNode thenSqlNode = thenOperands.get(i);
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
    boolean negated = false;
    String canonicalName;
    switch (functionKind) {
      case AND:
        return compileAndExpression(functionNode);
      case OR:
        return compileOrExpression(functionNode);
      // BETWEEN and LIKE might be negated (NOT BETWEEN, NOT LIKE)
      case BETWEEN:
        negated = ((SqlBetweenOperator) functionNode.getOperator()).isNegated();
        canonicalName = SqlKind.BETWEEN.name();
        break;
      case LIKE:
        negated = ((SqlLikeOperator) functionNode.getOperator()).isNegated();
        canonicalName = SqlKind.LIKE.name();
        break;
      case OTHER:
      case OTHER_FUNCTION:
      case DOT:
        String functionName = functionNode.getOperator().getName();
        if (functionName.equals("ITEM") || functionName.equals("DOT")) {
          // Calcite parses path expression such as "data[0][1].a.b[0]" into a chain of ITEM and/or DOT
          // functions. Collapse this chain into an identifier.
          StringBuilder pathBuilder = new StringBuilder();
          compilePathExpression(functionNode, pathBuilder);
          return RequestUtils.getIdentifierExpression(pathBuilder.toString());
        }
        canonicalName = RequestUtils.canonicalizeFunctionNamePreservingSpecialKey(functionName);
        if ((functionNode.getFunctionQuantifier() != null) && ("DISTINCT".equals(
            functionNode.getFunctionQuantifier().toString()))) {
          if (canonicalName.equals("count")) {
            canonicalName = "distinctcount";
          } else if (canonicalName.equals("sum")) {
            canonicalName = "distinctsum";
          } else if (canonicalName.equals("avg")) {
            canonicalName = "distinctavg";
          } else if (AggregationFunctionType.isAggregationFunction(canonicalName)) {
            // Aggregation functions other than COUNT, SUM, AVG on DISTINCT are not supported.
            throw new SqlCompilationException("Function '" + functionName + "' on DISTINCT is not supported.");
          }
        }
        break;
      default:
        canonicalName = RequestUtils.canonicalizeFunctionNamePreservingSpecialKey(functionKind.name());
        break;
    }
    // When there is no argument, set an empty list as the operands
    List<SqlNode> childNodes = functionNode.getOperandList();
    List<Expression> operands = new ArrayList<>(childNodes.size());
    for (SqlNode childNode : childNodes) {
      if (childNode instanceof SqlNodeList) {
        for (SqlNode node : (SqlNodeList) childNode) {
          operands.add(toExpression(node));
        }
      } else {
        operands.add(toExpression(childNode));
      }
    }
    validateFunction(canonicalName, operands);
    Expression functionExpression = RequestUtils.getFunctionExpression(canonicalName);
    functionExpression.getFunctionCall().setOperands(operands);
    if (negated) {
      Expression negatedFunctionExpression = RequestUtils.getFunctionExpression(FilterKind.NOT.name());
      // Do not use `Collections.singletonList()` because we might modify the operand later
      List<Expression> negatedFunctionOperands = new ArrayList<>(1);
      negatedFunctionOperands.add(functionExpression);
      negatedFunctionExpression.getFunctionCall().setOperands(negatedFunctionOperands);
      return negatedFunctionExpression;
    } else {
      return functionExpression;
    }
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
   * @param functionNode Root node of the DOT and/or ITEM operator function chain.
   * @param pathBuilder StringBuilder representation of path represented by DOT and/or ITEM function chain.
   */
  private static void compilePathExpression(SqlBasicCall functionNode, StringBuilder pathBuilder) {
    List<SqlNode> operands = functionNode.getOperandList();

    // Compile first operand of the function (either an identifier or another DOT and/or ITEM function).
    SqlNode operand0 = operands.get(0);
    SqlKind kind0 = operand0.getKind();
    if (kind0 == SqlKind.IDENTIFIER) {
      pathBuilder.append(operand0);
    } else if (kind0 == SqlKind.DOT || kind0 == SqlKind.OTHER_FUNCTION) {
      SqlBasicCall function0 = (SqlBasicCall) operand0;
      String name0 = function0.getOperator().getName();
      if (name0.equals("ITEM") || name0.equals("DOT")) {
        compilePathExpression(function0, pathBuilder);
      } else {
        throw new SqlCompilationException("SELECT list item has bad path expression.");
      }
    } else {
      throw new SqlCompilationException("SELECT list item has bad path expression.");
    }

    // Compile second operand of the function (either an identifier or literal).
    SqlNode operand1 = operands.get(1);
    SqlKind kind1 = operand1.getKind();
    if (kind1 == SqlKind.IDENTIFIER) {
      pathBuilder.append('.').append(((SqlIdentifier) operand1).getSimple());
    } else if (kind1 == SqlKind.LITERAL) {
      pathBuilder.append('[').append(((SqlLiteral) operand1).toValue()).append(']');
    } else {
      throw new SqlCompilationException("SELECT list item has bad path expression.");
    }
  }

  private static void validateFunction(String canonicalName, List<Expression> operands) {
    switch (canonicalName) {
      case "jsonextractscalar":
        validateJsonExtractScalarFunction(operands);
        break;
      case "jsonextractkey":
        validateJsonExtractKeyFunction(operands);
        break;
      default:
        break;
    }
  }

  private static void validateJsonExtractScalarFunction(List<Expression> operands) {
    int numOperands = operands.size();

    // Check that there are exactly 3 or 4 arguments
    if (numOperands != 3 && numOperands != 4) {
      throw new SqlCompilationException(
          "Expect 3 or 4 arguments for transform function: jsonExtractScalar(jsonFieldName, 'jsonPath', "
              + "'resultsType', ['defaultValue'])");
    }
    if (!operands.get(1).isSetLiteral() || !operands.get(2).isSetLiteral() || (numOperands == 4 && !operands.get(3)
        .isSetLiteral())) {
      throw new SqlCompilationException(
          "Expect the 2nd/3rd/4th argument of transform function: jsonExtractScalar(jsonFieldName, 'jsonPath',"
              + " 'resultsType', ['defaultValue']) to be a single-quoted literal value.");
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
          "Expect the 2nd argument for transform function: jsonExtractKey(jsonFieldName, 'jsonPath') to be a "
              + "single-quoted literal value.");
    }
  }

  /**
   * Helper method to flatten the operands for the AND expression.
   */
  private static Expression compileAndExpression(SqlBasicCall andNode) {
    List<Expression> operands = new ArrayList<>();
    for (SqlNode childNode : andNode.getOperandList()) {
      if (childNode.getKind() == SqlKind.AND) {
        Expression childAndExpression = compileAndExpression((SqlBasicCall) childNode);
        operands.addAll(childAndExpression.getFunctionCall().getOperands());
      } else {
        operands.add(toExpression(childNode));
      }
    }
    Expression andExpression = RequestUtils.getFunctionExpression(FilterKind.AND.name());
    andExpression.getFunctionCall().setOperands(operands);
    return andExpression;
  }

  /**
   * Helper method to flatten the operands for the OR expression.
   */
  private static Expression compileOrExpression(SqlBasicCall orNode) {
    List<Expression> operands = new ArrayList<>();
    for (SqlNode childNode : orNode.getOperandList()) {
      if (childNode.getKind() == SqlKind.OR) {
        Expression childAndExpression = compileOrExpression((SqlBasicCall) childNode);
        operands.addAll(childAndExpression.getFunctionCall().getOperands());
      } else {
        operands.add(toExpression(childNode));
      }
    }
    Expression andExpression = RequestUtils.getFunctionExpression(FilterKind.OR.name());
    andExpression.getFunctionCall().setOperands(operands);
    return andExpression;
  }

  public static boolean isLiteralOnlyExpression(Expression e) {
    if (e.getType() == ExpressionType.LITERAL) {
      return true;
    }
    if (e.getType() == ExpressionType.FUNCTION) {
      Function function = e.getFunctionCall();
      if (function.getOperator().equals("as")) {
        return isLiteralOnlyExpression(function.getOperands().get(0));
      }
      return false;
    }
    return false;
  }

  public static Expression removeOrderByFunctions(Expression expression) {
    while (expression.isSetFunctionCall() && ORDER_BY_FUNCTIONS.contains(expression.getFunctionCall().getOperator())) {
      expression = expression.getFunctionCall().getOperands().get(0);
    }
    return expression;
  }

  @Nullable
  public static Boolean isNullsLast(Expression expression) {
    String operator = expression.getFunctionCall().getOperator();
    if (operator.equals(CalciteSqlParser.NULLS_LAST)) {
      return true;
    } else if (operator.equals(CalciteSqlParser.NULLS_FIRST)) {
      return false;
    } else {
      return null;
    }
  }

  public static boolean isAsc(Expression expression, Boolean isNullsLast) {
    if (isNullsLast != null) {
      expression = expression.getFunctionCall().getOperands().get(0);
    }
    return expression.getFunctionCall().getOperator().equals(CalciteSqlParser.ASC);
  }
}
