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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
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
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.fun.SqlBetweenOperator;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.fun.SqlLikeOperator;
import org.apache.calcite.sql.parser.SqlAbstractParserImpl;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.pinot.common.function.scalar.arithmetic.NegateScalarFunction;
import org.apache.pinot.common.request.DataSource;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.Identifier;
import org.apache.pinot.common.request.Join;
import org.apache.pinot.common.request.JoinType;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.request.context.GroupingSets;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.sql.FilterKind;
import org.apache.pinot.sql.parsers.parser.SqlInsertFromFile;
import org.apache.pinot.sql.parsers.parser.SqlParserImpl;
import org.apache.pinot.sql.parsers.parser.SqlPinotCreateMaterializedView;
import org.apache.pinot.sql.parsers.parser.SqlPinotCreateTable;
import org.apache.pinot.sql.parsers.parser.SqlPinotDropMaterializedView;
import org.apache.pinot.sql.parsers.parser.SqlPinotDropTable;
import org.apache.pinot.sql.parsers.parser.SqlPinotShowCreateMaterializedView;
import org.apache.pinot.sql.parsers.parser.SqlPinotShowCreateTable;
import org.apache.pinot.sql.parsers.parser.SqlPinotShowMaterializedViews;
import org.apache.pinot.sql.parsers.parser.SqlPinotShowTables;
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
  // TODO: Add the ability to configure the parser's maximum identifier length via configuration if needed in the future
  public static final int CALCITE_SQL_PARSER_IDENTIFIER_MAX_LENGTH = 1024;
  private static final Logger LOGGER = LoggerFactory.getLogger(CalciteSqlParser.class);

  // To Keep the backward compatibility with 'OPTION' Functionality in PQL, which is used to
  // provide more hints for query processing.
  //
  // PQL syntax is: `OPTION (<key> = <value>)`
  //
  // Multiple OPTIONs is also supported by:
  // `OPTION (<k1> = <v1>, <k2> = <v2>, <k3> = <v3>)`
  private static final Pattern OPTIONS_REGEX_PATTEN =
      Pattern.compile("\\s*option\\s*\\(([^)]+)\\)\\s*;?\\s*\\Z", Pattern.CASE_INSENSITIVE);

  public static SqlNodeAndOptions compileToSqlNodeAndOptions(String sql)
      throws SqlCompilationException {
    long parseStartTimeNs = System.nanoTime();

    sql = ParserUtils.sanitizeSql(sql);

    // extract and remove OPTIONS string
    List<String> options = extractOptionsFromSql(sql);
    if (!options.isEmpty()) {
      sql = removeOptionsFromSql(sql);
    }

    try (StringReader inStream = new StringReader(sql)) {
      SqlParserImpl sqlParser = newSqlParser(inStream);
      SqlNodeList sqlNodeList = sqlParser.parseSqlStmtList();
      // Extract OPTION statements from sql.
      SqlNodeAndOptions sqlNodeAndOptions = extractSqlNodeAndOptions(sqlNodeList);
      // add legacy OPTIONS keyword-based options
      if (!options.isEmpty()) {
        sqlNodeAndOptions.setExtraOptions(extractOptionsMap(options));
      }
      sqlNodeAndOptions.setParseTimeNs(System.nanoTime() - parseStartTimeNs);
      return sqlNodeAndOptions;
    } catch (Throwable e) {
      throw new SqlCompilationException("Caught exception while parsing query: " + sql + ": " + e.getMessage(), e);
    }
  }

  private static SqlNodeAndOptions extractSqlNodeAndOptions(SqlNodeList sqlNodeList) {
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
      } else if (sqlNode instanceof SqlPinotShowTables
          || sqlNode instanceof SqlPinotShowMaterializedViews
          || sqlNode instanceof SqlPinotCreateTable
          || sqlNode instanceof SqlPinotShowCreateTable
          || sqlNode instanceof SqlPinotDropTable
          || sqlNode instanceof SqlPinotCreateMaterializedView
          || sqlNode instanceof SqlPinotShowCreateMaterializedView
          || sqlNode instanceof SqlPinotDropMaterializedView) {
        // Pinot-native DDL statements; the controller dispatches these via the DDL endpoint.
        // Ordering: Catalog → Table → Materialized View, lifecycle CREATE → SHOW CREATE → DROP,
        // matching `DdlOperation` and `DdlCompiler#compile`.
        if (sqlType == null) {
          sqlType = PinotSqlType.DDL;
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

  /// Should only be used for testing query rewriters.
  public static PinotQuery compileToPinotQueryWithoutRewrites(String sql) {
    return compileWithoutRewrite(compileToSqlNodeAndOptions(sql).getSqlNode());
  }

  public static PinotQuery compileToPinotQuery(SqlNodeAndOptions sqlNodeAndOptions) {
    // Compile SqlNode into PinotQuery
    PinotQuery pinotQuery = compileSqlNodeToPinotQuery(sqlNodeAndOptions.getSqlNode());
    // Set query options into PinotQuery
    pinotQuery.setQueryOptions(sqlNodeAndOptions.getOptions());
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
            "'" + RequestUtils.prettyPrint(selectExpression) + "' should be functionally dependent on the columns "
                + "used in GROUP BY clause.");
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
    } else if (operator.equals(FilterKind.SEMANTIC_MATCH.name())) {
      // SEMANTIC_MATCH(column, 'query text', topK) — validated here, rewritten by QueryRewriter
      List<Expression> smOperands = filterExpression.getFunctionCall().getOperands();
      if (smOperands.size() < 2 || smOperands.size() > 3) {
        throw new IllegalStateException(
            "SEMANTIC_MATCH requires 2 or 3 arguments: SEMANTIC_MATCH(column, 'query text'[, topK])");
      }
      if (!smOperands.get(0).isSetIdentifier()) {
        throw new IllegalStateException(
            "The first argument of SEMANTIC_MATCH must be a column identifier");
      }
      if (!smOperands.get(1).isSetLiteral() || smOperands.get(1).getLiteral().isSetNullValue()) {
        throw new IllegalStateException(
            "The second argument of SEMANTIC_MATCH must be a non-null string literal (query text)");
      }
      if (smOperands.size() == 3 && !smOperands.get(2).isSetLiteral()) {
        throw new IllegalStateException(
            "The third argument of SEMANTIC_MATCH must be an integer literal (topK)");
      }
    } else if (operator.equals(FilterKind.VECTOR_SIMILARITY.name())) {
      Expression vectorIdentifier = filterExpression.getFunctionCall().getOperands().get(0);
      if (!vectorIdentifier.isSetIdentifier()) {
        throw new IllegalStateException("The first argument of VECTOR_SIMILARITY must be an identifier of float array, "
            + "the signature is VECTOR_SIMILARITY(float[], float[], int).");
      }
      Expression vectorLiteral = filterExpression.getFunctionCall().getOperands().get(1);
      /*
       * Array Literal could be either:
       * 1. a function of type 'ARRAYVALUECONSTRUCTOR' with operands of float/double
       * 2. a float/double array literals
       * Also check in
       * {@link org.apache.pinot.sql.parsers.rewriter.PredicateComparisonRewriter#updateFunctionExpression(Expression)}
       */
      if ((vectorLiteral.isSetFunctionCall() && !vectorLiteral.getFunctionCall().getOperator().equalsIgnoreCase(
          "arrayvalueconstructor"))
          || (vectorLiteral.isSetLiteral() && !vectorLiteral.getLiteral().isSetFloatArrayValue()
          && !vectorLiteral.getLiteral().isSetDoubleArrayValue())) {
        throw new IllegalStateException("The second argument of VECTOR_SIMILARITY must be a float/double array "
            + "literal, the signature is VECTOR_SIMILARITY(float[], float[], int)");
      }
      if (filterExpression.getFunctionCall().getOperands().size() == 3) {
        Expression topK = filterExpression.getFunctionCall().getOperands().get(2);
        if (!topK.isSetLiteral()) {
          throw new IllegalStateException("The third argument of VECTOR_SIMILARITY must be an integer literal, "
              + "the signature is VECTOR_SIMILARITY(float[], float[], int)");
        }
      }
    } else if (operator.equals(FilterKind.VECTOR_SIMILARITY_RADIUS.name())) {
      Expression vectorIdentifier = filterExpression.getFunctionCall().getOperands().get(0);
      if (!vectorIdentifier.isSetIdentifier()) {
        throw new IllegalStateException(
            "The first argument of VECTOR_SIMILARITY_RADIUS must be an identifier of float array, "
                + "the signature is VECTOR_SIMILARITY_RADIUS(float[], float[], float).");
      }
      Expression vectorLiteral = filterExpression.getFunctionCall().getOperands().get(1);
      if ((vectorLiteral.isSetFunctionCall() && !vectorLiteral.getFunctionCall().getOperator().equalsIgnoreCase(
          "arrayvalueconstructor"))
          || (vectorLiteral.isSetLiteral() && !vectorLiteral.getLiteral().isSetFloatArrayValue()
          && !vectorLiteral.getLiteral().isSetDoubleArrayValue())) {
        throw new IllegalStateException(
            "The second argument of VECTOR_SIMILARITY_RADIUS must be a float/double array "
                + "literal, the signature is VECTOR_SIMILARITY_RADIUS(float[], float[], float)");
      }
      if (filterExpression.getFunctionCall().getOperands().size() == 3) {
        Expression threshold = filterExpression.getFunctionCall().getOperands().get(2);
        if (!threshold.isSetLiteral()) {
          throw new IllegalStateException(
              "The third argument of VECTOR_SIMILARITY_RADIUS must be a numeric literal, "
                  + "the signature is VECTOR_SIMILARITY_RADIUS(float[], float[], float)");
        }
      }
    } else {
      List<Expression> operands = filterExpression.getFunctionCall().getOperands();
      for (int i = 1; i < operands.size(); i++) {
        if (operands.get(i).getLiteral().isSetNullValue()) {
          throw new IllegalStateException("Using NULL in " + operator + " filter is not supported");
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

  /// Check recursively if an expression contains any reference not appearing in the GROUP BY clause.
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

  /// Extract all the identifiers from given expressions.
  ///
  /// @param expressions
  /// @param excludeAs if true, ignores the right side identifier for AS function.
  /// @return all the identifier names.
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
          identifiers.addAll(extractIdentifiers(List.of(function.getOperands().get(0)), true));
        } else {
          identifiers.addAll(extractIdentifiers(function.getOperands(), excludeAs));
        }
      }
    }
    return identifiers;
  }

  /// Compiles a String expression into [Expression].
  ///
  /// @param expression String expression.
  /// @return [Expression] equivalent of the string.
  ///
  /// @throws SqlCompilationException if String is not a valid expression.
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
    sqlParser.setIdentifierMaxLength(CALCITE_SQL_PARSER_IDENTIFIER_MAX_LENGTH);
    return sqlParser;
  }

  public static PinotQuery compileSqlNodeToPinotQuery(SqlNode sqlNode) {
    PinotQuery pinotQuery = compileWithoutRewrite(sqlNode);
    queryRewrite(pinotQuery);
    // Rewrite GROUPING / GROUPING_ID after validation: it references the internal $groupingId column, which is not a
    // GROUP BY column and would otherwise fail the group-by validation. The original GROUPING(col) form (whose
    // argument is a group-by column) validates cleanly.
    rewriteGroupingFunctions(pinotQuery);
    return pinotQuery;
  }

  private static PinotQuery compileWithoutRewrite(SqlNode sqlNode) {
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
      convertGroupBy(pinotQuery, groupByNodeList);
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

  public static void queryRewrite(PinotQuery pinotQuery) {
    for (QueryRewriter queryRewriter : QUERY_REWRITERS) {
      pinotQuery = queryRewriter.rewrite(pinotQuery);
    }
    // Validate
    validate(pinotQuery);
  }

  /// Applies a specific query rewriter to the given PinotQuery and validates the result.
  /// This method searches for a rewriter by class name and applies it to transform the query.
  ///
  /// @param pinotQuery the query to be rewritten
  /// @param rewriterClass the class name of the query rewriter to apply
  /// @throws IllegalArgumentException if no rewriter with the specified class name is found
  public static void queryRewrite(PinotQuery pinotQuery, Class<? extends QueryRewriter> rewriterClass) {
    QueryRewriter queryRewriter = QUERY_REWRITERS.stream()
        .filter(rewriter -> rewriter.getClass().equals(rewriterClass))
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException("Query rewriter not found: " + rewriterClass.getName()));
    queryRewriter.rewrite(pinotQuery);
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
    // NOTE: Create an ArrayList because we might need to modify the list later
    List<Expression> selectExpr = new ArrayList<>(1);
    selectExpr.add(convertDistinctAndSelectListToFunctionExpression(selectList));
    return selectExpr;
  }

  private static List<Expression> convertSelectList(SqlNodeList selectList) {
    List<Expression> selectExpr = new ArrayList<>(selectList.size());
    for (SqlNode sqlNode : selectList) {
      selectExpr.add(toExpression(sqlNode));
    }
    return selectExpr;
  }

  /**
   * Populates the GROUP BY clause of {@code pinotQuery}. For an ordinary GROUP BY this just sets the flat
   * {@code groupByList} (no masks), preserving existing behavior. When the clause contains {@code ROLLUP},
   * {@code CUBE}, {@code GROUPING SETS}, or an empty grouping set {@code ()}, it normalizes them into the
   * de-duplicated union of group-by columns ({@code groupByList}) plus the participation bitmasks
   * ({@code groupingSetsMasks}, one per grouping set) consumed by the engine. See {@link GroupingSets}.
   */
  private static void convertGroupBy(PinotQuery pinotQuery, SqlNodeList groupByNodeList) {
    // GROUP BY DISTINCT wraps the whole element list in a single internal $GROUP_BY_DISTINCT call.
    List<SqlNode> elements = groupByNodeList.getList();
    boolean distinct = false;
    if (elements.size() == 1 && elements.get(0).getKind() == SqlKind.GROUP_BY_DISTINCT) {
      distinct = true;
      elements = ((SqlBasicCall) elements.get(0)).getOperandList();
    }
    if (!hasGroupingSetConstruct(elements)) {
      // Ordinary GROUP BY: keep the flat list, do not set masks (engine treats this as a single full set).
      List<Expression> groupByList = new ArrayList<>(elements.size());
      for (SqlNode element : elements) {
        groupByList.add(toExpression(element));
      }
      pinotQuery.setGroupByList(groupByList);
      return;
    }
    // Normalize ROLLUP/CUBE/GROUPING SETS into union columns + participation masks (Cartesian over elements).
    // The column count is bounded in unionIndexOf (so masks cannot overflow) and the running set count is bounded
    // here (so a wide CUBE cannot OOM by eagerly materializing 2^N masks).
    Map<Expression, Integer> unionIndex = new LinkedHashMap<>();
    List<List<Integer>> perElementMasks = new ArrayList<>(elements.size());
    long setCount = 1;
    for (SqlNode element : elements) {
      List<Integer> elementMasks = expandGroupingElement(element, unionIndex);
      setCount *= elementMasks.size();
      checkGroupingSetCount(setCount);
      perElementMasks.add(elementMasks);
    }
    List<Integer> masks = GroupingSets.crossProduct(perElementMasks);
    if (distinct) {
      // GROUP BY DISTINCT removes duplicate grouping sets.
      masks = new ArrayList<>(new LinkedHashSet<>(masks));
    }
    pinotQuery.setGroupByList(new ArrayList<>(unionIndex.keySet()));
    pinotQuery.setGroupingSetsMasks(masks);
  }

  private static void checkGroupingSetCount(long count) {
    if (count > GroupingSets.MAX_GROUPING_SETS) {
      throw new SqlCompilationException(
          "GROUP BY produces too many grouping sets (limit " + GroupingSets.MAX_GROUPING_SETS
              + "); reduce the size of ROLLUP / CUBE / GROUPING SETS");
    }
  }

  private static boolean hasGroupingSetConstruct(List<SqlNode> elements) {
    for (SqlNode element : elements) {
      SqlKind kind = element.getKind();
      if (kind == SqlKind.ROLLUP || kind == SqlKind.CUBE || kind == SqlKind.GROUPING_SETS
          || (element instanceof SqlNodeList && ((SqlNodeList) element).size() == 0)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Expands one top-level GROUP BY element into the list of participation masks it contributes (the alternatives
   * that the Cartesian product combines). Columns are registered into {@code unionIndex} in first-appearance order.
   */
  private static List<Integer> expandGroupingElement(SqlNode element, Map<Expression, Integer> unionIndex) {
    switch (element.getKind()) {
      case ROLLUP:
        return GroupingSets.rollup(unionIndices(((SqlBasicCall) element).getOperandList(), unionIndex));
      case CUBE: {
        List<SqlNode> cubeColumns = ((SqlBasicCall) element).getOperandList();
        // CUBE(n) expands to 2^n sets; reject before cube() materializes them (guard the shift against overflow).
        int arity = cubeColumns.size();
        checkGroupingSetCount(arity >= Long.SIZE - 1 ? Long.MAX_VALUE : 1L << arity);
        return GroupingSets.cube(unionIndices(cubeColumns, unionIndex));
      }
      case GROUPING_SETS: {
        List<Integer> masks = new ArrayList<>();
        for (SqlNode item : ((SqlBasicCall) element).getOperandList()) {
          masks.addAll(expandGroupingSetItem(item, unionIndex));
          checkGroupingSetCount(masks.size());
        }
        return masks;
      }
      default:
        // A plain column, a parenthesized set (SqlNodeList), or a row of columns (e.g. the empty set () or (a, b)).
        return List.of(setMask(element, unionIndex));
    }
  }

  /** Expands a single {@code GROUPING SETS} item, which is one set (parenthesized list, row, or column) or a nested
   * ROLLUP/CUBE/GROUPING SETS. */
  private static List<Integer> expandGroupingSetItem(SqlNode item, Map<Expression, Integer> unionIndex) {
    SqlKind kind = item.getKind();
    if (kind == SqlKind.ROLLUP || kind == SqlKind.CUBE || kind == SqlKind.GROUPING_SETS) {
      return expandGroupingElement(item, unionIndex);
    }
    return List.of(setMask(item, unionIndex));
  }

  /**
   * Participation mask for one grouping set described by a node. A parenthesized group arrives either as a
   * {@link SqlNodeList} (e.g. the empty set {@code ()}) or as a {@code ROW} call (e.g. {@code (a, b)}); a bare column
   * arrives as itself. Empty group => mask 0 (the grand total).
   */
  private static int setMask(SqlNode node, Map<Expression, Integer> unionIndex) {
    List<SqlNode> columns;
    if (node instanceof SqlNodeList) {
      columns = ((SqlNodeList) node).getList();
    } else if (node.getKind() == SqlKind.ROW) {
      columns = ((SqlBasicCall) node).getOperandList();
    } else {
      return GroupingSets.maskOf(unionIndexOf(node, unionIndex));
    }
    int mask = 0;
    for (SqlNode column : columns) {
      mask |= GroupingSets.maskOf(unionIndexOf(column, unionIndex));
    }
    return mask;
  }

  private static int[] unionIndices(List<SqlNode> columns, Map<Expression, Integer> unionIndex) {
    int[] indices = new int[columns.size()];
    for (int i = 0; i < columns.size(); i++) {
      indices[i] = unionIndexOf(columns.get(i), unionIndex);
    }
    return indices;
  }

  /** Returns the stable union index of a group-by column, assigning a new one (in first-appearance order) if new. */
  private static int unionIndexOf(SqlNode column, Map<Expression, Integer> unionIndex) {
    Expression expression = toExpression(column);
    Integer index = unionIndex.get(expression);
    if (index != null) {
      return index;
    }
    int newIndex = unionIndex.size();
    // Bound the union here so participation masks (1 << index) cannot overflow the 32-bit int.
    if (newIndex >= GroupingSets.MAX_GROUP_BY_COLUMNS) {
      throw new SqlCompilationException("GROUPING SETS / ROLLUP / CUBE support at most "
          + GroupingSets.MAX_GROUP_BY_COLUMNS + " group-by columns");
    }
    unionIndex.put(expression, newIndex);
    return newIndex;
  }

  /**
   * Rewrites every {@code GROUPING(col)} / {@code GROUPING_ID(c1, ..., cp)} call in SELECT / HAVING / ORDER BY into a
   * scalar over the synthetic {@code $groupingId} column. Each argument column is resolved to a bit shift
   * ({@code numColumns - 1 - unionIndex}) against the GROUP BY union. Indicator functions are only valid with a
   * grouping-set GROUP BY (ROLLUP / CUBE / GROUPING SETS).
   */
  private static void rewriteGroupingFunctions(PinotQuery pinotQuery) {
    boolean hasGroupingSets = pinotQuery.getGroupingSetsMasks() != null;
    List<Expression> groupByList = pinotQuery.getGroupByList();
    Map<Expression, Integer> unionIndex = new HashMap<>();
    if (groupByList != null) {
      for (int i = 0; i < groupByList.size(); i++) {
        unionIndex.putIfAbsent(groupByList.get(i), i);
      }
    }
    int numColumns = unionIndex.size();
    if (pinotQuery.getSelectList() != null) {
      for (Expression expression : pinotQuery.getSelectList()) {
        rewriteGroupingExpression(expression, hasGroupingSets, unionIndex, numColumns);
      }
    }
    if (pinotQuery.getHavingExpression() != null) {
      rewriteGroupingExpression(pinotQuery.getHavingExpression(), hasGroupingSets, unionIndex, numColumns);
    }
    if (pinotQuery.getOrderByList() != null) {
      for (Expression expression : pinotQuery.getOrderByList()) {
        rewriteGroupingExpression(expression, hasGroupingSets, unionIndex, numColumns);
      }
    }
  }

  private static void rewriteGroupingExpression(Expression expression, boolean hasGroupingSets,
      Map<Expression, Integer> unionIndex, int numColumns) {
    if (expression.getType() != ExpressionType.FUNCTION) {
      return;
    }
    Function function = expression.getFunctionCall();
    if (isGroupingFunction(function.getOperator())) {
      if (!hasGroupingSets) {
        throw new SqlCompilationException(
            "GROUPING / GROUPING_ID can only be used with GROUP BY ROLLUP / CUBE / GROUPING SETS");
      }
      rewriteGroupingCall(function, unionIndex, numColumns);
      return;
    }
    List<Expression> operands = function.getOperands();
    if (operands != null) {
      for (Expression operand : operands) {
        rewriteGroupingExpression(operand, hasGroupingSets, unionIndex, numColumns);
      }
    }
  }

  private static boolean isGroupingFunction(String operator) {
    return operator.equalsIgnoreCase("grouping") || operator.equalsIgnoreCase("groupingid")
        || operator.equalsIgnoreCase("grouping_id");
  }

  private static void rewriteGroupingCall(Function function, Map<Expression, Integer> unionIndex, int numColumns) {
    List<Expression> columns = function.getOperands();
    int[] shifts = new int[columns.size()];
    for (int i = 0; i < columns.size(); i++) {
      Integer index = unionIndex.get(columns.get(i));
      if (index == null) {
        throw new SqlCompilationException("Argument of GROUPING / GROUPING_ID must be a GROUP BY column: "
            + RequestUtils.prettyPrint(columns.get(i)));
      }
      // $groupingId bit (numColumns - 1 - index) holds GROUPING(column); see GroupingSets conventions.
      shifts[i] = numColumns - 1 - index;
    }
    Expression groupingIdColumn = RequestUtils.getIdentifierExpression(GroupingSets.GROUPING_ID_COLUMN);
    if (function.getOperator().equalsIgnoreCase("grouping") && shifts.length == 1) {
      // GROUPING(col) => grouping($groupingId, shift)
      function.setOperator("grouping");
      function.setOperands(new ArrayList<>(List.of(groupingIdColumn, RequestUtils.getLiteralExpression(shifts[0]))));
    } else {
      // GROUPING_ID(c1..cp) (or GROUPING with multiple args) => groupingId($groupingId, [shifts])
      function.setOperator("groupingid");
      function.setOperands(new ArrayList<>(
          List.of(groupingIdColumn, RequestUtils.getLiteralExpression(RequestUtils.getLiteral(shifts)))));
    }
  }

  private static List<Expression> convertOrderByList(SqlNodeList orderList) {
    List<Expression> orderByExpr = new ArrayList<>(orderList.size());
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
      expression =
          RequestUtils.getFunctionExpression(NULLS_LAST, convertOrderBy(basicCall.getOperandList().get(0), true));
    } else if (node.getKind() == SqlKind.NULLS_FIRST) {
      SqlBasicCall basicCall = (SqlBasicCall) node;
      expression =
          RequestUtils.getFunctionExpression(NULLS_FIRST, convertOrderBy(basicCall.getOperandList().get(0), true));
    } else if (node.getKind() == SqlKind.DESCENDING) {
      SqlBasicCall basicCall = (SqlBasicCall) node;
      expression = RequestUtils.getFunctionExpression(DESC, convertOrderBy(basicCall.getOperandList().get(0), false));
    } else if (createAscExpression) {
      expression = RequestUtils.getFunctionExpression(ASC, toExpression(node));
    } else {
      return toExpression(node);
    }
    return expression;
  }

  /// DISTINCT is implemented as an aggregation function so need to take the select list items
  /// and convert them into a single function expression for handing over to execution engine
  /// either as a PinotQuery or BrokerRequest via conversion
  /// @param selectList select list items
  /// @return DISTINCT function expression
  private static Expression convertDistinctAndSelectListToFunctionExpression(SqlNodeList selectList) {
    List<Expression> operands = new ArrayList<>(selectList.size());
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
      operands.add(columnExpression);
    }
    return RequestUtils.getFunctionExpression("distinct", operands);
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
        return RequestUtils.getFunctionExpression("as", leftExpr, rightExpr);
      case CASE:
        // CASE WHEN Statement is model as a function with variable length parameters.
        // Assume N is number of WHEN Statements, total number of parameters is (2 * N + 1).
        // - N: Convert each WHEN Statement into a function Expression;
        // - N: Convert each THEN Statement into an Expression;
        // - 1: Convert ELSE Statement into an Expression.
        SqlCase caseSqlNode = (SqlCase) node;
        SqlNodeList whenOperands = caseSqlNode.getWhenOperands();
        int numWhenOperands = whenOperands.size();
        SqlNodeList thenOperands = caseSqlNode.getThenOperands();
        Preconditions.checkState(numWhenOperands == thenOperands.size());
        SqlNode elseOperand = caseSqlNode.getElseOperand();
        List<Expression> caseOperands = new ArrayList<>(2 * numWhenOperands + 1);
        for (int i = 0; i < numWhenOperands; i++) {
          caseOperands.add(toExpression(whenOperands.get(i)));
          caseOperands.add(toExpression(thenOperands.get(i)));
        }
        Expression elseExpression = toExpression(elseOperand);
        if (isAggregateExpression(elseExpression)) {
          throw new SqlCompilationException(
              "Aggregation functions inside ELSE Clause is not supported - " + elseExpression);
        }
        caseOperands.add(elseExpression);
        return RequestUtils.getFunctionExpression("case", caseOperands);
      default:
        if (node instanceof SqlDataTypeSpec) {
          // This is to handle expression like: CAST(col AS INT)
          return RequestUtils.getLiteralExpression(((SqlDataTypeSpec) node).getTypeName().getSimple());
        } else if (node instanceof SqlWindow) {
          // Window definitions appear as operands of OVER calls. PinotQuery does not model window frames directly, but
          // compiling them as literals keeps parsing/table-name extraction from failing on multi-stage window queries.
          return RequestUtils.getLiteralExpression(node.toString());
        } else if (node instanceof SqlBasicCall) {
          return compileFunctionExpression((SqlBasicCall) node);
        } else {
          throw new SqlCompilationException("Unsupported sql node - " + node);
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
      case MINUS_PREFIX:
        // SqlKind.MINUS_PREFIX.name() would canonicalize to "minusprefix", which has no matching entry in
        // FunctionRegistry. Map directly to the registered NegateScalarFunction name instead.
        canonicalName = NegateScalarFunction.FUNCTION_NAME;
        break;
      case PLUS_PREFIX:
        // Unary plus is identity -- unwrap to the operand directly (no function node needed).
        return toExpression(functionNode.getOperandList().get(0));
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
    ParserUtils.validateFunction(canonicalName, operands);
    Expression functionExpression = RequestUtils.getFunctionExpression(canonicalName, operands);
    if (negated) {
      return RequestUtils.getFunctionExpression(FilterKind.NOT.name(), functionExpression);
    } else {
      return functionExpression;
    }
  }

  /// Convert Calcite operator tree made up of ITEM and DOT functions to an identifier. For example, the operator tree
  /// shown below will be converted to IDENTIFIER "jsoncolumn.data[0][1].a.b[0]".
  ///
  /// ├── ITEM(jsoncolumn.data[0][1].a.b[0])
  /// ├── LITERAL (0)
  /// └── DOT (jsoncolumn.daa[0][1].a.b)
  /// ├── IDENTIFIER (b)
  /// └── DOT (jsoncolumn.data[0][1].a)
  /// ├── IDENTIFIER (a)
  /// └── ITEM (jsoncolumn.data[0][1])
  /// ├── LITERAL (1)
  /// └── ITEM (jsoncolumn.data[0])
  /// ├── LITERAL (1)
  /// └── IDENTIFIER (jsoncolumn.data)
  ///
  /// @param functionNode Root node of the DOT and/or ITEM operator function chain.
  /// @param pathBuilder StringBuilder representation of path represented by DOT and/or ITEM function chain.
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

  /// Helper method to flatten the operands for the AND expression.
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
    return RequestUtils.getFunctionExpression(FilterKind.AND.name(), operands);
  }

  /// Helper method to flatten the operands for the OR expression.
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
    return RequestUtils.getFunctionExpression(FilterKind.OR.name(), operands);
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
