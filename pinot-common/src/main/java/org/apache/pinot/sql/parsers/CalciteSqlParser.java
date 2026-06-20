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
import org.apache.calcite.sql.SqlCall;
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
  /// The grouping-set discriminator is encoded as a 32-bit INT bitmask over the union group-by columns, so a
  /// GROUPING SETS / ROLLUP / CUBE query may reference at most 31 distinct grouping columns.
  public static final int MAX_GROUPING_SETS_COLUMNS = 31;
  /// Upper bound on the number of grouping sets a single query may expand to (guards against CUBE blow-up).
  public static final int MAX_GROUPING_SETS = 4096;
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

    // A GROUPING SETS / ROLLUP / CUBE query must contain at least one aggregation (in SELECT, HAVING or
    // ORDER-BY): without one the engine would execute it as a selection query and silently ignore the grouping
    // sets. Note that GROUPING() / GROUPING_ID() are not aggregation functions. (Plain non-aggregation GROUP BY
    // queries are rewritten to DISTINCT by NonAggregationGroupByToDistinctQueryRewriter, but that rewrite cannot
    // represent multiple grouping sets.)
    if (pinotQuery.getGroupingSetMasks() != null && aggregateExprCount == 0 && !hasAggregationOutsideSelect(
        pinotQuery)) {
      throw new SqlCompilationException(
          "GROUP BY GROUPING SETS / ROLLUP / CUBE requires at least one aggregation function in the query");
    }
  }

  /// Returns true if the HAVING clause or any ORDER-BY expression contains an aggregation.
  private static boolean hasAggregationOutsideSelect(PinotQuery pinotQuery) {
    if (pinotQuery.getHavingExpression() != null && isAggregateExpression(pinotQuery.getHavingExpression())) {
      return true;
    }
    if (pinotQuery.getOrderByList() != null) {
      for (Expression orderBy : pinotQuery.getOrderByList()) {
        if (isAggregateExpression(orderBy)) {
          return true;
        }
      }
    }
    return false;
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
      setGroupByListAndGroupingSets(pinotQuery, groupByNodeList);
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

  /// Converts the GROUP BY clause into {@link PinotQuery#groupByList} and (when grouping constructs are
  /// present) {@link PinotQuery#groupingSetMasks}.
  ///
  /// For a plain GROUP BY (no ROLLUP / CUBE / GROUPING SETS) this behaves exactly like
  /// {@link #convertSelectList} and leaves {@code groupingSetMasks} unset, so non-grouping-set queries are
  /// unchanged. When grouping constructs are present, the grouping elements are cross-multiplied into the
  /// canonical, de-duplicated list of grouping sets (standard SQL semantics, e.g.
  /// {@code GROUP BY a, ROLLUP(b, c)} produces {@code {a,b,c}, {a,b}, {a}}); the ordered, de-duplicated
  /// union of all participating columns is stored as {@code groupByList}, and each grouping set is stored
  /// as a membership bitmask over that union — bit {@code i} set iff union column {@code i} participates,
  /// a mask of {@code 0} being the grand-total set {@code ()}.
  private static void setGroupByListAndGroupingSets(PinotQuery pinotQuery, SqlNodeList groupByNodeList) {
    boolean hasGroupingConstruct = false;
    for (SqlNode node : groupByNodeList) {
      if (isGroupingConstruct(node.getKind())) {
        hasGroupingConstruct = true;
        break;
      }
    }
    if (!hasGroupingConstruct) {
      pinotQuery.setGroupByList(convertSelectList(groupByNodeList));
      return;
    }

    /// Cross-multiply the grouping elements: the overall grouping sets are the union of one chosen set from
    /// each grouping element. Start with a single empty set (the multiplicative identity).
    List<LinkedHashSet<Expression>> combinedSets = new ArrayList<>();
    combinedSets.add(new LinkedHashSet<>());
    for (SqlNode element : groupByNodeList) {
      List<List<Expression>> elementSets = parseGroupingElement(element);
      List<LinkedHashSet<Expression>> next = new ArrayList<>(combinedSets.size() * elementSets.size());
      for (LinkedHashSet<Expression> prefix : combinedSets) {
        for (List<Expression> choice : elementSets) {
          LinkedHashSet<Expression> merged = new LinkedHashSet<>(prefix);
          merged.addAll(choice);
          next.add(merged);
        }
      }
      if (next.size() > MAX_GROUPING_SETS) {
        throw new SqlCompilationException(
            "GROUPING SETS / ROLLUP / CUBE expands to more than " + MAX_GROUPING_SETS + " grouping sets");
      }
      combinedSets = next;
    }

    /// Build the ordered, de-duplicated union of all participating columns (first-appearance order).
    LinkedHashMap<Expression, Integer> unionIndex = new LinkedHashMap<>();
    for (LinkedHashSet<Expression> set : combinedSets) {
      for (Expression expr : set) {
        unionIndex.computeIfAbsent(expr, k -> unionIndex.size());
      }
    }
    if (unionIndex.size() > MAX_GROUPING_SETS_COLUMNS) {
      throw new SqlCompilationException(
          "GROUPING SETS / ROLLUP / CUBE support at most " + MAX_GROUPING_SETS_COLUMNS
              + " distinct grouping columns, got " + unionIndex.size());
    }
    pinotQuery.setGroupByList(new ArrayList<>(unionIndex.keySet()));

    /// Encode each grouping set as a membership bitmask over the union columns (bit i set iff union column i
    /// participates), de-duplicating overlapping sets produced by CUBE/ROLLUP. A mask of 0 is the grand-total
    /// set (). The union is capped at MAX_GROUPING_SETS_COLUMNS (31) above, so a 32-bit mask never overflows.
    Set<Integer> seen = new HashSet<>();
    List<Integer> groupingSetMasks = new ArrayList<>();
    for (LinkedHashSet<Expression> set : combinedSets) {
      int mask = GroupingSets.participationMask(set.stream().mapToInt(unionIndex::get));
      if (seen.add(mask)) {
        groupingSetMasks.add(mask);
      }
    }
    pinotQuery.setGroupingSetMasks(groupingSetMasks);
  }

  private static boolean isGroupingConstruct(SqlKind kind) {
    return kind == SqlKind.ROLLUP || kind == SqlKind.CUBE || kind == SqlKind.GROUPING_SETS;
  }

  /// Expands a single grouping element into the list of grouping sets it represents (each set is an ordered
  /// list of column expressions; the empty list is the grand-total set).
  /// - {@code ROLLUP(l1, ..., ln)} -> the n+1 prefixes {@code {l1..ln}, {l1..ln-1}, ..., {l1}, {}}
  /// - {@code CUBE(l1, ..., ln)} -> the power set of the n levels
  /// - {@code GROUPING SETS(g1, ..., gm)} -> the concatenation of each operand's expansion (operands may be
  ///   nested ROLLUP/CUBE/GROUPING SETS or ordinary sets)
  /// - an ordinary grouping element (a single column or a parenthesized list) -> a single set
  private static List<List<Expression>> parseGroupingElement(SqlNode node) {
    switch (node.getKind()) {
      case ROLLUP: {
        List<List<Expression>> levels = parseLevels((SqlCall) node);
        List<List<Expression>> sets = new ArrayList<>(levels.size() + 1);
        for (int numLevels = levels.size(); numLevels >= 0; numLevels--) {
          List<Expression> set = new ArrayList<>();
          for (int i = 0; i < numLevels; i++) {
            set.addAll(levels.get(i));
          }
          sets.add(set);
        }
        return sets;
      }
      case CUBE: {
        List<List<Expression>> levels = parseLevels((SqlCall) node);
        int numLevels = levels.size();
        /// Guard the shift against overflow (1L << 64 wraps to 1) before comparing against the set-count cap.
        if (numLevels >= Integer.SIZE - 1 || (1L << numLevels) > MAX_GROUPING_SETS) {
          throw new SqlCompilationException(
              "CUBE expands to more than " + MAX_GROUPING_SETS + " grouping sets");
        }
        List<List<Expression>> sets = new ArrayList<>(1 << numLevels);
        for (int mask = (1 << numLevels) - 1; mask >= 0; mask--) {
          List<Expression> set = new ArrayList<>();
          for (int i = 0; i < numLevels; i++) {
            if ((mask & (1 << i)) != 0) {
              set.addAll(levels.get(i));
            }
          }
          sets.add(set);
        }
        return sets;
      }
      case GROUPING_SETS: {
        List<List<Expression>> sets = new ArrayList<>();
        for (SqlNode operand : ((SqlCall) node).getOperandList()) {
          sets.addAll(parseGroupingElement(operand));
        }
        return sets;
      }
      default:
        /// Ordinary grouping element: a single column expression or a parenthesized list of columns.
        return List.of(parseLevel(node));
    }
  }

  /// Parses each operand of a ROLLUP/CUBE call into a "level" (a level may be a single column or a
  /// parenthesized list of columns that roll up together).
  private static List<List<Expression>> parseLevels(SqlCall call) {
    List<List<Expression>> levels = new ArrayList<>(call.getOperandList().size());
    for (SqlNode operand : call.getOperandList()) {
      levels.add(parseLevel(operand));
    }
    return levels;
  }

  /// Parses a single grouping level/set node into its column expressions. Handles a parenthesized list
  /// (modeled by Calcite as either a {@link SqlNodeList} or a ROW call), and a bare single column. An empty
  /// parenthesized list yields an empty column list (the grand-total set).
  private static List<Expression> parseLevel(SqlNode node) {
    if (node instanceof SqlNodeList) {
      SqlNodeList list = (SqlNodeList) node;
      List<Expression> columns = new ArrayList<>(list.size());
      for (SqlNode column : list) {
        columns.add(toExpression(column));
      }
      return columns;
    }
    if (node.getKind() == SqlKind.ROW) {
      List<SqlNode> operands = ((SqlCall) node).getOperandList();
      List<Expression> columns = new ArrayList<>(operands.size());
      for (SqlNode column : operands) {
        columns.add(toExpression(column));
      }
      return columns;
    }
    return List.of(toExpression(node));
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
