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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSelectKeyword;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.sql.parser.babel.SqlBabelParserImpl;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.pinot.common.function.AggregationFunctionType;
import org.apache.pinot.common.function.FunctionDefinitionRegistry;
import org.apache.pinot.common.request.DataSource;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.Identifier;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.pql.parsers.Pql2Compiler;
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
  private static Lex PINOT_LEX = Lex.MYSQL_ANSI;

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
  }

  private static void validateSelectionClause(Map<Identifier, Expression> aliasMap, PinotQuery pinotQuery)
      throws SqlCompilationException {
    // Sanity check on selection expression shouldn't use alias reference.
    Set<String> aliasKeys = new HashSet<>();
    for (Identifier identifier : aliasMap.keySet()) {
      aliasKeys.add(identifier.getName().toLowerCase());
    }
    for (Expression selectExpr : pinotQuery.getSelectList()) {
      matchIdentifierInAliasMap(selectExpr, aliasKeys);
    }
  }

  private static void matchIdentifierInAliasMap(Expression selectExpr, Set<String> aliasKeys)
      throws SqlCompilationException {
    if (selectExpr.getFunctionCall() != null) {
      if (selectExpr.getFunctionCall().getOperator().equalsIgnoreCase("AS")) {
        matchIdentifierInAliasMap(selectExpr.getFunctionCall().getOperands().get(0), aliasKeys);
      } else {
        for (Expression operand : selectExpr.getFunctionCall().getOperands()) {
          matchIdentifierInAliasMap(operand, aliasKeys);
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
    for (Expression selectExpression : pinotQuery.getSelectList()) {
      if (!isAggregateExpression(selectExpression)) {
        boolean foundInGroupByClause = false;
        Expression selectionToCheck;
        if (selectExpression.getFunctionCall() != null && selectExpression.getFunctionCall().getOperator()
            .equalsIgnoreCase("AS")) {
          selectionToCheck = selectExpression.getFunctionCall().getOperands().get(0);
        } else {
          selectionToCheck = selectExpression;
        }
        for (Expression groupByExpression : pinotQuery.getGroupByList()) {
          if (groupByExpression.equals(selectionToCheck)) {
            foundInGroupByClause = true;
          }
        }
        if (!foundInGroupByClause) {
          throw new SqlCompilationException(
              "'" + RequestUtils.prettyPrint(selectionToCheck) + "' should appear in GROUP BY clause.");
        }
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

  private static boolean isAggregateExpression(Expression expression) {
    if (expression.getFunctionCall() != null) {
      String operator = expression.getFunctionCall().getOperator();
      try {
        AggregationFunctionType.getAggregationFunctionType(operator);
        return true;
      } catch (IllegalArgumentException e) {
      }
      for (Expression operand : expression.getFunctionCall().getOperands()) {
        if (isAggregateExpression(operand)) {
          return true;
        }
      }
    }
    return false;
  }

  private static Set<String> extractIdentifiers(List<Expression> expressions) {
    Set<String> identifiers = new HashSet<>();
    for (Expression expression : expressions) {
      if (expression.getIdentifier() != null) {
        identifiers.add(expression.getIdentifier().getName());
      } else if (expression.getFunctionCall() != null) {
        identifiers.addAll(extractIdentifiers(expression.getFunctionCall().getOperands()));
      }
    }
    return identifiers;
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
    SqlParser.ConfigBuilder parserBuilder = SqlParser.configBuilder();
    parserBuilder.setLex(PINOT_LEX);
    // BABEL is a very liberal conformance value that allows anything supported by any dialect
    parserBuilder.setConformance(SqlConformanceEnum.BABEL);
    parserBuilder.setParserFactory(SqlBabelParserImpl.FACTORY);
    SqlParser sqlParser = SqlParser.create(sql, parserBuilder.build());
    final SqlNode sqlNode;
    try {
      sqlNode = sqlParser.parseQuery();
    } catch (SqlParseException e) {
      throw new SqlCompilationException(e);
    }

    PinotQuery pinotQuery = new PinotQuery();

    SqlSelect selectSqlNode;
    SqlOrderBy selectOrderBySqlNode = null;
    switch (sqlNode.getKind()) {
      case ORDER_BY:
        selectOrderBySqlNode = (SqlOrderBy) sqlNode;
        if (selectOrderBySqlNode.orderList != null) {
          pinotQuery.setOrderByList(convertOrderByList(selectOrderBySqlNode.orderList));
        }
        if (selectOrderBySqlNode.fetch != null) {
          pinotQuery.setLimit(Integer.valueOf(((SqlNumericLiteral) selectOrderBySqlNode.fetch).toValue()));
        }
        if (selectOrderBySqlNode.offset != null) {
          pinotQuery.setOffset(Integer.valueOf(((SqlNumericLiteral) selectOrderBySqlNode.offset).toValue()));
        }
      case SELECT:
        if (sqlNode instanceof SqlOrderBy) {
          selectSqlNode = (SqlSelect) selectOrderBySqlNode.query;
        } else {
          selectSqlNode = (SqlSelect) sqlNode;
        }

        if (selectSqlNode.getFetch() != null) {
          pinotQuery.setLimit(Integer.valueOf(((SqlNumericLiteral) selectSqlNode.getFetch()).toValue()));
        }
        if (selectSqlNode.getOffset() != null) {
          pinotQuery.setOffset(Integer.valueOf(((SqlNumericLiteral) selectSqlNode.getOffset()).toValue()));
        }
        DataSource dataSource = new DataSource();
        dataSource.setTableName(selectSqlNode.getFrom().toString());
        pinotQuery.setDataSource(dataSource);
        if (selectSqlNode.getModifierNode(SqlSelectKeyword.DISTINCT) != null) {
          if (selectSqlNode.getGroup() != null) {
            // TODO: explore support for GROUP BY with DISTINCT
            throw new SqlCompilationException("DISTINCT with GROUP BY is not supported");
          }
          pinotQuery.setSelectList(convertDistinctSelectList(selectSqlNode.getSelectList()));
        } else {
          pinotQuery.setSelectList(convertSelectList(selectSqlNode.getSelectList()));
        }

        if (selectSqlNode.getWhere() != null) {
          pinotQuery.setFilterExpression(toExpression(selectSqlNode.getWhere()));
        }
        if (selectSqlNode.getGroup() != null) {
          pinotQuery.setGroupByList(convertSelectList(selectSqlNode.getGroup()));
        }
        break;
      default:
        throw new RuntimeException(
            "Unable to convert SqlNode: " + sqlNode + " to PinotQuery. Unknown node type: " + sqlNode.getKind());
    }
    Map<Identifier, Expression> aliasMap = extractAlias(pinotQuery.getSelectList());
    applyAlias(aliasMap, pinotQuery);
    validate(aliasMap, pinotQuery);
    return pinotQuery;
  }

  private static void applyAlias(Map<Identifier, Expression> aliasMap, PinotQuery pinotQuery) {
    if (pinotQuery.isSetFilterExpression()) {
      applyAlias(aliasMap, pinotQuery.getFilterExpression());
    }
    if (pinotQuery.isSetGroupByList()) {
      for (Expression groupByExpr : pinotQuery.getGroupByList()) {
        applyAlias(aliasMap, groupByExpr);
      }
    }
    if (pinotQuery.isSetOrderByList()) {
      for (Expression orderByExpr : pinotQuery.getOrderByList()) {
        applyAlias(aliasMap, orderByExpr);
      }
    }
  }

  private static void applyAlias(Map<Identifier, Expression> aliasMap, Expression expression) {
    if (expression == null) {
      return;
    }
    Identifier identifierKey = expression.getIdentifier();
    if ((identifierKey != null) && (aliasMap.containsKey(identifierKey))) {
      Expression aliasExpression = aliasMap.get(identifierKey);
      expression.setType(aliasExpression.getType()).setIdentifier(aliasExpression.getIdentifier())
          .setFunctionCall(aliasExpression.getFunctionCall()).setLiteral(aliasExpression.getLiteral());
    }
    if (expression.getFunctionCall() != null) {
      for (Expression operand : expression.getFunctionCall().getOperands()) {
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
      if (functionCall.getOperator().equalsIgnoreCase("AS")) {
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
    if (!Pql2Compiler.ENABLE_DISTINCT) {
      throw new SqlCompilationException("Support for DISTINCT is currently disabled in Pinot");
    }
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
    Iterator<SqlNode> iterator = selectList.iterator();
    while (iterator.hasNext()) {
      SqlNode next = iterator.next();
      Expression columnExpression = toExpression(next);
      if (columnExpression.getType() == ExpressionType.IDENTIFIER && columnExpression.getIdentifier().name
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
      default:
        SqlBasicCall funcSqlNode = (SqlBasicCall) node;
        String funcName = funcSqlNode.getOperator().getKind().name();
        if (funcSqlNode.getOperator().getKind() == SqlKind.OTHER_FUNCTION) {
          funcName = funcSqlNode.getOperator().getName();
        }
        final Expression funcExpr = RequestUtils.getFunctionExpression(funcName);
        for (SqlNode child : funcSqlNode.getOperands()) {
          if (child instanceof SqlNodeList) {
            final Iterator<SqlNode> iterator = ((SqlNodeList) child).iterator();
            while (iterator.hasNext()) {
              final SqlNode next = iterator.next();
              funcExpr.getFunctionCall().addToOperands(toExpression(next));
            }
          } else {
            funcExpr.getFunctionCall().addToOperands(toExpression(child));
          }
        }
        return funcExpr;
    }
  }
}
