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
package org.apache.pinot.query.parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.sql.parsers.SqlCompilationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Calcite parser to convert SQL expressions into {@link Expression}.
 *
 * <p>This class is extracted from {@link org.apache.pinot.sql.parsers.CalciteSqlParser}. It only contains the
 * {@link Expression} related info, this is used for ingestion and query rewrite.
 */
public class CalciteExpressionParser {
  private static final Logger LOGGER = LoggerFactory.getLogger(CalciteExpressionParser.class);

  private CalciteExpressionParser() {
    // do not instantiate.
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

  public static boolean isAggregateExpression(Expression expression) {
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

  public static boolean isAsFunction(Expression expression) {
    return expression.getFunctionCall() != null && expression.getFunctionCall().getOperator().equalsIgnoreCase("AS");
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
          identifiers.addAll(
              extractIdentifiers(Arrays.asList(expression.getFunctionCall().getOperands().get(0)), true));
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
    SqlParser sqlParser = SqlParser.create(expression, ParserUtils.PARSER_CONFIG);
    SqlNode sqlNode;
    try {
      sqlNode = sqlParser.parseExpression();
    } catch (SqlParseException e) {
      throw new SqlCompilationException("Caught exception while parsing expression: " + expression, e);
    }
    return toExpression(sqlNode);
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
        expression.getFunctionCall().addToOperands(toExpression(basicCall.getOperandList().get(0)));
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
            "Syntax error: Pinot currently does not support DISTINCT with *. Please specify each column name after "
                + "DISTINCT keyword");
      } else if (columnExpression.getType() == ExpressionType.FUNCTION) {
        Function functionCall = columnExpression.getFunctionCall();
        String function = functionCall.getOperator();
        if (AggregationFunctionType.isAggregationFunction(function)) {
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
    List<SqlNode> operands = functionNode.getOperandList();

    // Compile first operand of the function (either an identifier or another DOT and/or ITEM function).
    SqlKind kind0 = operands.get(0).getKind();
    if (kind0 == SqlKind.IDENTIFIER) {
      path.append(operands.get(0).toString());
    } else if (kind0 == SqlKind.DOT || kind0 == SqlKind.OTHER_FUNCTION) {
      SqlBasicCall function0 = (SqlBasicCall) operands.get(0);
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
    SqlKind kind1 = operands.get(1).getKind();
    if (kind1 == SqlKind.IDENTIFIER) {
      path.append(".").append(((SqlIdentifier) operands.get(1)).getSimple());
    } else if (kind1 == SqlKind.LITERAL) {
      path.append("[").append(((SqlLiteral) operands.get(1)).toValue()).append("]");
    } else {
      throw new SqlCompilationException("SELECT list item has bad path expression.");
    }
  }

  public static String canonicalize(String functionName) {
    return StringUtils.remove(functionName, '_').toLowerCase();
  }

  public static boolean isSameFunction(String function1, String function2) {
    return canonicalize(function1).equals(canonicalize(function2));
  }

  private static void validateFunction(String functionName, List<Expression> operands) {
    switch (canonicalize(functionName)) {
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
    Expression andExpression = RequestUtils.getFunctionExpression(SqlKind.AND.name());
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
    Expression andExpression = RequestUtils.getFunctionExpression(SqlKind.OR.name());
    andExpression.getFunctionCall().setOperands(operands);
    return andExpression;
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
