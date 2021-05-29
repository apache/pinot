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
package org.apache.pinot.core.query.optimizer.statement;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.common.function.scalar.ArithmeticFunctions;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.Identifier;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.core.query.optimizer.filter.StatementOptimizer;
import org.apache.pinot.pql.parsers.pql2.ast.FilterKind;
import org.apache.pinot.pql.parsers.pql2.ast.FloatingPointLiteralAstNode;
import org.apache.pinot.pql.parsers.pql2.ast.LiteralAstNode;
import org.apache.pinot.pql.parsers.pql2.ast.StringLiteralAstNode;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;


/**
 * This class will rewrite a query that has json path expressions into a query that uses JSON_EXTRACT_SCALAR and
 * JSON_MATCH functions.
 *
 * Example 1:
 *   From : SELECT jsonColumn.name.first
 *             FROM testTable
 *            WHERE jsonColumn.name.first IS NOT NULL
 *   TO   : SELECT JSON_EXTRACT_SCALAR(jsonColumn, '$.name.first', 'STRING', 'null') AS jsonColum.name.first
 *             FROM testTable
 *            WHERE JSON_MATCH('"$.name.first" IS NOT NULL')
 *
 * Note that the output datatype of any json path expression is 'STRING'. However, if json path expression appears as
 * an argument to a numerical function, then output of json path expression is set to 'DOUBLE' as shown in the example
 * below.
 *
 * Example 2:
 *   From : SELECT MIN(jsonColumn.id)
 *             FROM testTable
 *            WHERE jsonColumn.id IS NOT NULL
 *   TO   : SELECT MIN(JSON_EXTRACT_SCALAR(jsonColumn, '$.id', 'DOUBLE', '-Infinity')) AS min(jsonColum.id)
 *             FROM testTable
 *            WHERE JSON_MATCH('"$.id" IS NOT NULL')
 *
 * Example 3:
 *   From : SELECT jsonColumn.id, count(*)
 *             FROM testTable
 *            WHERE jsonColumn.name.first = 'Daffy' OR jsonColumn.id > 10
 *         GROUP BY jsonColumn.id
 *   TO   : SELECT MIN(JSON_EXTRACT_SCALAR(jsonColumn, '$.id', 'DOUBLE', '-Infinity')) AS min(jsonColumn.id)
 *             FROM testTable
 *            WHERE JSON_MATCH('"$.name.first" = ''Daffy''') OR JSON_MATCH('"$.id" = 10')
 *         GROUP BY JSON_EXTRACT_SCALAR(jsonColumn, '$.id', 'STRING', 'null');
 *
 * Note that in a filter expression, if json path appears in on the left-hand side, the right-hand side must be a
 * literal. In future this can be changed to have any expression on the right-hand side provided that we add support
 * for generating SQL fragements for a given expression.
 *
 * TODO:
 *  - Alias cannot currently be applied if another function wraps json path expression
 */
public class JsonStatementOptimizer implements StatementOptimizer {

  /**
   * Maintain a list of numerical functions that requiring json path expression to output numerical values. This allows
   * us to implicitly convert the output of json path expression to DOUBLE. TODO: There are much better ways of doing
   * this by changing the storage format (currently STRING) of JSON document and/or by pre-declaring the data types of
   * function operands.
   */
  private static Set<String> numericalFunctions = getNumericalFunctionList();

  @Override
  public void optimize(PinotQuery query, @Nullable Schema schema) {
    // In SELECT clause, replace JSON path expressions with JSON_EXTRACT_SCALAR function with an alias.
    List<Expression> expressions = query.getSelectList();
    for (Expression expression : expressions) {
      optimizeJsonIdentifier(expression, schema, false, true);
    }

    // In WHERE clause, replace JSON path expressions with JSON_MATCH function.
    Expression filter = query.getFilterExpression();
    if (filter != null) {
      optimizeJsonPredicate(filter, schema);
    }

    // In GROUP BY clause, replace JSON path expressions with JSON_EXTRACT_SCALAR function without an alias.
    expressions = query.getGroupByList();
    if (expressions != null) {
      for (Expression expression : expressions) {
        optimizeJsonIdentifier(expression, schema, false, false);
      }
    }

    // In HAVING clause, replace JSON path expressions with JSON_MATCH function.
    Expression expression = query.getHavingExpression();
    if (expression != null) {
      optimizeJsonPredicate(expression, schema);
    }
  }

  /** Replace an json path expression with an aliased JSON_EXTRACT_SCALAR function. */
  private static void optimizeJsonIdentifier(Expression expression, Schema schema, boolean hasNumericalOutput,
      boolean hasColumnAlias) {
    switch (expression.getType()) {
      case IDENTIFIER:
        String[] parts = getIdentifierParts(expression.getIdentifier());
        if (parts.length > 1 && isValidJSONColumn(parts[0], schema)) {
          // replace <column-name>.<json-path> with json_extract_scalar(<column-name>, '<json-path>', 'STRING', <JSON-null-value>)
          Function jsonExtractScalarFunction = getJsonExtractFunction(parts, hasNumericalOutput);
          Function aliasFunction =
              hasColumnAlias ? getAliasFunction(expression.getIdentifier().getName(), jsonExtractScalarFunction)
                  : jsonExtractScalarFunction;
          expression.setIdentifier(null);
          expression.setType(ExpressionType.FUNCTION);
          expression.setFunctionCall(aliasFunction);
        }
        break;
      case FUNCTION:
        Function function = expression.getFunctionCall();
        List<Expression> operands = function.getOperands();

        //hasColumnAlias &= isRelationalOperator(function.getOperator());
        hasNumericalOutput = numericalFunctions.contains(function.getOperator().toLowerCase(Locale.ROOT));
        for (Expression operand : operands) {
          // recursively check to see if there is a <json-column>.<json-path> identifier in this expression.
          optimizeJsonIdentifier(operand, schema, hasNumericalOutput, false);
        }
    }
  }

  /**
   * Example:
   *   Input:
   *     alias   : "jsoncolumn.x.y.z",
   *     function: JSON_EXTRACT_SCALAR('jsoncolumn', 'x.y.z', 'STRING', 'null')
   *   Output: AS(JSON_EXTRACT_SCALAR('jsoncolumn', 'x.y.z', 'STRING', 'null'), 'jsoncolumn.x.y.z')
   *
   * @return a Function with "AS" operator that wraps another function.
   * */
  private static Function getAliasFunction(String alias, Function function) {
    Function aliasFunction = new Function("AS");

    List<Expression> operands = new ArrayList<>();
    Expression expression = new Expression(ExpressionType.FUNCTION);
    expression.setFunctionCall(function);
    operands.add(expression);
    operands.add(RequestUtils.createIdentifierExpression(alias));
    aliasFunction.setOperands(operands);

    return aliasFunction;
  }

  /**
   * Example:
   * Input : ["jsoncolumn", "x","y","z[2]"]
   * Output: JSON_EXTRACT_SCALAR('jsoncolumn','$.x.y.z[2]','STRING','null')
   *
   * @param parts All the subparts of a fully qualified identifier (json path expression).
   * @param hasNumericalOutput true if json path expression must output a numerical value; otherwise, false.
   * @return a Function with JSON_EXTRACT_SCALAR operator created using parts of fully qualified identifier name.
   */
  private static Function getJsonExtractFunction(String[] parts, boolean hasNumericalOutput) {
    Function jsonExtractScalarFunction = new Function("JSON_EXTRACT_SCALAR");
    List<Expression> operands = new ArrayList<>();
    operands.add(RequestUtils.createIdentifierExpression(parts[0]));
    operands.add(RequestUtils.createLiteralExpression(new StringLiteralAstNode(getJsonPath(parts, false))));
    operands
        .add(RequestUtils.createLiteralExpression(new StringLiteralAstNode(hasNumericalOutput ? "DOUBLE" : "STRING")));

    LiteralAstNode defaultValue =
        hasNumericalOutput ? new FloatingPointLiteralAstNode(FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_DOUBLE)
            : new StringLiteralAstNode(FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_JSON);
    operands.add(RequestUtils.createLiteralExpression(defaultValue));
    jsonExtractScalarFunction.setOperands(operands);
    return jsonExtractScalarFunction;
  }

  /**
   * Example 1:
   * Input : "jsonColumn.name.first = 'daffy'"
   * Output: "JSON_MATCH(jsonColumn, '\"$.name.first\" = ''daffy''').
   *
   * Example 2:
   * Input : "jsonColumn.id = 4"
   * Output: "JSON_MATCH(jsonColumn, '\"$.id\" = 4')
   */
  private static void optimizeJsonPredicate(Expression expression, Schema schema) {
    if (expression.getType() == ExpressionType.FUNCTION) {
      Function function = expression.getFunctionCall();
      String operator = function.getOperator();
      FilterKind kind = FilterKind.valueOf(operator);
      List<Expression> operands = function.getOperands();
      switch (kind) {
        case AND:
        case OR: {
          operands.forEach(operand -> optimizeJsonPredicate(operand, schema));
          break;
        }
        case EQUALS:
        case NOT_EQUALS:
        case GREATER_THAN:
        case GREATER_THAN_OR_EQUAL:
        case LESS_THAN:
        case LESS_THAN_OR_EQUAL: {
          Expression left = operands.get(0);
          Expression right = operands.get(1);
          if (left.getType() == ExpressionType.IDENTIFIER && right.getType() == ExpressionType.LITERAL) {
            String[] parts = getIdentifierParts(left.getIdentifier());
            if (parts.length > 1 && isValidJSONColumn(parts[0], schema)) {
              Function jsonMatchFunction = new Function("JSON_MATCH");

              List<Expression> jsonMatchFunctionOperands = new ArrayList<>();
              jsonMatchFunctionOperands.add(RequestUtils.createIdentifierExpression(parts[0]));
              jsonMatchFunctionOperands.add(RequestUtils.createLiteralExpression(new StringLiteralAstNode(
                  getJsonPath(parts, true) + getOperatorSymbol(kind) + getLiteralValue(right.getLiteral()))));
              jsonMatchFunction.setOperands(jsonMatchFunctionOperands);

              expression.setFunctionCall(jsonMatchFunction);
            }
          }
          break;
        }
        case IS_NULL:
        case IS_NOT_NULL: {
          Expression operand = operands.get(0);
          if (operand.getType() == ExpressionType.IDENTIFIER) {
            String[] parts = getIdentifierParts(operand.getIdentifier());
            if (parts.length > 1 && isValidJSONColumn(parts[0], schema)) {
              Function jsonMatchFunction = new Function("JSON_MATCH");

              List<Expression> jsonMatchFunctionOperands = new ArrayList<>();
              jsonMatchFunctionOperands.add(RequestUtils.createIdentifierExpression(parts[0]));
              jsonMatchFunctionOperands.add(RequestUtils.createLiteralExpression(
                  new StringLiteralAstNode(getJsonPath(parts, true) + getOperatorSymbol(kind))));
              jsonMatchFunction.setOperands(jsonMatchFunctionOperands);

              expression.setFunctionCall(jsonMatchFunction);
            }
          }
          break;
        }
      }
    }
  }

  /**
   *  @return A string array containing all the parts of an identifier. An identifier may have one or more parts that
   *  are joined together using <DOT>. For example the identifier "testTable.jsonColumn.name.first" consists up of
   *  "testTable" (name of table), "jsonColumn" (name of column), "name" (json path), and "first" (json path). The last
   *  two parts when joined together (name.first) represent a JSON path expression.
   */
  private static String[] getIdentifierParts(Identifier identifier) {
    String fullName = identifier.getName();
    String[] parts = fullName.split("\\.");
    return parts;
  }

  /**
   * Builds a json path expression when given identifier parts. For example,given [jsonColumn, name, first], this
   * function will return "$.name.first" as json path expression.
   * @param parts identifier parts
   * @param applyDoubleQuote delimit json path with double quotes if true; otherwise, don't delimit json path.
   * @return JSON path expression associated with the given identifier parts.
   */
  private static String getJsonPath(String[] parts, boolean applyDoubleQuote) {
    StringBuilder builder = new StringBuilder();
    if (applyDoubleQuote) {
      builder.append("\"");
    }

    builder.append("$");
    for (int i = 1; i < parts.length; i++) {
      builder.append(".").append(parts[i]);
    }

    if (applyDoubleQuote) {
      builder.append("\"");
    }

    return builder.toString();
  }

  /** @return true if specified column has column datatype of JSON; otherwise, return false */
  private static boolean isValidJSONColumn(String columnName, Schema schema) {
    return schema.hasColumn(columnName) && schema.getFieldSpecFor(columnName).getDataType()
        .equals(FieldSpec.DataType.JSON);
  }

  /** @return symbolic representation of function operator delimited by spaces. */
  private static String getOperatorSymbol(FilterKind kind) {
    switch (kind) {
      case EQUALS:
        return " = ";
      case NOT_EQUALS:
        return " != ";
      case GREATER_THAN:
        return " > ";
      case GREATER_THAN_OR_EQUAL:
        return " >= ";
      case LESS_THAN:
        return " < ";
      case LESS_THAN_OR_EQUAL:
        return " <= ";
      case IN:
        return " IN ";
      case NOT_IN:
        return " NOT IN ";
      case IS_NULL:
        return " IS NULL ";
      case IS_NOT_NULL:
        return " IS NOT NULL ";
    }
    return " ";
  }

  /** @return Literal value converted into an SQL string. Note that BYTE, STRING, and BINARY values are delimited by quotes. */
  private static String getLiteralValue(Literal literal) {
    switch (literal.getSetField()) {
      case BOOL_VALUE:
        return String.valueOf(literal.getBinaryValue());
      case BYTE_VALUE:
        return "'" + String.valueOf(literal.getByteValue()) + "'";
      case SHORT_VALUE:
        return String.valueOf(literal.getShortValue());
      case INT_VALUE:
        return String.valueOf(literal.getIntValue());
      case LONG_VALUE:
        return String.valueOf(literal.getLongValue());
      case DOUBLE_VALUE:
        return String.valueOf(literal.getDoubleValue());
      case STRING_VALUE:
        return "'" + literal.getStringValue() + "'";
      case BINARY_VALUE:
        return "'" + String.valueOf(literal.getBinaryValue()) + "'";
    }

    return "";
  }

  /** List of function that require input to be in a number. */
  public static Set<String> getNumericalFunctionList() {
    Set<String> set = new HashSet<>();
    // Include all ArithmeticFunctions functions
    Method[] methods = ArithmeticFunctions.class.getDeclaredMethods();
    for (Method method : methods) {
      set.add(method.getName());
    }

    // Include all aggregation functions
    AggregationFunctionType[] aggs = AggregationFunctionType.values();
    for (AggregationFunctionType agg : aggs) {
      set.add(agg.getName());
    }

    return set;
  }
}
