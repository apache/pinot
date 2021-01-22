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
package org.apache.pinot.parsers.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.FilterOperator;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.pql.parsers.pql2.ast.FilterKind;


/**
 * Class for holding Parser specific utility functions.
 */
public class ParserUtils {
  // Private constructor to disable instantiation.
  private ParserUtils() {

  }

  private static final Map<FilterKind, FilterOperator> FILTER_OPERATOR_MAP;

  static {
    FILTER_OPERATOR_MAP = new HashMap<>();
    FILTER_OPERATOR_MAP.put(FilterKind.AND, FilterOperator.AND);
    FILTER_OPERATOR_MAP.put(FilterKind.OR, FilterOperator.OR);
    FILTER_OPERATOR_MAP.put(FilterKind.EQUALS, FilterOperator.EQUALITY);
    FILTER_OPERATOR_MAP.put(FilterKind.NOT_EQUALS, FilterOperator.NOT);
    FILTER_OPERATOR_MAP.put(FilterKind.GREATER_THAN, FilterOperator.RANGE);
    FILTER_OPERATOR_MAP.put(FilterKind.LESS_THAN, FilterOperator.RANGE);
    FILTER_OPERATOR_MAP.put(FilterKind.GREATER_THAN_OR_EQUAL, FilterOperator.RANGE);
    FILTER_OPERATOR_MAP.put(FilterKind.LESS_THAN_OR_EQUAL, FilterOperator.RANGE);
    FILTER_OPERATOR_MAP.put(FilterKind.BETWEEN, FilterOperator.RANGE);
    FILTER_OPERATOR_MAP.put(FilterKind.IN, FilterOperator.IN);
    FILTER_OPERATOR_MAP.put(FilterKind.NOT_IN, FilterOperator.NOT_IN);
    FILTER_OPERATOR_MAP.put(FilterKind.REGEXP_LIKE, FilterOperator.REGEXP_LIKE);
    FILTER_OPERATOR_MAP.put(FilterKind.IS_NULL, FilterOperator.IS_NULL);
    FILTER_OPERATOR_MAP.put(FilterKind.IS_NOT_NULL, FilterOperator.IS_NOT_NULL);
    FILTER_OPERATOR_MAP.put(FilterKind.TEXT_MATCH, FilterOperator.TEXT_MATCH);
    FILTER_OPERATOR_MAP.put(FilterKind.JSON_MATCH, FilterOperator.JSON_MATCH);
  }

  /**
   * Utility method that returns the {@link FilterOperator} for a given expression.
   * Assumes that the passed in expression is a filter expression.
   *
   * @param expression Expression for which to get the filter type
   * @return Filter Operator for the given Expression.
   */
  public static FilterOperator getFilterType(Expression expression) {
    String operator = expression.getFunctionCall().getOperator();
    return filterKindToOperator(FilterKind.valueOf(operator));
  }

  /**
   * Utility method that returns the {@link FilterKind} for a given expression.
   * Assumes that the passed in expression is a filter expression.
   *
   * @param expression Expression for which to get the filter type
   * @return Filter Kind for the given Expression.
   */
  public static FilterKind getFilterKind(Expression expression) {
    String operator = expression.getFunctionCall().getOperator();
    return FilterKind.valueOf(operator);
  }

  /**
   * Utility method to map {@link FilterKind} to {@link FilterOperator}.
   *
   * @param filterKind Filter kind for which to get the Filter Operator
   * @return Filter Operator for the given Filter kind
   */
  public static FilterOperator filterKindToOperator(FilterKind filterKind) {
    return FILTER_OPERATOR_MAP.get(filterKind);
  }

  /**
   * Returns the filter column from a given expression.
   * Assumes that the passed expression is a filter expression of form LHS Operator RHS.
   * LHS is expected to be a single column name.
   *
   * @param expression Filter expression
   * @return
   */
  public static String getFilterColumn(Expression expression) {
    Function functionCall = expression.getFunctionCall();
    Expression operand = functionCall.getOperands().get(0);
    return standardizeExpression(operand, false);
  }

  /**
   * Utility method that returns the RHS of a filter expression as a list of String's.
   *
   * @param filterExpression Filter expression
   * @return RHS of the filter expression
   */
  public static List<String> getFilterValues(Expression filterExpression) {
    Function function = filterExpression.getFunctionCall();
    FilterKind filterKind = FilterKind.valueOf(function.getOperator());

    return getFilterValues(filterKind, function.getOperands());
  }

  /**
   * Given a expression filter, returns the values (RHS) on which the filter predicate applies.
   *
   * @param filterKind Kind of filter
   * @param operands Filter operands
   * @return Values to filter on, as a list of strings.
   */
  public static List<String> getFilterValues(FilterKind filterKind, List<Expression> operands) {
    List<String> valueList = new ArrayList<>();

    // For non-range expressions, RHS is just the list of values from first index.
    if (!filterKind.isRange()) {
      for (int i = 1; i < operands.size(); i++) {
        valueList.add(standardizeExpression(operands.get(i), true));
      }
      return valueList;
    }

    String rangeExpression;
    //PQL does not quote the string literals when we create expression
    boolean treatLiteralAsIdentifier = true;

    if (FilterKind.LESS_THAN == filterKind) {

      String value = standardizeExpression(operands.get(1), treatLiteralAsIdentifier);
      rangeExpression = "(*\t\t" + value + ")";
    } else if (FilterKind.LESS_THAN_OR_EQUAL == filterKind) {

      String value = standardizeExpression(operands.get(1), treatLiteralAsIdentifier);
      rangeExpression = "(*\t\t" + value + "]";
    } else if (FilterKind.GREATER_THAN == filterKind) {

      String value = standardizeExpression(operands.get(1), treatLiteralAsIdentifier);
      rangeExpression = "(" + value + "\t\t*)";
    } else if (FilterKind.GREATER_THAN_OR_EQUAL == filterKind) {

      String value = standardizeExpression(operands.get(1), treatLiteralAsIdentifier);
      rangeExpression = "[" + value + "\t\t*)";
    } else if (FilterKind.BETWEEN == filterKind) {

      String left = standardizeExpression(operands.get(1), treatLiteralAsIdentifier);
      String right = standardizeExpression(operands.get(2), treatLiteralAsIdentifier);
      rangeExpression = "[" + left + "\t\t" + right + "]";
    } else {
      throw new UnsupportedOperationException("Unknown Filter Kind:" + filterKind);
    }

    valueList.add(rangeExpression);
    return valueList;
  }

  /**
   * Standardizes a given expression, by quoting String literals.
   *
   * @param expression Expression to standardize
   * @param treatLiteralAsIdentifier If true, literals are not quoted
   * @return Standardized expression
   */
  public static String standardizeExpression(Expression expression, boolean treatLiteralAsIdentifier) {
    return standardizeExpression(expression, treatLiteralAsIdentifier, false);
  }

  public static String standardizeExpression(Expression expression, boolean treatLiteralAsIdentifier,
      boolean forceSingleQuoteOnNonStringLiteral) {
    switch (expression.getType()) {
      case LITERAL:
        Literal literal = expression.getLiteral();
        String literalString = literal.getFieldValue().toString();
        if (literal.isSetStringValue()) {
          if (treatLiteralAsIdentifier) {
            return literalString;
          } else {
            return "'" + StringUtils.replace(literalString, "'", "''") + "'";
          }
        } else {
          if (forceSingleQuoteOnNonStringLiteral) {
            return "'" + literalString + "'";
          } else {
            return literalString;
          }
        }
      case IDENTIFIER:
        return expression.getIdentifier().getName();
      case FUNCTION:
        Function functionCall = expression.getFunctionCall();
        return standardizeFunction(functionCall);
      default:
        throw new UnsupportedOperationException("Unknown Expression type: " + expression.getType());
    }
  }

  /**
   * Standardizes a function call by quoting literal operands.
   *
   * @param functionCall Function call to standardize
   * @return Standardized String equivalent of the Function.
   */
  public static String standardizeFunction(Function functionCall) {
    StringBuilder sb = new StringBuilder();
    sb.append(functionCall.getOperator().toLowerCase());
    sb.append("(");
    String delim = "";
    for (Expression operand : functionCall.getOperands()) {
      sb.append(delim);
      sb.append(standardizeExpression(operand, false, true));
      delim = ",";
    }
    sb.append(")");
    return sb.toString();
  }
}
