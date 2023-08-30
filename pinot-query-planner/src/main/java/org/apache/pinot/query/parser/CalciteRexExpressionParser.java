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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.RelFieldCollation.NullDirection;
import org.apache.calcite.sql.SqlKind;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.plannode.SortNode;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.sql.FilterKind;
import org.apache.pinot.sql.parsers.SqlCompilationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Calcite parser to convert SQL expressions into {@link Expression}.
 *
 * <p>This class is extracted from {@link org.apache.pinot.sql.parsers.CalciteSqlParser}. It contains the logic
 * to parsed {@link org.apache.calcite.rex.RexNode}, in the format of {@link RexExpression} and convert them into
 * Thrift {@link Expression} format.
 */
public class CalciteRexExpressionParser {
  private static final Logger LOGGER = LoggerFactory.getLogger(CalciteRexExpressionParser.class);
  private static final Map<String, String> CANONICAL_NAME_TO_SPECIAL_KEY_MAP;
  private static final String ARRAY_TO_MV_FUNCTION_NAME = "arraytomv";

  static {
    CANONICAL_NAME_TO_SPECIAL_KEY_MAP = new HashMap<>();
    for (FilterKind filterKind : FilterKind.values()) {
      CANONICAL_NAME_TO_SPECIAL_KEY_MAP.put(canonicalizeFunctionNameInternal(filterKind.name()), filterKind.name());
    }
  }

  private CalciteRexExpressionParser() {
    // do not instantiate.
  }

  // --------------------------------------------------------------------------
  // Relational conversion Utils
  // --------------------------------------------------------------------------

  public static List<Expression> convertProjectList(List<RexExpression> projectList, PinotQuery pinotQuery) {
    List<Expression> selectExpr = new ArrayList<>();
    final Iterator<RexExpression> iterator = projectList.iterator();
    while (iterator.hasNext()) {
      final RexExpression next = iterator.next();
      selectExpr.add(toExpression(next, pinotQuery));
    }
    return selectExpr;
  }

  public static List<Expression> convertAggregateList(List<Expression> groupSetList, List<RexExpression> aggCallList,
      List<Integer> filterArgIndices, PinotQuery pinotQuery) {
    List<Expression> selectExpr = new ArrayList<>(groupSetList);

    for (int idx = 0; idx < aggCallList.size(); idx++) {
      final RexExpression aggCall = aggCallList.get(idx);
      int filterArgIdx = filterArgIndices.get(idx);
      if (filterArgIdx == -1) {
        selectExpr.add(toExpression(aggCall, pinotQuery));
      } else {
        selectExpr.add(toExpression(new RexExpression.FunctionCall(SqlKind.FILTER, aggCall.getDataType(), "FILTER",
            Arrays.asList(aggCall, new RexExpression.InputRef(filterArgIdx))), pinotQuery));
      }
    }

    return selectExpr;
  }

  public static List<Expression> convertGroupByList(List<RexExpression> rexNodeList, PinotQuery pinotQuery) {
    List<Expression> groupByExpr = new ArrayList<>();

    final Iterator<RexExpression> iterator = rexNodeList.iterator();
    while (iterator.hasNext()) {
      final RexExpression next = iterator.next();
      groupByExpr.add(toExpression(next, pinotQuery));
    }

    return groupByExpr;
  }

  private static List<Expression> convertDistinctSelectList(RexExpression.FunctionCall rexCall, PinotQuery pinotQuery) {
    List<Expression> selectExpr = new ArrayList<>();
    selectExpr.add(convertDistinctAndSelectListToFunctionExpression(rexCall, pinotQuery));
    return selectExpr;
  }

  public static List<Expression> convertOrderByList(SortNode node, PinotQuery pinotQuery) {
    List<RexExpression> collationKeys = node.getCollationKeys();
    List<Direction> collationDirections = node.getCollationDirections();
    List<NullDirection> collationNullDirections = node.getCollationNullDirections();
    int numKeys = collationKeys.size();
    List<Expression> orderByExpressions = new ArrayList<>(numKeys);
    for (int i = 0; i < numKeys; i++) {
      orderByExpressions.add(
          convertOrderBy(collationKeys.get(i), collationDirections.get(i), collationNullDirections.get(i), pinotQuery));
    }
    return orderByExpressions;
  }

  private static Expression convertOrderBy(RexExpression rexNode, Direction direction, NullDirection nullDirection,
      PinotQuery pinotQuery) {
    if (direction == Direction.ASCENDING) {
      Expression expression = getFunctionExpression("asc");
      expression.getFunctionCall().addToOperands(toExpression(rexNode, pinotQuery));
      // NOTE: Add explicit NULL direction only if it is not the default behavior (default behavior treats NULL as the
      //       largest value)
      if (nullDirection == NullDirection.FIRST) {
        Expression nullFirstExpression = getFunctionExpression("nullsfirst");
        nullFirstExpression.getFunctionCall().addToOperands(expression);
        return nullFirstExpression;
      } else {
        return expression;
      }
    } else {
      Expression expression = getFunctionExpression("desc");
      expression.getFunctionCall().addToOperands(toExpression(rexNode, pinotQuery));
      // NOTE: Add explicit NULL direction only if it is not the default behavior (default behavior treats NULL as the
      //       largest value)
      if (nullDirection == NullDirection.LAST) {
        Expression nullLastExpression = getFunctionExpression("nullslast");
        nullLastExpression.getFunctionCall().addToOperands(expression);
        return nullLastExpression;
      } else {
        return expression;
      }
    }
  }

  private static Expression convertDistinctAndSelectListToFunctionExpression(RexExpression.FunctionCall rexCall,
      PinotQuery pinotQuery) {
    Expression functionExpression = getFunctionExpression("distinct");
    for (RexExpression node : rexCall.getFunctionOperands()) {
      Expression columnExpression = toExpression(node, pinotQuery);
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

  public static Expression toExpression(RexExpression rexNode, PinotQuery pinotQuery) {
    LOGGER.debug("Current processing RexNode: {}, node.getKind(): {}", rexNode, rexNode.getKind());
    switch (rexNode.getKind()) {
      case INPUT_REF:
        return inputRefToIdentifier((RexExpression.InputRef) rexNode, pinotQuery);
      case LITERAL:
        return RequestUtils.getLiteralExpression(((RexExpression.Literal) rexNode).getValue());
      default:
        return compileFunctionExpression((RexExpression.FunctionCall) rexNode, pinotQuery);
    }
  }

  private static Expression inputRefToIdentifier(RexExpression.InputRef inputRef, PinotQuery pinotQuery) {
    List<Expression> selectList = pinotQuery.getSelectList();
    return selectList.get(inputRef.getIndex());
  }

  private static Expression compileFunctionExpression(RexExpression.FunctionCall rexCall, PinotQuery pinotQuery) {
    SqlKind functionKind = rexCall.getKind();
    String functionName;
    switch (functionKind) {
      case AND:
        return compileAndExpression(rexCall, pinotQuery);
      case OR:
        return compileOrExpression(rexCall, pinotQuery);
      case OTHER_FUNCTION:
        functionName = rexCall.getFunctionName();
        // Special handle for leaf stage multi-value columns, as the default behavior for filter and group by is not
        // sql standard, so need to use `array_to_mv` to convert the array to v1 multi-value column for behavior
        // consistency meanwhile not violating the sql standard.
        if (ARRAY_TO_MV_FUNCTION_NAME.equals(canonicalizeFunctionName(functionName))) {
          return toExpression(rexCall.getFunctionOperands().get(0), pinotQuery);
        }
        break;
      default:
        functionName = functionKind.name();
        break;
    }
    List<RexExpression> childNodes = rexCall.getFunctionOperands();
    List<Expression> operands = new ArrayList<>(childNodes.size());
    for (RexExpression childNode : childNodes) {
      operands.add(toExpression(childNode, pinotQuery));
    }
    // for COUNT, add a star (*) identifier to operand list b/c V1 doesn't handle empty operand functions.
    if (functionKind == SqlKind.COUNT && operands.isEmpty()) {
      operands.add(RequestUtils.getIdentifierExpression("*"));
    }
    ParserUtils.validateFunction(functionName, operands);
    Expression functionExpression = getFunctionExpression(functionName);
    functionExpression.getFunctionCall().setOperands(operands);
    return functionExpression;
  }

  /**
   * Helper method to flatten the operands for the AND expression.
   */
  private static Expression compileAndExpression(RexExpression.FunctionCall andNode, PinotQuery pinotQuery) {
    List<Expression> operands = new ArrayList<>();
    for (RexExpression childNode : andNode.getFunctionOperands()) {
      if (childNode.getKind() == SqlKind.AND) {
        Expression childAndExpression = compileAndExpression((RexExpression.FunctionCall) childNode, pinotQuery);
        operands.addAll(childAndExpression.getFunctionCall().getOperands());
      } else {
        operands.add(toExpression(childNode, pinotQuery));
      }
    }
    Expression andExpression = getFunctionExpression(SqlKind.AND.name());
    andExpression.getFunctionCall().setOperands(operands);
    return andExpression;
  }

  /**
   * Helper method to flatten the operands for the OR expression.
   */
  private static Expression compileOrExpression(RexExpression.FunctionCall orNode, PinotQuery pinotQuery) {
    List<Expression> operands = new ArrayList<>();
    for (RexExpression childNode : orNode.getFunctionOperands()) {
      if (childNode.getKind() == SqlKind.OR) {
        Expression childAndExpression = compileOrExpression((RexExpression.FunctionCall) childNode, pinotQuery);
        operands.addAll(childAndExpression.getFunctionCall().getOperands());
      } else {
        operands.add(toExpression(childNode, pinotQuery));
      }
    }
    Expression andExpression = getFunctionExpression(SqlKind.OR.name());
    andExpression.getFunctionCall().setOperands(operands);
    return andExpression;
  }

  private static Expression getFunctionExpression(String canonicalName) {
    Expression expression = new Expression(ExpressionType.FUNCTION);
    Function function = new Function(canonicalizeFunctionName(canonicalName));
    expression.setFunctionCall(function);
    return expression;
  }

  private static String canonicalizeFunctionName(String functionName) {
    String canonicalizeName = canonicalizeFunctionNameInternal(functionName);
    return CANONICAL_NAME_TO_SPECIAL_KEY_MAP.getOrDefault(canonicalizeName, canonicalizeName);
  }

  /**
   * Canonicalize Calcite generated Logical function names.
   */
  private static String canonicalizeFunctionNameInternal(String functionName) {
    if (functionName.endsWith("0")) {
      return functionName.substring(0, functionName.length() - 1).replace("_", "").toLowerCase();
    } else {
      return functionName.replace("_", "").toLowerCase();
    }
  }
}
