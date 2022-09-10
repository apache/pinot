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
import java.util.Iterator;
import java.util.List;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.sql.SqlKind;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.segment.spi.AggregationFunctionType;
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

  private CalciteRexExpressionParser() {
    // do not instantiate.
  }

  // --------------------------------------------------------------------------
  // Relational conversion Utils
  // --------------------------------------------------------------------------

  public static List<Expression> overwriteSelectList(List<RexExpression> rexNodeList, PinotQuery pinotQuery) {
    return addSelectList(new ArrayList<>(), rexNodeList, pinotQuery);
  }

  public static List<Expression> addSelectList(List<Expression> existingList, List<RexExpression> rexNodeList,
      PinotQuery pinotQuery) {
    List<Expression> selectExpr = new ArrayList<>(existingList);

    final Iterator<RexExpression> iterator = rexNodeList.iterator();
    while (iterator.hasNext()) {
      final RexExpression next = iterator.next();
      selectExpr.add(toExpression(next, pinotQuery));
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

  public static List<Expression> convertOrderByList(List<RexExpression> rexInputRefs,
      List<RelFieldCollation.Direction> directions, PinotQuery pinotQuery) {
    List<Expression> orderByExpr = new ArrayList<>();

    for (int i = 0; i < rexInputRefs.size(); i++) {
      orderByExpr.add(convertOrderBy(rexInputRefs.get(i), directions.get(i), pinotQuery));
    }
    return orderByExpr;
  }

  private static Expression convertOrderBy(RexExpression rexNode, RelFieldCollation.Direction direction,
      PinotQuery pinotQuery) {
    Expression expression;
    switch (direction) {
      case DESCENDING:
        expression = RequestUtils.getFunctionExpression("DESC");
        expression.getFunctionCall().addToOperands(toExpression(rexNode, pinotQuery));
        break;
      case ASCENDING:
      default:
        expression = RequestUtils.getFunctionExpression("ASC");
        expression.getFunctionCall().addToOperands(toExpression(rexNode, pinotQuery));
        break;
    }
    return expression;
  }

  private static Expression convertDistinctAndSelectListToFunctionExpression(RexExpression.FunctionCall rexCall,
      PinotQuery pinotQuery) {
    String functionName = AggregationFunctionType.DISTINCT.getName();
    Expression functionExpression = RequestUtils.getFunctionExpression(functionName);
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
        return rexLiteralToExpression((RexExpression.Literal) rexNode);
      default:
        return compileFunctionExpression((RexExpression.FunctionCall) rexNode, pinotQuery);
    }
  }

  private static Expression rexLiteralToExpression(RexExpression.Literal rexLiteral) {
    return RequestUtils.getLiteralExpression(rexLiteral.getValue());
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
        break;
      default:
        functionName = functionKind.name();
        break;
    }
    // When there is no argument, set an empty list as the operands
    List<RexExpression> childNodes = rexCall.getFunctionOperands();
    List<Expression> operands = new ArrayList<>(childNodes.size());
    for (RexExpression childNode : childNodes) {
      operands.add(toExpression(childNode, pinotQuery));
    }
    ParserUtils.validateFunction(functionName, operands);
    Expression functionExpression = RequestUtils.getFunctionExpression(canonicalizeFunctionName(functionName));
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
    Expression andExpression = RequestUtils.getFunctionExpression(SqlKind.AND.name());
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
    Expression andExpression = RequestUtils.getFunctionExpression(SqlKind.OR.name());
    andExpression.getFunctionCall().setOperands(operands);
    return andExpression;
  }

  /**
   * Canonicalize Calcite generated Logical function names.
   */
  private static String canonicalizeFunctionName(String functionName) {
    if (functionName.endsWith("0")) {
      return functionName.substring(0, functionName.length() - 1);
    } else {
      return functionName;
    }
  }
}
