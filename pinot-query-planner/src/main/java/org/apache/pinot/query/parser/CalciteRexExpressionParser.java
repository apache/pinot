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
import java.util.List;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.RelFieldCollation.NullDirection;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.plannode.SortNode;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.sql.parsers.ParserUtils;


/**
 * Calcite parser to convert SQL expressions into {@link Expression}.
 *
 * <p>This class is extracted from {@link org.apache.pinot.sql.parsers.CalciteSqlParser}. It contains the logic
 * to parsed {@link org.apache.calcite.rex.RexNode}, in the format of {@link RexExpression} and convert them into
 * Thrift {@link Expression} format.
 */
public class CalciteRexExpressionParser {
  private CalciteRexExpressionParser() {
  }

  // The following function names are canonical names.
  private static final String AND = "AND";
  private static final String OR = "OR";
  private static final String FILTER = "filter";
  private static final String ASC = "asc";
  private static final String DESC = "desc";
  private static final String NULLS_FIRST = "nullsfirst";
  private static final String NULLS_LAST = "nullslast";
  private static final String COUNT = "count";
  private static final String ARRAY_TO_MV = "arraytomv";

  // --------------------------------------------------------------------------
  // Relational conversion Utils
  // --------------------------------------------------------------------------

  public static List<Expression> convertRexNodes(List<RexExpression> rexNodes, PinotQuery pinotQuery) {
    List<Expression> expressions = new ArrayList<>(rexNodes.size());
    for (RexExpression rexNode : rexNodes) {
      expressions.add(toExpression(rexNode, pinotQuery));
    }
    return expressions;
  }

  public static List<Expression> convertAggregateList(List<Expression> groupByList, List<RexExpression> aggCallList,
      List<Integer> filterArgIndices, PinotQuery pinotQuery) {
    int numAggCalls = aggCallList.size();
    List<Expression> expressions = new ArrayList<>(groupByList.size() + numAggCalls);
    expressions.addAll(groupByList);
    for (int i = 0; i < numAggCalls; i++) {
      Expression aggFunction = toExpression(aggCallList.get(i), pinotQuery);
      int filterArgIdx = filterArgIndices.get(i);
      if (filterArgIdx == -1) {
        expressions.add(aggFunction);
      } else {
        expressions.add(
            RequestUtils.getFunctionExpression(FILTER, aggFunction, pinotQuery.getSelectList().get(filterArgIdx)));
      }
    }
    return expressions;
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
    Expression expression = toExpression(rexNode, pinotQuery);
    if (direction == Direction.ASCENDING) {
      Expression asc = RequestUtils.getFunctionExpression(ASC, expression);
      // NOTE: Add explicit NULL direction only if it is not the default behavior (default behavior treats NULL as the
      //       largest value)
      return nullDirection == NullDirection.FIRST ? RequestUtils.getFunctionExpression(NULLS_FIRST, asc) : asc;
    } else {
      Expression desc = RequestUtils.getFunctionExpression(DESC, expression);
      // NOTE: Add explicit NULL direction only if it is not the default behavior (default behavior treats NULL as the
      //       largest value)
      return nullDirection == NullDirection.LAST ? RequestUtils.getFunctionExpression(NULLS_LAST, desc) : desc;
    }
  }

  public static Expression toExpression(RexExpression rexNode, PinotQuery pinotQuery) {
    if (rexNode instanceof RexExpression.InputRef) {
      return inputRefToIdentifier((RexExpression.InputRef) rexNode, pinotQuery);
    } else if (rexNode instanceof RexExpression.Literal) {
      return compileLiteralExpression(((RexExpression.Literal) rexNode).getValue());
    } else {
      assert rexNode instanceof RexExpression.FunctionCall;
      return compileFunctionExpression((RexExpression.FunctionCall) rexNode, pinotQuery);
    }
  }

  /**
   * Copy and modify from {@link RequestUtils#getLiteralExpression(Object)}.
   * TODO: Revisit whether we should use internal value type (e.g. 0/1 for BOOLEAN, ByteArray for BYTES) here.
   */
  private static Expression compileLiteralExpression(Object object) {
    if (object instanceof ByteArray) {
      return RequestUtils.getLiteralExpression(((ByteArray) object).getBytes());
    }
    return RequestUtils.getLiteralExpression(object);
  }

  private static Expression inputRefToIdentifier(RexExpression.InputRef inputRef, PinotQuery pinotQuery) {
    List<Expression> selectList = pinotQuery.getSelectList();
    return selectList.get(inputRef.getIndex());
  }

  private static Expression compileFunctionExpression(RexExpression.FunctionCall rexCall, PinotQuery pinotQuery) {
    String functionName = rexCall.getFunctionName();
    if (functionName.equals(AND)) {
      return compileAndExpression(rexCall, pinotQuery);
    }
    if (functionName.equals(OR)) {
      return compileOrExpression(rexCall, pinotQuery);
    }
    String canonicalName = RequestUtils.canonicalizeFunctionNamePreservingSpecialKey(functionName);
    List<RexExpression> childNodes = rexCall.getFunctionOperands();
    if (canonicalName.equals(COUNT) && childNodes.isEmpty()) {
      return RequestUtils.getFunctionExpression(COUNT, RequestUtils.getIdentifierExpression("*"));
    }
    if (canonicalName.equals(ARRAY_TO_MV)) {
      return toExpression(childNodes.get(0), pinotQuery);
    }
    List<Expression> operands = convertRexNodes(childNodes, pinotQuery);
    ParserUtils.validateFunction(canonicalName, operands);
    return RequestUtils.getFunctionExpression(canonicalName, operands);
  }

  /**
   * Helper method to flatten the operands for the AND expression.
   */
  private static Expression compileAndExpression(RexExpression.FunctionCall andNode, PinotQuery pinotQuery) {
    List<Expression> operands = new ArrayList<>();
    for (RexExpression childNode : andNode.getFunctionOperands()) {
      if (childNode instanceof RexExpression.FunctionCall && ((RexExpression.FunctionCall) childNode).getFunctionName()
          .equals(AND)) {
        Expression childAndExpression = compileAndExpression((RexExpression.FunctionCall) childNode, pinotQuery);
        operands.addAll(childAndExpression.getFunctionCall().getOperands());
      } else {
        operands.add(toExpression(childNode, pinotQuery));
      }
    }
    return RequestUtils.getFunctionExpression(AND, operands);
  }

  /**
   * Helper method to flatten the operands for the OR expression.
   */
  private static Expression compileOrExpression(RexExpression.FunctionCall orNode, PinotQuery pinotQuery) {
    List<Expression> operands = new ArrayList<>();
    for (RexExpression childNode : orNode.getFunctionOperands()) {
      if (childNode instanceof RexExpression.FunctionCall && ((RexExpression.FunctionCall) childNode).getFunctionName()
          .equals(OR)) {
        Expression childAndExpression = compileOrExpression((RexExpression.FunctionCall) childNode, pinotQuery);
        operands.addAll(childAndExpression.getFunctionCall().getOperands());
      } else {
        operands.add(toExpression(childNode, pinotQuery));
      }
    }
    return RequestUtils.getFunctionExpression(OR, operands);
  }
}
