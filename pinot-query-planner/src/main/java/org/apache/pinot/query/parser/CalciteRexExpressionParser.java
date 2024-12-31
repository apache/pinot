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
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.RelFieldCollation.NullDirection;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.spi.utils.BooleanUtils;
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

  public static List<Expression> convertInputRefs(List<Integer> inputRefs, PinotQuery pinotQuery) {
    List<Expression> selectList = pinotQuery.getSelectList();
    List<Expression> expressions = new ArrayList<>(inputRefs.size());
    for (Integer inputRef : inputRefs) {
      expressions.add(selectList.get(inputRef));
    }
    return expressions;
  }

  public static List<Expression> convertAggregateList(List<Expression> groupByList,
      List<RexExpression.FunctionCall> aggCalls, List<Integer> filterArgs, PinotQuery pinotQuery) {
    int numAggCalls = aggCalls.size();
    List<Expression> expressions = new ArrayList<>(groupByList.size() + numAggCalls);
    expressions.addAll(groupByList);
    for (int i = 0; i < numAggCalls; i++) {
      Expression aggFunction = compileFunctionExpression(aggCalls.get(i), pinotQuery);
      int filterArgIdx = filterArgs.get(i);
      if (filterArgIdx == -1) {
        expressions.add(aggFunction);
      } else {
        expressions.add(
            RequestUtils.getFunctionExpression(FILTER, aggFunction, pinotQuery.getSelectList().get(filterArgIdx)));
      }
    }
    return expressions;
  }

  public static List<Expression> convertOrderByList(List<RelFieldCollation> collations, PinotQuery pinotQuery) {
    List<Expression> orderByExpressions = new ArrayList<>(collations.size());
    for (RelFieldCollation collation : collations) {
      orderByExpressions.add(convertOrderBy(collation, pinotQuery));
    }
    return orderByExpressions;
  }

  private static Expression convertOrderBy(RelFieldCollation collation, PinotQuery pinotQuery) {
    Expression key = pinotQuery.getSelectList().get(collation.getFieldIndex());
    if (collation.direction == Direction.ASCENDING) {
      Expression asc = RequestUtils.getFunctionExpression(ASC, key);
      // NOTE: Add explicit NULL direction only if it is not the default behavior (default behavior treats NULL as the
      //       largest value)
      return collation.nullDirection == NullDirection.FIRST ? RequestUtils.getFunctionExpression(NULLS_FIRST, asc)
          : asc;
    } else {
      Expression desc = RequestUtils.getFunctionExpression(DESC, key);
      // NOTE: Add explicit NULL direction only if it is not the default behavior (default behavior treats NULL as the
      //       largest value)
      return collation.nullDirection == NullDirection.LAST ? RequestUtils.getFunctionExpression(NULLS_LAST, desc)
          : desc;
    }
  }

  public static Expression toExpression(RexExpression rexNode, PinotQuery pinotQuery) {
    if (rexNode instanceof RexExpression.InputRef) {
      return inputRefToIdentifier((RexExpression.InputRef) rexNode, pinotQuery);
    } else if (rexNode instanceof RexExpression.Literal) {
      return RequestUtils.getLiteralExpression(toLiteral((RexExpression.Literal) rexNode));
    } else {
      assert rexNode instanceof RexExpression.FunctionCall;
      return compileFunctionExpression((RexExpression.FunctionCall) rexNode, pinotQuery);
    }
  }

  private static Expression inputRefToIdentifier(RexExpression.InputRef inputRef, PinotQuery pinotQuery) {
    List<Expression> selectList = pinotQuery.getSelectList();
    return selectList.get(inputRef.getIndex());
  }

  public static Literal toLiteral(RexExpression.Literal literal) {
    Object value = literal.getValue();
    if (value == null) {
      return RequestUtils.getNullLiteral();
    }
    // NOTE: Value is stored in internal format in RexExpression.Literal.
    //       Do not convert TIMESTAMP/BOOLEAN_ARRAY/TIMESTAMP_ARRAY to external format because they are not explicitly
    //       supported in single-stage engine Literal.
    ColumnDataType dataType = literal.getDataType();
    if (dataType == ColumnDataType.BOOLEAN) {
      value = BooleanUtils.isTrueInternalValue(value);
    } else if (dataType == ColumnDataType.BYTES) {
      value = ((ByteArray) value).getBytes();
    }
    return RequestUtils.getLiteral(value);
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
