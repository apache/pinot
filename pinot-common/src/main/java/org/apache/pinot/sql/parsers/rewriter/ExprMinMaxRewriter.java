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
package org.apache.pinot.sql.parsers.rewriter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.spi.utils.CommonConstants;


/**
 * This rewriter rewrites EXPR_MIN/EXPR_MAX function, so that the functions with the same measuring expressions
 * are consolidated and added as a single function with a list of projection expressions. For example, the query
 * "SELECT EXPR_MIN(col3, col1, col2), EXPR_MIN(col4, col1, col2) FROM myTable" will be consolidated to a single
 * function "PARENT_EXPR_MIN(0, 2, col1, col2, col3, col4)". and added to the end of the selection list.
 * While the original EXPR_MIN(col3, col1, col2) and EXPR_MIN(col4, col1, col2) will be rewritten to
 * CHILD_EXPR_MIN(0, col3, col3, col1, col2) and CHILD_EXPR_MIN(0, col4, col4, col1, col2) respectively.
 * The 2 new parameters for CHILD_EXPR_MIN are the function ID (0) and the projection column (col1/col4),
 * used as column key in the parent aggregation result, during result rewriting.
 * PARENT_EXPR_MIN(0, 2, col1, col2, col3, col4) means a parent aggregation function with function ID 0,
 * 2 measuring columns (col1, col2), 2 projection columns (col3, col4). The function ID is unique for each
 * consolidated function with the same function type and measuring columns.
 * Later, the aggregation, result of the consolidated function will be filled into the corresponding
 * columns of the original EXPR_MIN/EXPR_MAX. For more syntax details please refer to ParentAggregationFunction,
 * ChildAggregationFunction and ChildAggregationResultRewriter.
 */
public class ExprMinMaxRewriter implements QueryRewriter {

  private static final String EXPR_MAX = "exprmax";
  private static final String EXPR_MIN = "exprmin";

  private static final String EXPR_MAX_PARENT =
      CommonConstants.RewriterConstants.PARENT_AGGREGATION_NAME_PREFIX + EXPR_MAX;
  private static final String EXPR_MIN_PARENT =
      CommonConstants.RewriterConstants.PARENT_AGGREGATION_NAME_PREFIX + EXPR_MIN;

  @Override
  public PinotQuery rewrite(PinotQuery pinotQuery) {
    // This map stores the mapping from the list of measuring expressions to the set of projection expressions
    HashMap<List<Expression>, Set<Expression>> exprMinFunctionMap = new HashMap<>();
    // This map stores the mapping from the list of measuring expressions to the function ID
    HashMap<List<Expression>, Integer> exprMinFunctionIDMap = new HashMap<>();

    HashMap<List<Expression>, Set<Expression>> exprMaxFunctionMap = new HashMap<>();
    HashMap<List<Expression>, Integer> exprMaxFunctionIDMap = new HashMap<>();

    Iterator<Expression> iterator = pinotQuery.getSelectList().iterator();
    while (iterator.hasNext()) {
      boolean added = extractAndRewriteExprMinMaxFunctions(iterator.next(), exprMaxFunctionMap, exprMaxFunctionIDMap,
          exprMinFunctionMap, exprMinFunctionIDMap);
      // Remove the original function if it is not added, meaning it is a duplicate
      if (!added) {
        iterator.remove();
      }
    }

    appendParentExprMinMaxFunctions(false, pinotQuery.getSelectList(), exprMinFunctionMap, exprMinFunctionIDMap);
    appendParentExprMinMaxFunctions(true, pinotQuery.getSelectList(), exprMaxFunctionMap, exprMaxFunctionIDMap);

    return pinotQuery;
  }

  /**
   * This method appends the consolidated EXPR_MIN/EXPR_MAX functions to the end of the selection list.
   * The consolidated function call will be in the following format:
   * EXPR_MAX(functionID, numMeasuringColumns, measuringColumn1, measuringColumn2, ... projectionColumn1,
   * projectionColumn2, ...)
   * where functionID is the ID of the consolidated function, numMeasuringColumns is the number of measuring
   * columns, measuringColumn1, measuringColumn2, ... are the measuring columns, and projectionColumn1,
   * projectionColumn2, ... are the projection columns.
   * The number of projection columns is the same as the number of EXPR_MIN/EXPR_MAX functions with the same
   * measuring columns.
   */
  private void appendParentExprMinMaxFunctions(boolean isMax, List<Expression> selectList,
      HashMap<List<Expression>, Set<Expression>> exprMinMaxFunctionMap,
      HashMap<List<Expression>, Integer> exprMinMaxFunctionIDMap) {
    for (Map.Entry<List<Expression>, Set<Expression>> entry : exprMinMaxFunctionMap.entrySet()) {
      Literal functionID = new Literal();
      functionID.setLongValue(exprMinMaxFunctionIDMap.get(entry.getKey()));
      Literal numMeasuringColumns = new Literal();
      numMeasuringColumns.setLongValue(entry.getKey().size());

      Function parentFunction = new Function(isMax ? EXPR_MAX_PARENT : EXPR_MIN_PARENT);
      parentFunction.addToOperands(new Expression(ExpressionType.LITERAL).setLiteral(functionID));
      parentFunction.addToOperands(new Expression(ExpressionType.LITERAL).setLiteral(numMeasuringColumns));
      for (Expression expression : entry.getKey()) {
        parentFunction.addToOperands(expression);
      }
      for (Expression expression : entry.getValue()) {
        parentFunction.addToOperands(expression);
      }
      selectList.add(new Expression(ExpressionType.FUNCTION).setFunctionCall(parentFunction));
    }
  }

  /**
   * This method extracts the EXPR_MIN/EXPR_MAX functions from the given expression and rewrites the functions
   * with the same measuring expressions to use the same function ID.
   * @return true if the function is not duplicated, false otherwise.
   */
  private boolean extractAndRewriteExprMinMaxFunctions(Expression expression,
      HashMap<List<Expression>, Set<Expression>> exprMaxFunctionMap,
      HashMap<List<Expression>, Integer> exprMaxFunctionIDMap,
      HashMap<List<Expression>, Set<Expression>> exprMinFunctionMap,
      HashMap<List<Expression>, Integer> exprMinFunctionIDMap) {
    Function function = expression.getFunctionCall();
    if (function == null) {
      return true;
    }
    String functionName = function.getOperator();
    if (!(functionName.equals(EXPR_MIN) || functionName.equals(EXPR_MAX))) {
      return true;
    }
    List<Expression> operands = function.getOperands();
    if (operands.size() < 2) {
      throw new IllegalStateException("Invalid number of arguments for " + functionName + ", exprmin/exprmax should "
          + "have at least 2 arguments, got: " + operands.size());
    }
    List<Expression> exprMinMaxMeasuringExpressions = new ArrayList<>();
    for (int i = 1; i < operands.size(); i++) {
      exprMinMaxMeasuringExpressions.add(operands.get(i));
    }
    Expression exprMinMaxProjectionExpression = operands.get(0);

    if (functionName.equals(EXPR_MIN)) {
      return updateExprMinMaxFunctionMap(exprMinMaxMeasuringExpressions, exprMinMaxProjectionExpression,
          exprMinFunctionMap, exprMinFunctionIDMap, function);
    } else {
      return updateExprMinMaxFunctionMap(exprMinMaxMeasuringExpressions, exprMinMaxProjectionExpression,
          exprMaxFunctionMap, exprMaxFunctionIDMap, function);
    }
  }

  /**
   * This method rewrites the EXPR_MIN/EXPR_MAX function with the given measuring expressions to use the same
   * function ID.
   * @return true if the function is not duplicated, false otherwise.
   */
  private boolean updateExprMinMaxFunctionMap(List<Expression> exprMinMaxMeasuringExpressions,
      Expression exprMinMaxProjectionExpression, HashMap<List<Expression>, Set<Expression>> exprMinMaxFunctionMap,
      HashMap<List<Expression>, Integer> exprMinMaxFunctionIDMap, Function function) {
    int size = exprMinMaxFunctionIDMap.size();
    int id = exprMinMaxFunctionIDMap.computeIfAbsent(exprMinMaxMeasuringExpressions, (k) -> size);

    AtomicBoolean added = new AtomicBoolean(true);

    exprMinMaxFunctionMap.compute(exprMinMaxMeasuringExpressions, (k, v) -> {
      if (v == null) {
        v = new HashSet<>();
      }
      added.set(v.add(exprMinMaxProjectionExpression));
      return v;
    });

    String operator = function.operator;
    function.setOperator(CommonConstants.RewriterConstants.CHILD_AGGREGATION_NAME_PREFIX + operator);

    List<Expression> operands = function.getOperands();
    operands.add(0, exprMinMaxProjectionExpression);
    Literal functionID = new Literal();
    functionID.setLongValue(id);
    operands.add(0, new Expression(ExpressionType.LITERAL).setLiteral(functionID));

    return added.get();
  }
}
