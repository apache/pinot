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
 * This rewriter rewrites ARG_MIN/ARG_MAX function, so that the functions with the same measuring expressions
 * are consolidated and added as a single function with a list of projection expressions. For example, the query
 * "SELECT ARG_MIN(col1, col2, col3), ARG_MIN(col1, col2, col4) FROM myTable" will be consolidated to a single
 * function "PARENT_ARG_MIN(0, 2, col1, col2, col3, col4)". and added to the end of the selection list.
 * While the original ARG_MIN(col1, col2, col3) and ARG_MIN(col1, col2, col4) will be rewritten to
 * CHILD_ARG_MIN(0, col3, col1, col2, col3) and CHILD_ARG_MIN(0, col4, col1, col2, col4) respectively.
 * The 2 new parameters for CHILD_ARG_MIN are the function ID (0) and the projection column (col1/col4),
 * used as column key in the parent aggregation result, during result rewriting.
 * PARENT_ARG_MIN(0, 2, col1, col2, col3, col4) means a parent aggregation function with function ID 0,
 * 2 measuring columns (col1, col2), 2 projection columns (col3, col4). The function ID is unique for each
 * consolidated function with the same function type and measuring columns.
 * Later, the aggregation, result of the consolidated function will be filled into the corresponding
 * columns of the original ARG_MIN/ARG_MAX. For more syntax details please refer to ParentAggregationFunction,
 * ChildAggregationFunction and ChildAggregationResultRewriter.
 */
public class ArgMinMaxRewriter implements QueryRewriter {

  private static final String ARG_MAX = "argmax";
  private static final String ARG_MIN = "argmin";

  private static final String ARG_MAX_PARENT =
      CommonConstants.RewriterConstants.PARENT_AGGREGATION_NAME_PREFIX + ARG_MAX;
  private static final String ARG_MIN_PARENT =
      CommonConstants.RewriterConstants.PARENT_AGGREGATION_NAME_PREFIX + ARG_MIN;

  @Override
  public PinotQuery rewrite(PinotQuery pinotQuery) {
    // This map stores the mapping from the list of measuring expressions to the set of projection expressions
    HashMap<List<Expression>, Set<Expression>> argMinFunctionMap = new HashMap<>();
    // This map stores the mapping from the list of measuring expressions to the function ID
    HashMap<List<Expression>, Integer> argMinFunctionIDMap = new HashMap<>();

    HashMap<List<Expression>, Set<Expression>> argMaxFunctionMap = new HashMap<>();
    HashMap<List<Expression>, Integer> argMaxFunctionIDMap = new HashMap<>();

    Iterator<Expression> iterator = pinotQuery.getSelectList().iterator();
    while (iterator.hasNext()) {
      boolean added = extractAndRewriteArgMinMaxFunctions(iterator.next(), argMaxFunctionMap, argMaxFunctionIDMap,
          argMinFunctionMap, argMinFunctionIDMap);
      // Remove the original function if it is not added, meaning it is a duplicate
      if (!added) {
        iterator.remove();
      }
    }

    appendParentArgMinMaxFunctions(false, pinotQuery.getSelectList(), argMinFunctionMap, argMinFunctionIDMap);
    appendParentArgMinMaxFunctions(true, pinotQuery.getSelectList(), argMaxFunctionMap, argMaxFunctionIDMap);

    return pinotQuery;
  }

  /**
   * This method appends the consolidated ARG_MIN/ARG_MAX functions to the end of the selection list.
   * The consolidated function call will be in the following format:
   * ARG_MAX(functionID, numMeasuringColumns, measuringColumn1, measuringColumn2, ... projectionColumn1,
   * projectionColumn2, ...)
   * where functionID is the ID of the consolidated function, numMeasuringColumns is the number of measuring
   * columns, measuringColumn1, measuringColumn2, ... are the measuring columns, and projectionColumn1,
   * projectionColumn2, ... are the projection columns.
   * The number of projection columns is the same as the number of ARG_MIN/ARG_MAX functions with the same
   * measuring columns.
   */
  private void appendParentArgMinMaxFunctions(boolean isMax, List<Expression> selectList,
      HashMap<List<Expression>, Set<Expression>> argMinMaxFunctionMap,
      HashMap<List<Expression>, Integer> argMinMaxFunctionIDMap) {
    for (Map.Entry<List<Expression>, Set<Expression>> entry : argMinMaxFunctionMap.entrySet()) {
      Literal functionID = new Literal();
      functionID.setLongValue(argMinMaxFunctionIDMap.get(entry.getKey()));
      Literal numMeasuringColumns = new Literal();
      numMeasuringColumns.setLongValue(entry.getKey().size());

      Function parentFunction = new Function(isMax ? ARG_MAX_PARENT : ARG_MIN_PARENT);
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
   * This method extracts the ARG_MIN/ARG_MAX functions from the given expression and rewrites the functions
   * with the same measuring expressions to use the same function ID.
   * @return true if the function is not duplicated, false otherwise.
   */
  private boolean extractAndRewriteArgMinMaxFunctions(Expression expression,
      HashMap<List<Expression>, Set<Expression>> argMaxFunctionMap,
      HashMap<List<Expression>, Integer> argMaxFunctionIDMap,
      HashMap<List<Expression>, Set<Expression>> argMinFunctionMap,
      HashMap<List<Expression>, Integer> argMinFunctionIDMap) {
    Function function = expression.getFunctionCall();
    if (function == null) {
      return true;
    }
    String functionName = function.getOperator();
    if (!(functionName.equals(ARG_MIN) || functionName.equals(ARG_MAX))) {
      return true;
    }
    List<Expression> operands = function.getOperands();
    if (operands.size() < 2) {
      throw new IllegalStateException("Invalid number of arguments for " + functionName + ", argmin/argmax should "
          + "have at least 2 arguments, got: " + operands.size());
    }
    List<Expression> argMinMaxMeasuringExpressions = new ArrayList<>();
    for (int i = 0; i < operands.size() - 1; i++) {
      argMinMaxMeasuringExpressions.add(operands.get(i));
    }
    Expression argMinMaxProjectionExpression = operands.get(operands.size() - 1);

    if (functionName.equals(ARG_MIN)) {
      return updateArgMinMaxFunctionMap(argMinMaxMeasuringExpressions, argMinMaxProjectionExpression, argMinFunctionMap,
          argMinFunctionIDMap, function);
    } else {
      return updateArgMinMaxFunctionMap(argMinMaxMeasuringExpressions, argMinMaxProjectionExpression, argMaxFunctionMap,
          argMaxFunctionIDMap, function);
    }
  }

  /**
   * This method rewrites the ARG_MIN/ARG_MAX function with the given measuring expressions to use the same
   * function ID.
   * @return true if the function is not duplicated, false otherwise.
   */
  private boolean updateArgMinMaxFunctionMap(List<Expression> argMinMaxMeasuringExpressions,
      Expression argMinMaxProjectionExpression, HashMap<List<Expression>, Set<Expression>> argMinMaxFunctionMap,
      HashMap<List<Expression>, Integer> argMinMaxFunctionIDMap, Function function) {
    int size = argMinMaxFunctionIDMap.size();
    int id = argMinMaxFunctionIDMap.computeIfAbsent(argMinMaxMeasuringExpressions, (k) -> size);

    AtomicBoolean added = new AtomicBoolean(true);

    argMinMaxFunctionMap.compute(argMinMaxMeasuringExpressions, (k, v) -> {
      if (v == null) {
        v = new HashSet<>();
      }
      added.set(v.add(argMinMaxProjectionExpression));
      return v;
    });

    String operator = function.operator;
    function.setOperator(CommonConstants.RewriterConstants.CHILD_AGGREGATION_NAME_PREFIX + operator);

    List<Expression> operands = function.getOperands();
    operands.add(0, argMinMaxProjectionExpression);
    Literal functionID = new Literal();
    functionID.setLongValue(id);
    operands.add(0, new Expression(ExpressionType.LITERAL).setLiteral(functionID));

    return added.get();
  }
}
