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
package org.apache.pinot.materializedview.rewrite;

import java.util.List;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.segment.spi.AggregationFunctionType;


/// Classifies a [PinotQuery] into a high-level structural shape so that
/// subsumption strategies can quickly determine whether they are applicable.
///
/// `DISTINCT` is given its own value so strategies can explicitly
/// reject it rather than misclassifying it as `SCAN`. Any query shape
/// not matching `DISTINCT`, `AGGREGATION`, or `SCAN` falls
/// through to `SCAN` as the safe default (no strategy will accept it
/// unless its `acceptsShape` check passes).
///
/// **Thread-safety:** stateless enum.  Methods are pure functions of their arguments and do
/// not retain references to inputs.  Safe to invoke concurrently from the broker hot path.
public enum MaterializedViewQueryShape {

  /// No aggregation functions, no GROUP BY, no DISTINCT.
  SCAN,

  /// Contains aggregation functions in SELECT and/or a GROUP BY clause.
  AGGREGATION,

  /// `SELECT DISTINCT ...` query. CalciteSqlParser compiles this as
  /// `SELECT distinct(col1, col2, ...)` — a single function-call
  /// expression wrapping the select list.
  DISTINCT;

  /// Determines the structural shape of the given query.
  ///
  /// @param query the compiled PinotQuery to classify
  /// @return the query shape
  public static MaterializedViewQueryShape classify(PinotQuery query) {
    List<Expression> selectList = query.getSelectList();

    // DISTINCT detection: CalciteSqlParser wraps SELECT DISTINCT as a single
    // distinct(...) function call containing all projected columns.
    if (selectList != null && selectList.size() == 1) {
      Function func = selectList.get(0).getFunctionCall();
      if (func != null && "distinct".equals(func.getOperator())) {
        return DISTINCT;
      }
    }

    if (query.isSetGroupByList()) {
      return AGGREGATION;
    }

    if (selectList != null) {
      for (Expression expr : selectList) {
        if (containsAggregation(expr)) {
          return AGGREGATION;
        }
      }
    }

    return SCAN;
  }

  /// Recursively checks whether an expression contains an aggregation function call.
  /// Aliases (`as(expr, name)`) are transparently unwrapped.
  private static boolean containsAggregation(Expression expr) {
    if (expr.getType() != ExpressionType.FUNCTION) {
      return false;
    }
    Function func = expr.getFunctionCall();
    if (func == null) {
      return false;
    }
    String operator = func.getOperator();

    if ("as".equals(operator)) {
      return containsAggregation(func.getOperands().get(0));
    }

    if (AggregationFunctionType.isAggregationFunction(operator)) {
      return true;
    }

    for (Expression operand : func.getOperands()) {
      if (containsAggregation(operand)) {
        return true;
      }
    }
    return false;
  }
}
