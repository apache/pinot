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
package org.apache.pinot.core.query.optimizer.filter;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.sql.FilterKind;


/**
 * The {@code FlattenAndOrFilterOptimizer} flattens the nested AND/OR filters. For example, AND(a, AND(b, c)) can
 * be flattened to AND(a, b, c).
 */
public class FlattenAndOrFilterOptimizer implements FilterOptimizer {

  @Override
  public Expression optimize(Expression filterExpression, @Nullable Schema schema) {
    return optimize(filterExpression);
  }

  private Expression optimize(Expression filterExpression) {
    Function function = filterExpression.getFunctionCall();
    if (function == null) {
      return filterExpression;
    }
    String operator = function.getOperator();
    if (!operator.equals(FilterKind.AND.name()) && !operator.equals(FilterKind.OR.name())) {
      return filterExpression;
    }
    List<Expression> children = function.getOperands();
    assert children != null;
    List<Expression> newChildren = new ArrayList<>();
    for (Expression child : children) {
      Expression optimizedChild = optimize(child);
      Function childFunction = optimizedChild.getFunctionCall();
      if (childFunction != null && childFunction.getOperator().equals(operator)) {
        newChildren.addAll(childFunction.getOperands());
      } else {
        newChildren.add(optimizedChild);
      }
    }
    function.setOperands(newChildren);
    return filterExpression;
  }
}
