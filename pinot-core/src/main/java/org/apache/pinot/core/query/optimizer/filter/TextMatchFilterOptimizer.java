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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.sql.FilterKind;


/**
 * The {@code TextMatchFilterOptimizer} merges `TEXT_MATCH` predicates on the same column within an `OR` or `AND`,
 * maximizing the amount of the query that can be pushed down to Lucene
 *
 * NOTE: This optimizer follows the {@link FlattenAndOrFilterOptimizer}, so all the AND/OR filters are already
 *       flattened.
 */
public class TextMatchFilterOptimizer implements FilterOptimizer {
  private static final String SPACE = " ";

  @Override
  public Expression optimize(Expression filterExpression, @Nullable Schema schema) {
    return filterExpression.getType() == ExpressionType.FUNCTION ? optimize(filterExpression) : filterExpression;
  }

  private Expression optimize(Expression filterExpression) {
    Function function = filterExpression.getFunctionCall();
    if (function == null) {
      return filterExpression;
    }

    // no optimization can be performed unless the function is an OR, AND, or NOT
    String operator = function.getOperator();
    if (!operator.equals(FilterKind.OR.name()) && !operator.equals(FilterKind.AND.name()) && !operator.equals(
        FilterKind.NOT.name())) {
      return filterExpression;
    }

    List<Expression> children = function.getOperands();
    children.replaceAll(this::optimize);

    List<Expression> newChildren = new ArrayList<>();
    Map<Expression, List<Expression>> textMatchMap = new HashMap<>();
    boolean recreateFilter = false;

    // iterate over all child expressions to collect TEXT_MATCH filters for each identifier
    for (Expression child : children) {
      Function childFunction = child.getFunctionCall();
      if (childFunction == null) {
        newChildren.add(child);
      } else {
        String childOperator = childFunction.getOperator();
        if (childOperator.equals(FilterKind.TEXT_MATCH.name())) {
          List<Expression> operands = childFunction.getOperands();
          Expression identifier = operands.get(0);
          textMatchMap.computeIfAbsent(identifier, k -> new ArrayList<>()).add(child);
        } else if (childOperator.equals(FilterKind.NOT.name())) {
          assert childFunction.getOperands().size() == 1;
          Expression operand = childFunction.getOperands().get(0);
          Function notChildFunction = operand.getFunctionCall();
          if (notChildFunction == null) {
            newChildren.add(child);
            continue;
          }
          if (notChildFunction.getOperator().equals(FilterKind.TEXT_MATCH.name())) {
            List<Expression> operands = notChildFunction.getOperands();
            Expression identifier = operands.get(0);
            textMatchMap.computeIfAbsent(identifier, k -> new ArrayList<>()).add(child);
            continue;
          }
          newChildren.add(child);
        } else {
          Expression newChild = optimize(child);
          if (!newChild.equals(child)) {
            recreateFilter = true;
          }
          newChildren.add(optimize(child));
        }
      }
    }

    for (List<Expression> values : textMatchMap.values()) {
      if (values.size() > 1) {
        recreateFilter = true;
        break;
      }
    }
    if (recreateFilter) {
      return getNewFilter(operator, newChildren, textMatchMap);
    }
    return filterExpression;
  }

  private Expression getNewFilter(String operator, List<Expression> newChildren,
      Map<Expression, List<Expression>> textMatchMap) {
    // for each key in textMatchMap, build a TEXT_MATCH expression (merge list of filters)
    for (Map.Entry<Expression, List<Expression>> entry : textMatchMap.entrySet()) {
      // special case: if all expressions are NOT, then wrap the merged text match inside a NOT. otherwise, push the
      // NOT down into the text match expression
      boolean allNot = true;
      for (Expression expression : entry.getValue()) {
        if (!expression.getFunctionCall().getOperator().equals(FilterKind.NOT.name())) {
          allNot = false;
          break;
        }
      }

      List<String> literals = new ArrayList<>();
      if (allNot) {
        for (Expression expression : entry.getValue()) {
          Expression operand = expression.getFunctionCall().getOperands().get(0);
          literals.add(operand.getFunctionCall().getOperands().get(1).getLiteral().getStringValue());
        }
      } else {
        for (Expression expression : entry.getValue()) {
          if (expression.getFunctionCall().getOperator().equals(FilterKind.NOT.name())) {
            Expression operand = expression.getFunctionCall().getOperands().get(0);
            literals.add(FilterKind.NOT.name() + SPACE + operand.getFunctionCall().getOperands().get(1).getLiteral()
                .getStringValue());
            continue;
          }
          assert expression.getFunctionCall().getOperator().equals(FilterKind.TEXT_MATCH.name());
          literals.add(expression.getFunctionCall().getOperands().get(1).getLiteral().getStringValue());
        }
      }

      // build the merged TEXT_MATCH expression
      String mergedTextMatchFilter;
      if (allNot) {
        assert operator.equals(FilterKind.AND.name()) || operator.equals(FilterKind.OR.name());
        if (operator.equals(FilterKind.AND.name())) {
          mergedTextMatchFilter = String.join(SPACE + FilterKind.OR.name() + SPACE, literals);
        } else {
          mergedTextMatchFilter = String.join(SPACE + FilterKind.AND.name() + SPACE, literals);
        }
      } else {
        mergedTextMatchFilter = String.join(SPACE + operator + SPACE, literals);
      }
      Expression mergedTextMatchExpression = RequestUtils.getFunctionExpression(FilterKind.TEXT_MATCH.name());
      Expression mergedTextMatchFilterExpression = RequestUtils.getLiteralExpression(mergedTextMatchFilter);
      mergedTextMatchExpression.getFunctionCall()
          .setOperands(Arrays.asList(entry.getKey(), mergedTextMatchFilterExpression));

      if (allNot) {
        Expression notExpression = RequestUtils.getFunctionExpression(FilterKind.NOT.name());
        notExpression.getFunctionCall().setOperands(Collections.singletonList(mergedTextMatchExpression));
        newChildren.add(notExpression);
        continue;
      }
      newChildren.add(mergedTextMatchExpression);
    }

    if (newChildren.size() == 1) {
      return newChildren.get(0);
    }
    assert operator.equals(FilterKind.OR.name()) || operator.equals(FilterKind.AND.name());
    Expression newExpression = RequestUtils.getFunctionExpression(operator);
    newExpression.getFunctionCall().setOperands(newChildren);
    return newExpression;
  }
}
