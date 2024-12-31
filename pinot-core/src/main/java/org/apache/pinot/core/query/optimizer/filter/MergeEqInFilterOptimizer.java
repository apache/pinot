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

import com.google.common.collect.Maps;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.sql.FilterKind;


/**
 * The {@code MergeEqualInFilterOptimizer} merges EQ and IN predicates on the same column joined by OR, and performs the
 * following optimizations:
 * <ul>
 *   <li>Merge multiple EQ and IN predicates into one IN predicate (or one EQ predicate if possible)</li>
 *   <li>De-duplicates the values in the IN predicate</li>
 *   <li>Converts single value IN predicate to EQ predicate</li>
 *   <li>Pulls up the merged predicate in the absence of other predicates</li>
 * </ul>
 *
 * NOTE: This optimizer follows the {@link FlattenAndOrFilterOptimizer}, so all the AND/OR filters are already
 *       flattened.
 */
public class MergeEqInFilterOptimizer implements FilterOptimizer {

  @Override
  public Expression optimize(Expression filterExpression, @Nullable Schema schema) {
    return filterExpression.getType() == ExpressionType.FUNCTION ? optimize(filterExpression) : filterExpression;
  }

  private Expression optimize(Expression filterExpression) {
    Function function = filterExpression.getFunctionCall();
    if (function == null) {
      return filterExpression;
    }
    String operator = function.getOperator();
    if (operator.equals(FilterKind.OR.name())) {
      List<Expression> children = function.getOperands();
      // Key is the lhs of the EQ/IN predicate, value is the map from string representation of the value to the value
      Map<Expression, Map<String, Expression>> valuesMap = new HashMap<>();
      List<Expression> newChildren = new ArrayList<>(children.size());
      boolean[] recreateFilter = new boolean[1];

      // Iterate over all the child filters to merge EQ and IN predicates
      for (Expression child : children) {
        Function childFunction = child.getFunctionCall();
        if (childFunction == null) {
          newChildren.add(child);
        } else {
          String childOperator = childFunction.getOperator();
          assert !childOperator.equals(FilterKind.OR.name());
          if (childOperator.equals(FilterKind.AND.name()) || childOperator.equals(FilterKind.NOT.name())) {
            childFunction.getOperands().replaceAll(this::optimize);
            newChildren.add(child);
          } else if (childOperator.equals(FilterKind.EQUALS.name())) {
            List<Expression> operands = childFunction.getOperands();
            Expression lhs = operands.get(0);
            Expression value = operands.get(1);
            // Use string value to de-duplicate the values to prevent the overhead of Expression.hashCode(). This is
            // consistent with how server handles predicates.
            String stringValue = RequestContextUtils.getStringValue(value);
            valuesMap.compute(lhs, (k, v) -> {
              if (v == null) {
                Map<String, Expression> values = new HashMap<>();
                values.put(stringValue, value);
                return values;
              } else {
                v.put(stringValue, value);
                // Recreate filter when multiple predicates can be merged
                recreateFilter[0] = true;
                return v;
              }
            });
          } else if (childOperator.equals(FilterKind.IN.name())) {
            List<Expression> operands = childFunction.getOperands();
            Expression lhs = operands.get(0);
            valuesMap.compute(lhs, (k, v) -> {
              if (v == null) {
                Map<String, Expression> values = getInValues(operands);
                int numUniqueValues = values.size();
                if (numUniqueValues == 1 || numUniqueValues != operands.size() - 1) {
                  // Recreate filter when the IN predicate contains only 1 value (can be rewritten to EQ predicate), or
                  // values can be de-duplicated
                  recreateFilter[0] = true;
                }
                return values;
              } else {
                int numOperands = operands.size();
                for (int i = 1; i < numOperands; i++) {
                  Expression value = operands.get(i);
                  // Use string value to de-duplicate the values to prevent the overhead of Expression.hashCode(). This
                  // is consistent with how server handles predicates.
                  String stringValue = RequestContextUtils.getStringValue(value);
                  v.put(stringValue, value);
                }
                // Recreate filter when multiple predicates can be merged
                recreateFilter[0] = true;
                return v;
              }
            });
          } else {
            newChildren.add(child);
          }
        }
      }

      if (recreateFilter[0]) {
        if (newChildren.isEmpty() && valuesMap.size() == 1) {
          // Single range without other filters
          Map.Entry<Expression, Map<String, Expression>> entry = valuesMap.entrySet().iterator().next();
          return getFilterExpression(entry.getKey(), entry.getValue().values());
        } else {
          for (Map.Entry<Expression, Map<String, Expression>> entry : valuesMap.entrySet()) {
            newChildren.add(getFilterExpression(entry.getKey(), entry.getValue().values()));
          }
          function.setOperands(newChildren);
          return filterExpression;
        }
      } else {
        return filterExpression;
      }
    } else if (operator.equals(FilterKind.AND.name())) {
      function.getOperands().replaceAll(this::optimize);
      return filterExpression;
    } else if (operator.equals(FilterKind.IN.name())) {
      List<Expression> operands = function.getOperands();
      Map<String, Expression> values = getInValues(operands);
      int numUniqueValues = values.size();
      if (numUniqueValues == 1 || numUniqueValues != operands.size() - 1) {
        // Recreate filter when the IN predicate contains only 1 value (can be rewritten to EQ predicate), or values can
        // be de-duplicated
        return getFilterExpression(operands.get(0), values.values());
      } else {
        return filterExpression;
      }
    } else {
      return filterExpression;
    }
  }

  /**
   * Helper method to get the values from the IN predicate. Returns a map from string representation of the value to the
   * value.
   */
  private Map<String, Expression> getInValues(List<Expression> operands) {
    int numOperands = operands.size();
    Map<String, Expression> values = Maps.newHashMapWithExpectedSize(numOperands - 1);
    for (int i = 1; i < numOperands; i++) {
      Expression value = operands.get(i);
      // Use string value to de-duplicate the values to prevent the overhead of Expression.hashCode(). This is
      // consistent with how server handles predicates.
      String stringValue = RequestContextUtils.getStringValue(value);
      values.put(stringValue, value);
    }
    return values;
  }

  /**
   * Helper method to construct a EQ or IN predicate filter Expression from the given lhs and values.
   */
  private static Expression getFilterExpression(Expression lhs, Collection<Expression> values) {
    int numValues = values.size();
    if (numValues == 1) {
      return RequestUtils.getFunctionExpression(FilterKind.EQUALS.name(), lhs, values.iterator().next());
    } else {
      List<Expression> operands = new ArrayList<>(numValues + 1);
      operands.add(lhs);
      operands.addAll(values);
      return RequestUtils.getFunctionExpression(FilterKind.IN.name(), operands);
    }
  }
}
