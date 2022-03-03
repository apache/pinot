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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
import org.apache.pinot.common.request.FilterOperator;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.utils.request.FilterQueryTree;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.pql.parsers.pql2.ast.FilterKind;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;


/**
 * The {@code MergeRangeFilterOptimizer} merges multiple RANGE predicates on the same column joined by AND by taking
 * their intersection. It also pulls up the merged predicate in the absence of other predicates.
 *
 * NOTE: This optimizer follows the {@link FlattenAndOrFilterOptimizer}, so all the AND/OR filters are already
 *       flattened.
 */
public class MergeRangeFilterOptimizer implements FilterOptimizer {

  @Override
  public FilterQueryTree optimize(FilterQueryTree filterQueryTree, @Nullable Schema schema) {
    if (schema == null) {
      return filterQueryTree;
    }
    FilterOperator operator = filterQueryTree.getOperator();
    if (operator == FilterOperator.AND) {
      List<FilterQueryTree> children = filterQueryTree.getChildren();
      Map<String, Range> rangeMap = new HashMap<>();
      List<FilterQueryTree> newChildren = new ArrayList<>();
      boolean recreateFilter = false;

      // Iterate over all the child filters to create and merge ranges
      for (FilterQueryTree child : children) {
        FilterOperator childOperator = child.getOperator();
        assert childOperator != FilterOperator.AND;
        if (childOperator == FilterOperator.OR) {
          child.getChildren().replaceAll(c -> optimize(c, schema));
          newChildren.add(child);
        } else if (childOperator == FilterOperator.RANGE) {
          String column = child.getColumn();
          FieldSpec fieldSpec = schema.getFieldSpecFor(column);
          if (fieldSpec == null || !fieldSpec.isSingleValueField()) {
            // Skip optimizing transform expression and multi-value column
            // NOTE: We cannot optimize multi-value column because [0, 10] will match filter "col < 1 AND col > 9", but
            //       not the merged one.
            newChildren.add(child);
            continue;
          }
          // Create a range and merge with current range if exists
          Range range = Range.getRange(child.getValue().get(0), fieldSpec.getDataType());
          Range currentRange = rangeMap.get(column);
          if (currentRange == null) {
            rangeMap.put(column, range);
          } else {
            currentRange.intersect(range);
            recreateFilter = true;
          }
        } else {
          newChildren.add(child);
        }
      }

      if (recreateFilter) {
        if (newChildren.isEmpty() && rangeMap.size() == 1) {
          // Single range without other filters
          Map.Entry<String, Range> entry = rangeMap.entrySet().iterator().next();
          return getRangeFilterQueryTree(entry.getKey(), entry.getValue());
        } else {
          for (Map.Entry<String, Range> entry : rangeMap.entrySet()) {
            newChildren.add(getRangeFilterQueryTree(entry.getKey(), entry.getValue()));
          }
          return new FilterQueryTree(null, null, FilterOperator.AND, newChildren);
        }
      } else {
        return filterQueryTree;
      }
    } else if (operator == FilterOperator.OR) {
      filterQueryTree.getChildren().replaceAll(c -> optimize(c, schema));
      return filterQueryTree;
    } else {
      return filterQueryTree;
    }
  }

  /**
   * Helper method to construct a RANGE predicate FilterQueryTree from the given column and range.
   */
  private static FilterQueryTree getRangeFilterQueryTree(String column, Range range) {
    return new FilterQueryTree(column, Collections.singletonList(range.getRangeString()), FilterOperator.RANGE, null);
  }

  @Override
  public Expression optimize(Expression filterExpression, @Nullable Schema schema) {
    if (schema == null || filterExpression.getType() != ExpressionType.FUNCTION) {
      return filterExpression;
    }
    Function function = filterExpression.getFunctionCall();
    String operator = function.getOperator();
    if (operator.equals(FilterKind.AND.name())) {
      List<Expression> children = function.getOperands();
      Map<String, Range> rangeMap = new HashMap<>();
      List<Expression> newChildren = new ArrayList<>();
      boolean recreateFilter = false;

      // Iterate over all the child filters to create and merge ranges
      for (Expression child : children) {
        Function childFunction = child.getFunctionCall();
        FilterKind filterKind = FilterKind.valueOf(childFunction.getOperator());
        assert filterKind != FilterKind.AND;
        if (filterKind == FilterKind.OR) {
          childFunction.getOperands().replaceAll(o -> optimize(o, schema));
          newChildren.add(child);
        } else if (filterKind.isRange()) {
          List<Expression> operands = childFunction.getOperands();
          Expression lhs = operands.get(0);
          if (lhs.getType() != ExpressionType.IDENTIFIER) {
            // Skip optimizing transform expression
            newChildren.add(child);
            continue;
          }
          String column = lhs.getIdentifier().getName();
          FieldSpec fieldSpec = schema.getFieldSpecFor(column);
          if (fieldSpec == null || !fieldSpec.isSingleValueField()) {
            // Skip optimizing multi-value column
            // NOTE: We cannot optimize multi-value column because [0, 10] will match filter "col < 1 AND col > 9", but
            //       not the merged one.
            newChildren.add(child);
            continue;
          }
          // Create a range and merge with current range if exists
          DataType dataType = fieldSpec.getDataType();
          Range range = getRange(filterKind, operands, dataType);
          Range currentRange = rangeMap.get(column);
          if (currentRange == null) {
            rangeMap.put(column, range);
          } else {
            currentRange.intersect(range);
            recreateFilter = true;
          }
        } else {
          newChildren.add(child);
        }
      }

      if (recreateFilter) {
        if (newChildren.isEmpty() && rangeMap.size() == 1) {
          // Single range without other filters
          Map.Entry<String, Range> entry = rangeMap.entrySet().iterator().next();
          return getRangeFilterExpression(entry.getKey(), entry.getValue());
        } else {
          for (Map.Entry<String, Range> entry : rangeMap.entrySet()) {
            newChildren.add(getRangeFilterExpression(entry.getKey(), entry.getValue()));
          }
          function.setOperands(newChildren);
          return filterExpression;
        }
      } else {
        return filterExpression;
      }
    } else if (operator.equals(FilterKind.OR.name())) {
      List<Expression> children = function.getOperands();
      Map<String, Set<Range>> rangeMap = new HashMap<>();
      List<Expression> equalityExpressions = new ArrayList<>();

      List<Expression> newChildren = new ArrayList<>();
      // Iterate over all the child filters to create and merge ranges
      for (Expression child : children) {
        Function childFunction = child.getFunctionCall();
        FilterKind filterKind = FilterKind.valueOf(childFunction.getOperator());
        assert !filterKind.name().equals(operator);
        if (filterKind.isRange() || filterKind.equals(FilterKind.EQUALS)) {
          List<Expression> operands = childFunction.getOperands();
          Expression lhs = operands.get(0);
          if (lhs.getType() != ExpressionType.IDENTIFIER) {
            // Skip optimizing transform expression
            newChildren.add(child);
            continue;
          }
          String column = lhs.getIdentifier().getName();
          FieldSpec fieldSpec = schema.getFieldSpecFor(column);
          if (fieldSpec == null || !fieldSpec.isSingleValueField()) {
            // Skip optimizing multi-value column
            // NOTE: We cannot optimize multi-value column because [0, 10] will match filter "col < 1 AND col > 9", but
            //       not the merged one.
            newChildren.add(child);
            continue;
          }
          // Create a range and merge with current range if exists
          DataType dataType = fieldSpec.getDataType();
          if (filterKind.equals(FilterKind.EQUALS)) {
            equalityExpressions.add(child);
          } else {
            Range range = getRange(filterKind, operands, dataType);
            Set<Range> rangeSet = rangeMap.get(column);
            if (rangeSet == null) {
              rangeSet = new HashSet<>();
            }
            rangeSet.add(range);
            rangeMap.put(column, rangeSet);
          }
        } else {
          Expression optimizedChild = optimize(child, schema);
          newChildren.add(optimizedChild);
        }
      }
      for (Expression equalityExpression: equalityExpressions) {
        List<Expression> operands = equalityExpression.getFunctionCall().getOperands();
        Expression lhs = operands.get(0);
        String column = lhs.getIdentifier().getName();
        FieldSpec fieldSpec = schema.getFieldSpecFor(column);
        boolean isEqualityProcessed = false;
        Comparable equalityValue = getComparable(operands.get(1), fieldSpec.getDataType());
        if (rangeMap.containsKey(column)) {
          Set<Range> possibleRanges = rangeMap.get(column);
          for (Range range: possibleRanges) {
            if (range.getLowerBound() != null && range.getLowerBound().equals(equalityValue)) {
              isEqualityProcessed = true;
              range.setLowerInclusive(true);
            } else if (range.getUpperBound() != null && range.getUpperBound().equals(equalityValue)) {
              isEqualityProcessed = true;
              range.setUpperInclusive(true);
            }
          }
          if (!isEqualityProcessed) {
            newChildren.add(equalityExpression);
          }
        } else {
          newChildren.add(equalityExpression);
        }
      }
      for (Map.Entry<String, Set<Range>> entry : rangeMap.entrySet()) {
        for (Range range: entry.getValue()) {
          newChildren.add(getRangeFilterExpression(entry.getKey(), range));
        }
      }
      if (newChildren.size() == 1) {
        // Single range without other filters
        return newChildren.get(0);
      } else {
        function.setOperands(newChildren);
        return filterExpression;
      }
    } else {
      return filterExpression;
    }
  }

  /**
   * Helper method to create a Range from the given filter kind, operands and data type.
   */
  private static Range getRange(FilterKind filterKind, List<Expression> operands, DataType dataType) {
    switch (filterKind) {
      case GREATER_THAN:
        return new Range(getComparable(operands.get(1), dataType), false, null, false);
      case GREATER_THAN_OR_EQUAL:
        return new Range(getComparable(operands.get(1), dataType), true, null, false);
      case LESS_THAN:
        return new Range(null, false, getComparable(operands.get(1), dataType), false);
      case LESS_THAN_OR_EQUAL:
        return new Range(null, false, getComparable(operands.get(1), dataType), true);
      case BETWEEN:
        return new Range(getComparable(operands.get(1), dataType), true, getComparable(operands.get(2), dataType),
            true);
      case RANGE:
        return Range.getRange(operands.get(1).getLiteral().getStringValue(), dataType);
      default:
        throw new IllegalStateException("Unsupported filter kind: " + filterKind);
    }
  }

  /**
   * Helper method to create a Comparable from the given literal expression and data type.
   */
  @SuppressWarnings("rawtypes")
  private static Comparable getComparable(Expression literalExpression, DataType dataType) {
    return dataType.convertInternal(literalExpression.getLiteral().getFieldValue().toString());
  }

  /**
   * Helper method to construct a RANGE predicate filter Expression from the given column and range.
   */
  private static Expression getRangeFilterExpression(String column, Range range) {
    Expression rangeFilter = RequestUtils.getFunctionExpression(FilterKind.RANGE.name());
    rangeFilter.getFunctionCall().setOperands(Arrays.asList(RequestUtils.createIdentifierExpression(column),
        RequestUtils.getLiteralExpression(range.getRangeString())));
    return rangeFilter;
  }
}
