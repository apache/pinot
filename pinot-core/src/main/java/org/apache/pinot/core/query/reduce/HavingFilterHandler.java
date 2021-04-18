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
package org.apache.pinot.core.query.reduce;

import java.util.List;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluatorProvider;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * Handler for HAVING clause.
 */
public class HavingFilterHandler {
  private final PostAggregationHandler _postAggregationHandler;
  private final RowMatcher _rowMatcher;

  public HavingFilterHandler(FilterContext havingFilter, PostAggregationHandler postAggregationHandler) {
    _postAggregationHandler = postAggregationHandler;
    _rowMatcher = getRowMatcher(havingFilter);
  }

  /**
   * Returns {@code true} if the given row matches the HAVING clause, {@code false} otherwise.
   */
  public boolean isMatch(Object[] row) {
    return _rowMatcher.isMatch(row);
  }

  /**
   * Helper method to construct a RowMatcher based on the given filter.
   */
  private RowMatcher getRowMatcher(FilterContext filter) {
    switch (filter.getType()) {
      case AND:
        return new AndRowMatcher(filter.getChildren());
      case OR:
        return new OrRowMatcher(filter.getChildren());
      case PREDICATE:
        return new PredicateRowMatcher(filter.getPredicate());
      default:
        throw new IllegalStateException();
    }
  }

  /**
   * Filter matcher for the row.
   */
  private interface RowMatcher {

    /**
     * Returns {@code true} if the given row matches the filter, {@code false} otherwise.
     */
    boolean isMatch(Object[] row);
  }

  /**
   * AND filter matcher.
   */
  private class AndRowMatcher implements RowMatcher {
    RowMatcher[] _childMatchers;

    AndRowMatcher(List<FilterContext> childFilters) {
      int numChildren = childFilters.size();
      _childMatchers = new RowMatcher[numChildren];
      for (int i = 0; i < numChildren; i++) {
        _childMatchers[i] = getRowMatcher(childFilters.get(i));
      }
    }

    @Override
    public boolean isMatch(Object[] row) {
      for (RowMatcher childMatcher : _childMatchers) {
        if (!childMatcher.isMatch(row)) {
          return false;
        }
      }
      return true;
    }
  }

  /**
   * OR filter matcher.
   */
  private class OrRowMatcher implements RowMatcher {
    RowMatcher[] _childMatchers;

    OrRowMatcher(List<FilterContext> childFilters) {
      int numChildren = childFilters.size();
      _childMatchers = new RowMatcher[numChildren];
      for (int i = 0; i < numChildren; i++) {
        _childMatchers[i] = getRowMatcher(childFilters.get(i));
      }
    }

    @Override
    public boolean isMatch(Object[] row) {
      for (RowMatcher childMatcher : _childMatchers) {
        if (childMatcher.isMatch(row)) {
          return true;
        }
      }
      return false;
    }
  }

  /**
   * Predicate matcher.
   */
  private class PredicateRowMatcher implements RowMatcher {
    PostAggregationHandler.ValueExtractor _valueExtractor;
    DataType _valueType;
    PredicateEvaluator _predicateEvaluator;

    PredicateRowMatcher(Predicate predicate) {
      _valueExtractor = _postAggregationHandler.getValueExtractor(predicate.getLhs());
      _valueType = _valueExtractor.getColumnDataType().toDataType();
      _predicateEvaluator = PredicateEvaluatorProvider.getPredicateEvaluator(predicate, null, _valueType);
    }

    @Override
    public boolean isMatch(Object[] row) {
      Object value = _valueExtractor.extract(row);
      switch (_valueType) {
        case INT:
          return _predicateEvaluator.applySV((int) value);
        case LONG:
          return _predicateEvaluator.applySV((long) value);
        case FLOAT:
          return _predicateEvaluator.applySV((float) value);
        case DOUBLE:
          return _predicateEvaluator.applySV((double) value);
        case STRING:
          return _predicateEvaluator.applySV((String) value);
        case BYTES:
          return _predicateEvaluator.applySV((byte[]) value);
        default:
          throw new IllegalStateException();
      }
    }
  }
}
