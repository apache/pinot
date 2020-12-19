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
package org.apache.pinot.core.plan;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.core.common.DataSource;
import org.apache.pinot.core.indexsegment.IndexSegment;
import org.apache.pinot.core.operator.filter.BaseFilterOperator;
import org.apache.pinot.core.operator.filter.BitmapBasedFilterOperator;
import org.apache.pinot.core.operator.filter.EmptyFilterOperator;
import org.apache.pinot.core.operator.filter.ExpressionFilterOperator;
import org.apache.pinot.core.operator.filter.FilterOperatorUtils;
import org.apache.pinot.core.operator.filter.H3IndexFilterOperator;
import org.apache.pinot.core.operator.filter.MatchAllFilterOperator;
import org.apache.pinot.core.operator.filter.TextMatchFilterOperator;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluatorProvider;
import org.apache.pinot.core.query.request.context.ExpressionContext;
import org.apache.pinot.core.query.request.context.FilterContext;
import org.apache.pinot.core.query.request.context.FunctionContext;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.predicate.GeoPredicate;
import org.apache.pinot.core.query.request.context.predicate.Predicate;
import org.apache.pinot.core.query.request.context.predicate.TextMatchPredicate;
import org.apache.pinot.core.segment.index.readers.NullValueVectorReader;
import org.apache.pinot.core.segment.index.readers.ValidDocIndexReader;
import org.apache.pinot.core.util.QueryOptions;


public class FilterPlanNode implements PlanNode {
  private final IndexSegment _indexSegment;
  private final QueryContext _queryContext;
  private final int _numDocs;

  public FilterPlanNode(IndexSegment indexSegment, QueryContext queryContext) {
    _indexSegment = indexSegment;
    _queryContext = queryContext;
    // NOTE: Fetch number of documents in the segment when creating the plan node so that it is consistent among all
    //       filter operators. Number of documents will keep increasing for MutableSegment (CONSUMING segment).
    _numDocs = _indexSegment.getSegmentMetadata().getTotalDocs();
  }

  @Override
  public BaseFilterOperator run() {
    FilterContext filter = _queryContext.getFilter();
    ValidDocIndexReader validDocIndexReader = _indexSegment.getValidDocIndex();
    boolean upsertSkipped = false;
    if (_queryContext.getQueryOptions() != null) {
      upsertSkipped = new QueryOptions(_queryContext.getQueryOptions()).isSkipUpsert();
    }
    if (filter != null) {
      BaseFilterOperator filterOperator = constructPhysicalOperator(filter, _queryContext.getDebugOptions());
      if (validDocIndexReader != null && !upsertSkipped) {
        BaseFilterOperator validDocFilter =
            new BitmapBasedFilterOperator(validDocIndexReader.getValidDocBitmap(), false, _numDocs);
        return FilterOperatorUtils.getAndFilterOperator(Arrays.asList(filterOperator, validDocFilter), _numDocs,
            _queryContext.getDebugOptions());
      } else {
        return filterOperator;
      }
    } else if (validDocIndexReader != null && !upsertSkipped) {
      return new BitmapBasedFilterOperator(validDocIndexReader.getValidDocBitmap(), false, _numDocs);
    } else {
      return new MatchAllFilterOperator(_numDocs);
    }
  }

  /**
   * Helper method to build the operator tree from the filter.
   */
  private BaseFilterOperator constructPhysicalOperator(FilterContext filter,
      @Nullable Map<String, String> debugOptions) {
    switch (filter.getType()) {
      case AND:
        List<FilterContext> childFilters = filter.getChildren();
        List<BaseFilterOperator> childFilterOperators = new ArrayList<>(childFilters.size());
        for (FilterContext childFilter : childFilters) {
          BaseFilterOperator childFilterOperator = constructPhysicalOperator(childFilter, debugOptions);
          if (childFilterOperator.isResultEmpty()) {
            // Return empty filter operator if any of the child filter operator's result is empty
            return EmptyFilterOperator.getInstance();
          } else if (!childFilterOperator.isResultMatchingAll()) {
            // Remove child filter operators that match all records
            childFilterOperators.add(childFilterOperator);
          }
        }
        return FilterOperatorUtils.getAndFilterOperator(childFilterOperators, _numDocs, debugOptions);
      case OR:
        childFilters = filter.getChildren();
        childFilterOperators = new ArrayList<>(childFilters.size());
        for (FilterContext childFilter : childFilters) {
          BaseFilterOperator childFilterOperator = constructPhysicalOperator(childFilter, debugOptions);
          if (childFilterOperator.isResultMatchingAll()) {
            // Return match all filter operator if any of the child filter operator matches all records
            return new MatchAllFilterOperator(_numDocs);
          } else if (!childFilterOperator.isResultEmpty()) {
            // Remove child filter operators whose result is empty
            childFilterOperators.add(childFilterOperator);
          }
        }
        return FilterOperatorUtils.getOrFilterOperator(childFilterOperators, _numDocs, debugOptions);
      case PREDICATE:
        Predicate predicate = filter.getPredicate();
        ExpressionContext lhs = predicate.getLhs();
        if (lhs.getType() == ExpressionContext.Type.FUNCTION) {
          FunctionContext function = lhs.getFunction();
          if (function.getFunctionName().equalsIgnoreCase("H3_WITHIN")) {
            String columnName = function.getArguments().get(0).getIdentifier();
            GeoPredicate geoPredicate = new GeoPredicate();
            //set geo predicate
            return new H3IndexFilterOperator(geoPredicate, _indexSegment.getDataSource(columnName), _numDocs);
          } else {
            // TODO: ExpressionFilterOperator does not support predicate types without PredicateEvaluator (IS_NULL,
            //       IS_NOT_NULL, TEXT_MATCH)
            return new ExpressionFilterOperator(_indexSegment, predicate, _numDocs);
          }
        } else {
          DataSource dataSource = _indexSegment.getDataSource(lhs.getIdentifier());
          switch (predicate.getType()) {
            case TEXT_MATCH:
              return new TextMatchFilterOperator(dataSource.getTextIndex(), ((TextMatchPredicate) predicate).getValue(),
                  _numDocs);
            case IS_NULL:
              NullValueVectorReader nullValueVector = dataSource.getNullValueVector();
              if (nullValueVector != null) {
                return new BitmapBasedFilterOperator(nullValueVector.getNullBitmap(), false, _numDocs);
              } else {
                return EmptyFilterOperator.getInstance();
              }
            case IS_NOT_NULL:
              nullValueVector = dataSource.getNullValueVector();
              if (nullValueVector != null) {
                return new BitmapBasedFilterOperator(nullValueVector.getNullBitmap(), true, _numDocs);
              } else {
                return new MatchAllFilterOperator(_numDocs);
              }
            default:
              PredicateEvaluator predicateEvaluator = PredicateEvaluatorProvider
                  .getPredicateEvaluator(predicate, dataSource.getDictionary(),
                      dataSource.getDataSourceMetadata().getDataType());
              return FilterOperatorUtils.getLeafFilterOperator(predicateEvaluator, dataSource, _numDocs);
          }
        }
      default:
        throw new IllegalStateException();
    }
  }
}
