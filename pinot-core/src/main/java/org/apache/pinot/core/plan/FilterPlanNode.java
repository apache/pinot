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

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.request.context.predicate.JsonMatchPredicate;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.common.request.context.predicate.RegexpLikePredicate;
import org.apache.pinot.common.request.context.predicate.TextMatchPredicate;
import org.apache.pinot.core.geospatial.transform.function.StDistanceFunction;
import org.apache.pinot.core.operator.filter.BaseFilterOperator;
import org.apache.pinot.core.operator.filter.BitmapBasedFilterOperator;
import org.apache.pinot.core.operator.filter.EmptyFilterOperator;
import org.apache.pinot.core.operator.filter.ExpressionFilterOperator;
import org.apache.pinot.core.operator.filter.FilterOperatorUtils;
import org.apache.pinot.core.operator.filter.H3IndexFilterOperator;
import org.apache.pinot.core.operator.filter.JsonMatchFilterOperator;
import org.apache.pinot.core.operator.filter.MatchAllFilterOperator;
import org.apache.pinot.core.operator.filter.TextMatchFilterOperator;
import org.apache.pinot.core.operator.filter.predicate.FSTBasedRegexpPredicateEvaluatorFactory;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluatorProvider;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.util.QueryOptionsUtils;
import org.apache.pinot.segment.local.segment.index.datasource.MutableDataSource;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.segment.spi.index.reader.JsonIndexReader;
import org.apache.pinot.segment.spi.index.reader.NullValueVectorReader;
import org.apache.pinot.spi.exception.BadQueryRequestException;


public class FilterPlanNode implements PlanNode {
  private final IndexSegment _indexSegment;
  private final QueryContext _queryContext;
  private final int _numDocs;

  private BaseFilterOperator _filterOperator;

  public FilterPlanNode(IndexSegment indexSegment, QueryContext queryContext) {
    _indexSegment = indexSegment;
    _queryContext = queryContext;
    // NOTE: Fetch number of documents in the segment when creating the plan node so that it is consistent among all
    //       filter operators. Number of documents will keep increasing for MutableSegment (CONSUMING segment).
    _numDocs = _indexSegment.getSegmentMetadata().getTotalDocs();
  }

  public FilterPlanNode(IndexSegment indexSegment, QueryContext queryContext, BaseFilterOperator filterOperator) {
    this(indexSegment, queryContext);

    _filterOperator = filterOperator;
  }

  @Override
  public BaseFilterOperator run() {
    FilterContext filter = _queryContext.getFilter();
    ThreadSafeMutableRoaringBitmap validDocIds = _indexSegment.getValidDocIds();
    boolean applyValidDocIds = validDocIds != null && !QueryOptionsUtils.isSkipUpsert(_queryContext.getQueryOptions());
    if (filter != null) {
      if (_filterOperator == null) {
        _filterOperator = constructPhysicalOperator(filter, _indexSegment, _numDocs, _queryContext.getDebugOptions());
      }
      if (applyValidDocIds) {
        BaseFilterOperator validDocFilter =
            new BitmapBasedFilterOperator(validDocIds.getMutableRoaringBitmap(), false, _numDocs);
        return FilterOperatorUtils.getAndFilterOperator(Arrays.asList(_filterOperator, validDocFilter), _numDocs,
            _queryContext.getDebugOptions());
      } else {
        return _filterOperator;
      }
    } else if (applyValidDocIds) {
      return new BitmapBasedFilterOperator(validDocIds.getMutableRoaringBitmap(), false, _numDocs);
    } else {
      return new MatchAllFilterOperator(_numDocs);
    }
  }

  /**
   * H3 index can be applied iff:
   * <ul>
   *   <li>Predicate is of type RANGE</li>
   *   <li>Left-hand-side of the predicate is an ST_Distance function</li>
   *   <li>One argument of the ST_Distance function is an identifier, the other argument is an literal</li>
   *   <li>The identifier column has H3 index</li>
   * </ul>
   */
  private static boolean canApplyH3Index(IndexSegment indexSegment, Predicate predicate, FunctionContext function) {
    if (predicate.getType() != Predicate.Type.RANGE) {
      return false;
    }
    if (!function.getFunctionName().equalsIgnoreCase(StDistanceFunction.FUNCTION_NAME)) {
      return false;
    }
    List<ExpressionContext> arguments = function.getArguments();
    if (arguments.size() != 2) {
      throw new BadQueryRequestException("Expect 2 arguments for function: " + StDistanceFunction.FUNCTION_NAME);
    }
    // TODO: handle nested geography/geometry conversion functions
    String columnName = null;
    boolean findLiteral = false;
    for (ExpressionContext argument : arguments) {
      if (argument.getType() == ExpressionContext.Type.IDENTIFIER) {
        columnName = argument.getIdentifier();
      } else if (argument.getType() == ExpressionContext.Type.LITERAL) {
        findLiteral = true;
      }
    }
    return columnName != null && indexSegment.getDataSource(columnName).getH3Index() != null && findLiteral;
  }

  /**
   * Helper method to build the operator tree from the filter.
   */
  public static BaseFilterOperator constructPhysicalOperator(FilterContext filter, IndexSegment indexSegment, int numDocs,
      @Nullable Map<String, String> debugOptions) {
    switch (filter.getType()) {
      case AND:
        List<FilterContext> childFilters = filter.getChildren();
        List<BaseFilterOperator> childFilterOperators = new ArrayList<>(childFilters.size());
        for (FilterContext childFilter : childFilters) {
          BaseFilterOperator childFilterOperator = constructPhysicalOperator(childFilter, indexSegment, numDocs, debugOptions);
          if (childFilterOperator.isResultEmpty()) {
            // Return empty filter operator if any of the child filter operator's result is empty
            return EmptyFilterOperator.getInstance();
          } else if (!childFilterOperator.isResultMatchingAll()) {
            // Remove child filter operators that match all records
            childFilterOperators.add(childFilterOperator);
          }
        }
        return FilterOperatorUtils.getAndFilterOperator(childFilterOperators, numDocs, debugOptions);
      case OR:
        childFilters = filter.getChildren();
        childFilterOperators = new ArrayList<>(childFilters.size());
        for (FilterContext childFilter : childFilters) {
          BaseFilterOperator childFilterOperator = constructPhysicalOperator(childFilter, indexSegment, numDocs, debugOptions);
          if (childFilterOperator.isResultMatchingAll()) {
            // Return match all filter operator if any of the child filter operator matches all records
            return new MatchAllFilterOperator(numDocs);
          } else if (!childFilterOperator.isResultEmpty()) {
            // Remove child filter operators whose result is empty
            childFilterOperators.add(childFilterOperator);
          }
        }
        return FilterOperatorUtils.getOrFilterOperator(childFilterOperators, numDocs, debugOptions);
      case PREDICATE:
        Predicate predicate = filter.getPredicate();
        ExpressionContext lhs = predicate.getLhs();
        if (lhs.getType() == ExpressionContext.Type.FUNCTION) {
          if (canApplyH3Index(indexSegment, predicate, lhs.getFunction())) {
            return new H3IndexFilterOperator(indexSegment, predicate, numDocs);
          }
          // TODO: ExpressionFilterOperator does not support predicate types without PredicateEvaluator (IS_NULL,
          //       IS_NOT_NULL, TEXT_MATCH)
          return new ExpressionFilterOperator(indexSegment, predicate, numDocs);
        } else {
          String column = lhs.getIdentifier();
          DataSource dataSource = indexSegment.getDataSource(column);
          switch (predicate.getType()) {
            case TEXT_MATCH:
              return new TextMatchFilterOperator(dataSource.getTextIndex(), ((TextMatchPredicate) predicate).getValue(),
                  numDocs);
            case REGEXP_LIKE:
              // FST Index is available only for rolled out segments. So, we use different evaluator for rolled out and
              // consuming segments.
              //
              // Rolled out segments (immutable): FST Index reader is available use FSTBasedEvaluator
              // else use regular flow of getting predicate evaluator.
              //
              // Consuming segments: When FST is enabled, use AutomatonBasedEvaluator so that regexp matching logic is
              // similar to that of FSTBasedEvaluator, else use regular flow of getting predicate evaluator.
              PredicateEvaluator evaluator;
              if (dataSource.getFSTIndex() != null) {
                evaluator = FSTBasedRegexpPredicateEvaluatorFactory.newFSTBasedEvaluator(dataSource.getFSTIndex(),
                    dataSource.getDictionary(), ((RegexpLikePredicate) predicate).getValue());
              } else if (dataSource instanceof MutableDataSource && ((MutableDataSource) dataSource).isFSTEnabled()) {
                evaluator =
                    FSTBasedRegexpPredicateEvaluatorFactory.newAutomatonBasedEvaluator(dataSource.getDictionary(),
                        ((RegexpLikePredicate) predicate).getValue());
              } else {
                evaluator = PredicateEvaluatorProvider.getPredicateEvaluator(predicate, dataSource.getDictionary(),
                    dataSource.getDataSourceMetadata().getDataType());
              }
              return FilterOperatorUtils.getLeafFilterOperator(evaluator, dataSource, numDocs);
            case JSON_MATCH:
              JsonIndexReader jsonIndex = dataSource.getJsonIndex();

              Preconditions
                  .checkState(jsonIndex != null, "Cannot apply JSON_MATCH on column: %s without json index", column);
              return new JsonMatchFilterOperator(jsonIndex, ((JsonMatchPredicate) predicate).getValue(), numDocs);
            case IS_NULL:
              NullValueVectorReader nullValueVector = dataSource.getNullValueVector();
              if (nullValueVector != null) {
                return new BitmapBasedFilterOperator(nullValueVector.getNullBitmap(), false, numDocs);
              } else {
                return EmptyFilterOperator.getInstance();
              }
            case IS_NOT_NULL:
              nullValueVector = dataSource.getNullValueVector();
              if (nullValueVector != null) {
                return new BitmapBasedFilterOperator(nullValueVector.getNullBitmap(), true, numDocs);
              } else {
                return new MatchAllFilterOperator(numDocs);
              }
            default:
              PredicateEvaluator predicateEvaluator =
                  PredicateEvaluatorProvider.getPredicateEvaluator(predicate, dataSource.getDictionary(),
                      dataSource.getDataSourceMetadata().getDataType());
              return FilterOperatorUtils.getLeafFilterOperator(predicateEvaluator, dataSource, numDocs);
          }
        }
      default:
        throw new IllegalStateException();
    }
  }
}
