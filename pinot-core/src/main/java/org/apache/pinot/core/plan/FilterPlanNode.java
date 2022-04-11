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
import java.util.HashMap;
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
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.segment.spi.index.reader.JsonIndexReader;
import org.apache.pinot.segment.spi.index.reader.NullValueVectorReader;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


public class FilterPlanNode implements PlanNode {
  private final IndexSegment _indexSegment;
  private final QueryContext _queryContext;
  private final FilterContext _filter;

  // Cache the predicate evaluators
  private final Map<Predicate, PredicateEvaluator> _predicateEvaluatorMap = new HashMap<>();

  public FilterPlanNode(IndexSegment indexSegment, QueryContext queryContext) {
    this(indexSegment, queryContext, null);
  }

  public FilterPlanNode(IndexSegment indexSegment, QueryContext queryContext, @Nullable FilterContext filter) {
    _indexSegment = indexSegment;
    _queryContext = queryContext;
    _filter = filter;
  }

  @Override
  public BaseFilterOperator run() {
    // NOTE: Snapshot the validDocIds before reading the numDocs to prevent the latest updates getting lost
    ThreadSafeMutableRoaringBitmap validDocIds = _indexSegment.getValidDocIds();
    MutableRoaringBitmap validDocIdsSnapshot =
        validDocIds != null && !_queryContext.isSkipUpsert() ? validDocIds.getMutableRoaringBitmap() : null;
    int numDocs = _indexSegment.getSegmentMetadata().getTotalDocs();

    FilterContext filter = _filter != null ? _filter : _queryContext.getFilter();
    if (filter != null) {
      BaseFilterOperator filterOperator = constructPhysicalOperator(filter, numDocs);
      if (validDocIdsSnapshot != null) {
        BaseFilterOperator validDocFilter = new BitmapBasedFilterOperator(validDocIdsSnapshot, false, numDocs);
        return FilterOperatorUtils.getAndFilterOperator(Arrays.asList(filterOperator, validDocFilter), numDocs,
            _queryContext.getDebugOptions());
      } else {
        return filterOperator;
      }
    } else if (validDocIdsSnapshot != null) {
      return new BitmapBasedFilterOperator(validDocIdsSnapshot, false, numDocs);
    } else {
      return new MatchAllFilterOperator(numDocs);
    }
  }

  /**
   * Returns a map from predicates to their evaluators.
   */
  public Map<Predicate, PredicateEvaluator> getPredicateEvaluatorMap() {
    return _predicateEvaluatorMap;
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
  private boolean canApplyH3Index(Predicate predicate, FunctionContext function) {
    if (predicate.getType() != Predicate.Type.RANGE) {
      return false;
    }
    String functionName = function.getFunctionName();
    if (!functionName.equals("st_distance") && !functionName.equals("stdistance")) {
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
    return columnName != null && _indexSegment.getDataSource(columnName).getH3Index() != null && findLiteral;
  }

  /**
   * Helper method to build the operator tree from the filter.
   */
  private BaseFilterOperator constructPhysicalOperator(FilterContext filter, int numDocs) {
    switch (filter.getType()) {
      case AND:
        List<FilterContext> childFilters = filter.getChildren();
        List<BaseFilterOperator> childFilterOperators = new ArrayList<>(childFilters.size());
        for (FilterContext childFilter : childFilters) {
          BaseFilterOperator childFilterOperator = constructPhysicalOperator(childFilter, numDocs);
          if (childFilterOperator.isResultEmpty()) {
            // Return empty filter operator if any of the child filter operator's result is empty
            return EmptyFilterOperator.getInstance();
          } else if (!childFilterOperator.isResultMatchingAll()) {
            // Remove child filter operators that match all records
            childFilterOperators.add(childFilterOperator);
          }
        }
        return FilterOperatorUtils.getAndFilterOperator(childFilterOperators, numDocs, _queryContext.getDebugOptions());
      case OR:
        childFilters = filter.getChildren();
        childFilterOperators = new ArrayList<>(childFilters.size());
        for (FilterContext childFilter : childFilters) {
          BaseFilterOperator childFilterOperator = constructPhysicalOperator(childFilter, numDocs);
          if (childFilterOperator.isResultMatchingAll()) {
            // Return match all filter operator if any of the child filter operator matches all records
            return new MatchAllFilterOperator(numDocs);
          } else if (!childFilterOperator.isResultEmpty()) {
            // Remove child filter operators whose result is empty
            childFilterOperators.add(childFilterOperator);
          }
        }
        return FilterOperatorUtils.getOrFilterOperator(childFilterOperators, numDocs, _queryContext.getDebugOptions());
      case NOT:
        childFilters = filter.getChildren();
        assert childFilters.size() == 1;
        BaseFilterOperator childFilterOperator = constructPhysicalOperator(childFilters.get(0), numDocs);
        return FilterOperatorUtils.getNotFilterOperator(childFilterOperator, numDocs, null);
      case PREDICATE:
        Predicate predicate = filter.getPredicate();
        ExpressionContext lhs = predicate.getLhs();
        if (lhs.getType() == ExpressionContext.Type.FUNCTION) {
          if (canApplyH3Index(predicate, lhs.getFunction())) {
            return new H3IndexFilterOperator(_indexSegment, predicate, numDocs);
          }
          // TODO: ExpressionFilterOperator does not support predicate types without PredicateEvaluator (IS_NULL,
          //       IS_NOT_NULL, TEXT_MATCH)
          return new ExpressionFilterOperator(_indexSegment, predicate, numDocs);
        } else {
          String column = lhs.getIdentifier();
          DataSource dataSource = _indexSegment.getDataSource(column);
          PredicateEvaluator predicateEvaluator = _predicateEvaluatorMap.get(predicate);
          if (predicateEvaluator != null) {
            return FilterOperatorUtils.getLeafFilterOperator(predicateEvaluator, dataSource, numDocs);
          }
          switch (predicate.getType()) {
            case TEXT_MATCH:
              return new TextMatchFilterOperator(dataSource.getTextIndex(), (TextMatchPredicate) predicate, numDocs);
            case REGEXP_LIKE:
              // FST Index is available only for rolled out segments. So, we use different evaluator for rolled out and
              // consuming segments.
              //
              // Rolled out segments (immutable): FST Index reader is available use FSTBasedEvaluator
              // else use regular flow of getting predicate evaluator.
              //
              // Consuming segments: When FST is enabled, use AutomatonBasedEvaluator so that regexp matching logic is
              // similar to that of FSTBasedEvaluator, else use regular flow of getting predicate evaluator.
              if (dataSource.getFSTIndex() != null) {
                predicateEvaluator =
                    FSTBasedRegexpPredicateEvaluatorFactory.newFSTBasedEvaluator((RegexpLikePredicate) predicate,
                        dataSource.getFSTIndex(), dataSource.getDictionary());
              } else {
                predicateEvaluator =
                    PredicateEvaluatorProvider.getPredicateEvaluator(predicate, dataSource.getDictionary(),
                        dataSource.getDataSourceMetadata().getDataType());
              }
              _predicateEvaluatorMap.put(predicate, predicateEvaluator);
              return FilterOperatorUtils.getLeafFilterOperator(predicateEvaluator, dataSource, numDocs);
            case JSON_MATCH:
              JsonIndexReader jsonIndex = dataSource.getJsonIndex();
              Preconditions.checkState(jsonIndex != null, "Cannot apply JSON_MATCH on column: %s without json index",
                  column);
              return new JsonMatchFilterOperator(jsonIndex, (JsonMatchPredicate) predicate, numDocs);
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
              predicateEvaluator =
                  PredicateEvaluatorProvider.getPredicateEvaluator(predicate, dataSource.getDictionary(),
                      dataSource.getDataSourceMetadata().getDataType());
              _predicateEvaluatorMap.put(predicate, predicateEvaluator);
              return FilterOperatorUtils.getLeafFilterOperator(predicateEvaluator, dataSource, numDocs);
          }
        }
      default:
        throw new IllegalStateException();
    }
  }
}
