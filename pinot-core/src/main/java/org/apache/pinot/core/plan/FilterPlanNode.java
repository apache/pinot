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
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.request.context.predicate.JsonMatchPredicate;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.common.request.context.predicate.RegexpLikePredicate;
import org.apache.pinot.common.request.context.predicate.TextMatchPredicate;
import org.apache.pinot.common.utils.RegexpPatternConverterUtils;
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

  // Cache the predicate evaluators
  private final Map<Predicate, PredicateEvaluator> _predicateEvaluatorMap = new HashMap<>();

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
    ThreadSafeMutableRoaringBitmap validDocIds = _indexSegment.getValidDocIds();
    boolean applyValidDocIds = validDocIds != null && !QueryOptionsUtils.isSkipUpsert(_queryContext.getQueryOptions());
    if (filter != null) {
      BaseFilterOperator filterOperator = constructPhysicalOperator(filter);
      if (applyValidDocIds) {
        BaseFilterOperator validDocFilter =
            new BitmapBasedFilterOperator(validDocIds.getMutableRoaringBitmap(), false, _numDocs);
        return FilterOperatorUtils.getAndFilterOperator(Arrays.asList(filterOperator, validDocFilter), _numDocs,
            _queryContext.getDebugOptions());
      } else {
        return filterOperator;
      }
    } else if (applyValidDocIds) {
      return new BitmapBasedFilterOperator(validDocIds.getMutableRoaringBitmap(), false, _numDocs);
    } else {
      return new MatchAllFilterOperator(_numDocs);
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
    return columnName != null && _indexSegment.getDataSource(columnName).getH3Index() != null && findLiteral;
  }

  /**
   * Helper method to build the operator tree from the filter.
   */
  private BaseFilterOperator constructPhysicalOperator(FilterContext filter) {
    switch (filter.getType()) {
      case AND:
        List<FilterContext> childFilters = filter.getChildren();
        List<BaseFilterOperator> childFilterOperators = new ArrayList<>(childFilters.size());
        for (FilterContext childFilter : childFilters) {
          BaseFilterOperator childFilterOperator = constructPhysicalOperator(childFilter);
          if (childFilterOperator.isResultEmpty()) {
            // Return empty filter operator if any of the child filter operator's result is empty
            return EmptyFilterOperator.getInstance();
          } else if (!childFilterOperator.isResultMatchingAll()) {
            // Remove child filter operators that match all records
            childFilterOperators.add(childFilterOperator);
          }
        }
        return FilterOperatorUtils.getAndFilterOperator(childFilterOperators, _numDocs,
            _queryContext.getDebugOptions());
      case OR:
        childFilters = filter.getChildren();
        childFilterOperators = new ArrayList<>(childFilters.size());
        for (FilterContext childFilter : childFilters) {
          BaseFilterOperator childFilterOperator = constructPhysicalOperator(childFilter);
          if (childFilterOperator.isResultMatchingAll()) {
            // Return match all filter operator if any of the child filter operator matches all records
            return new MatchAllFilterOperator(_numDocs);
          } else if (!childFilterOperator.isResultEmpty()) {
            // Remove child filter operators whose result is empty
            childFilterOperators.add(childFilterOperator);
          }
        }
        return FilterOperatorUtils.getOrFilterOperator(childFilterOperators, _numDocs, _queryContext.getDebugOptions());
      case PREDICATE:
        Predicate predicate = filter.getPredicate();
        ExpressionContext lhs = predicate.getLhs();
        if (lhs.getType() == ExpressionContext.Type.FUNCTION) {
          if (canApplyH3Index(predicate, lhs.getFunction())) {
            return new H3IndexFilterOperator(_indexSegment, predicate, _numDocs);
          }
          // TODO: ExpressionFilterOperator does not support predicate types without PredicateEvaluator (IS_NULL,
          //       IS_NOT_NULL, TEXT_MATCH)
          return new ExpressionFilterOperator(_indexSegment, predicate, _numDocs);
        } else {
          String column = lhs.getIdentifier();
          DataSource dataSource = _indexSegment.getDataSource(column);
          PredicateEvaluator predicateEvaluator = _predicateEvaluatorMap.get(predicate);
          if (predicateEvaluator != null) {
            return FilterOperatorUtils.getLeafFilterOperator(predicateEvaluator, dataSource, _numDocs);
          }
          switch (predicate.getType()) {
            case TEXT_MATCH:
              return new TextMatchFilterOperator(dataSource.getTextIndex(), ((TextMatchPredicate) predicate).getValue(),
                  _numDocs);
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
                    FSTBasedRegexpPredicateEvaluatorFactory.newFSTBasedEvaluator(dataSource.getFSTIndex(),
                        dataSource.getDictionary(), RegexpPatternConverterUtils.regexpLikeToLuceneRegExp(
                            ((RegexpLikePredicate) predicate).getValue()));
              } else if (dataSource.getNativeFSTIndex() != null) {
                predicateEvaluator =
                    FSTBasedRegexpPredicateEvaluatorFactory.newFSTBasedEvaluator(dataSource.getNativeFSTIndex(),
                    dataSource.getDictionary(), RegexpPatternConverterUtils.regexpLikeToLuceneRegExp(
                        ((RegexpLikePredicate) predicate).getValue()));
              } else if (dataSource instanceof MutableDataSource &&
                  (((MutableDataSource) dataSource).isFSTEnabled() || ((MutableDataSource) dataSource).isNativeFSTEnabled())) {
                predicateEvaluator =
                    FSTBasedRegexpPredicateEvaluatorFactory.newAutomatonBasedEvaluator(dataSource.getDictionary(),
                        RegexpPatternConverterUtils.regexpLikeToLuceneRegExp(
                            ((RegexpLikePredicate) predicate).getValue()));
              } else {
                predicateEvaluator =
                    PredicateEvaluatorProvider.getPredicateEvaluator(predicate, dataSource.getDictionary(),
                        dataSource.getDataSourceMetadata().getDataType());
              }
              _predicateEvaluatorMap.put(predicate, predicateEvaluator);
              return FilterOperatorUtils.getLeafFilterOperator(predicateEvaluator, dataSource, _numDocs);
            case JSON_MATCH:
              JsonIndexReader jsonIndex = dataSource.getJsonIndex();
              Preconditions.checkState(jsonIndex != null, "Cannot apply JSON_MATCH on column: %s without json index",
                  column);
              return new JsonMatchFilterOperator(jsonIndex, ((JsonMatchPredicate) predicate).getValue(), _numDocs);
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
              predicateEvaluator =
                  PredicateEvaluatorProvider.getPredicateEvaluator(predicate, dataSource.getDictionary(),
                      dataSource.getDataSourceMetadata().getDataType());
              _predicateEvaluatorMap.put(predicate, predicateEvaluator);
              return FilterOperatorUtils.getLeafFilterOperator(predicateEvaluator, dataSource, _numDocs);
          }
        }
      default:
        throw new IllegalStateException();
    }
  }
}
