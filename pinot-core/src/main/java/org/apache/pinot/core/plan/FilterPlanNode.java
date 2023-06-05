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
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.request.context.predicate.JsonMatchPredicate;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.common.request.context.predicate.RegexpLikePredicate;
import org.apache.pinot.common.request.context.predicate.TextContainsPredicate;
import org.apache.pinot.common.request.context.predicate.TextMatchPredicate;
import org.apache.pinot.core.geospatial.transform.function.StDistanceFunction;
import org.apache.pinot.core.operator.filter.BaseFilterOperator;
import org.apache.pinot.core.operator.filter.BitmapBasedFilterOperator;
import org.apache.pinot.core.operator.filter.EmptyFilterOperator;
import org.apache.pinot.core.operator.filter.ExpressionFilterOperator;
import org.apache.pinot.core.operator.filter.FilterOperatorUtils;
import org.apache.pinot.core.operator.filter.H3InclusionIndexFilterOperator;
import org.apache.pinot.core.operator.filter.H3IndexFilterOperator;
import org.apache.pinot.core.operator.filter.JsonMatchFilterOperator;
import org.apache.pinot.core.operator.filter.MatchAllFilterOperator;
import org.apache.pinot.core.operator.filter.TextContainsFilterOperator;
import org.apache.pinot.core.operator.filter.TextMatchFilterOperator;
import org.apache.pinot.core.operator.filter.predicate.FSTBasedRegexpPredicateEvaluatorFactory;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluatorProvider;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.local.realtime.impl.invertedindex.NativeMutableTextIndex;
import org.apache.pinot.segment.local.segment.index.readers.text.NativeTextIndexReader;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.segment.spi.index.reader.JsonIndexReader;
import org.apache.pinot.segment.spi.index.reader.NullValueVectorReader;
import org.apache.pinot.segment.spi.index.reader.TextIndexReader;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


public class FilterPlanNode implements PlanNode {

  private final IndexSegment _indexSegment;
  private final QueryContext _queryContext;
  private final FilterContext _filter;

  // Cache the predicate evaluators
  private final List<Pair<Predicate, PredicateEvaluator>> _predicateEvaluators = new ArrayList<>(4);

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
    // NOTE: Snapshot the queryableDocIds before reading the numDocs to prevent the latest updates getting lost
    MutableRoaringBitmap queryableDocIdSnapshot = null;
    if (!_queryContext.isSkipUpsert()) {
      ThreadSafeMutableRoaringBitmap queryableDocIds = _indexSegment.getQueryableDocIds();
      if (queryableDocIds != null) {
        queryableDocIdSnapshot = queryableDocIds.getMutableRoaringBitmap();
      } else {
        ThreadSafeMutableRoaringBitmap validDocIds = _indexSegment.getValidDocIds();
        if (validDocIds != null) {
          queryableDocIdSnapshot = validDocIds.getMutableRoaringBitmap();
        }
      }
    }
    int numDocs = _indexSegment.getSegmentMetadata().getTotalDocs();

    FilterContext filter = _filter != null ? _filter : _queryContext.getFilter();
    if (filter != null) {
      BaseFilterOperator filterOperator = constructPhysicalOperator(filter, numDocs);
      if (queryableDocIdSnapshot != null) {
        BaseFilterOperator validDocFilter = new BitmapBasedFilterOperator(queryableDocIdSnapshot, false, numDocs);
        return FilterOperatorUtils.getAndFilterOperator(_queryContext, Arrays.asList(filterOperator, validDocFilter),
            numDocs);
      } else {
        return filterOperator;
      }
    } else if (queryableDocIdSnapshot != null) {
      return new BitmapBasedFilterOperator(queryableDocIdSnapshot, false, numDocs);
    } else {
      return new MatchAllFilterOperator(numDocs);
    }
  }

  /**
   * Returns a mapping from predicates to their evaluators.
   */
  public List<Pair<Predicate, PredicateEvaluator>> getPredicateEvaluators() {
    return _predicateEvaluators;
  }

  /**
   * H3 index can be applied on ST_Distance iff:
   * <ul>
   *   <li>Predicate is of type RANGE</li>
   *   <li>Left-hand-side of the predicate is an ST_Distance function</li>
   *   <li>One argument of the ST_Distance function is an identifier, the other argument is an literal</li>
   *   <li>The identifier column has H3 index</li>
   * </ul>
   */
  private boolean canApplyH3IndexForDistanceCheck(Predicate predicate, FunctionContext function) {
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
        columnName = argument.getIdentifierName();
      } else if (argument.getType() == ExpressionContext.Type.LITERAL) {
        findLiteral = true;
      }
    }
    return columnName != null && _indexSegment.getDataSource(columnName).getH3Index() != null && findLiteral;
  }

  /**
   * H3 index can be applied for inclusion check iff:
   * <ul>
   *   <li>Predicate is of type EQ</li>
   *   <li>Left-hand-side of the predicate is an ST_Within or ST_Contains function</li>
   *   <li>For ST_Within, the first argument is an identifier, the second argument is literal</li>
   *   <li>For ST_Contains function the first argument is literal, the second argument is an identifier</li>
   *   <li>The identifier column has H3 index</li>
   * </ul>
   */
  private boolean canApplyH3IndexForInclusionCheck(Predicate predicate, FunctionContext function) {
    if (predicate.getType() != Predicate.Type.EQ) {
      return false;
    }
    String functionName = function.getFunctionName();
    if (!functionName.equals("stwithin") && !functionName.equals("stcontains")) {
      return false;
    }
    List<ExpressionContext> arguments = function.getArguments();
    if (arguments.size() != 2) {
      throw new BadQueryRequestException("Expect 2 arguments for function: " + functionName);
    }
    // TODO: handle nested geography/geometry conversion functions
    if (functionName.equals("stwithin")) {
      if (arguments.get(0).getType() == ExpressionContext.Type.IDENTIFIER
          && arguments.get(1).getType() == ExpressionContext.Type.LITERAL) {
        String columnName = arguments.get(0).getIdentifierName();
        return _indexSegment.getDataSource(columnName).getH3Index() != null;
      }
      return false;
    } else {
      if (arguments.get(1).getType() == ExpressionContext.Type.IDENTIFIER
          && arguments.get(0).getType() == ExpressionContext.Type.LITERAL) {
        String columnName = arguments.get(1).getIdentifierName();
        return _indexSegment.getDataSource(columnName).getH3Index() != null;
      }
      return false;
    }
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
        return FilterOperatorUtils.getAndFilterOperator(_queryContext, childFilterOperators, numDocs);
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
        return FilterOperatorUtils.getOrFilterOperator(_queryContext, childFilterOperators, numDocs);
      case NOT:
        childFilters = filter.getChildren();
        assert childFilters.size() == 1;
        BaseFilterOperator childFilterOperator = constructPhysicalOperator(childFilters.get(0), numDocs);
        return FilterOperatorUtils.getNotFilterOperator(_queryContext, childFilterOperator, numDocs);
      case PREDICATE:
        Predicate predicate = filter.getPredicate();
        ExpressionContext lhs = predicate.getLhs();
        if (lhs.getType() == ExpressionContext.Type.FUNCTION) {
          if (canApplyH3IndexForDistanceCheck(predicate, lhs.getFunction())) {
            return new H3IndexFilterOperator(_indexSegment, _queryContext, predicate, numDocs);
          } else if (canApplyH3IndexForInclusionCheck(predicate, lhs.getFunction())) {
            return new H3InclusionIndexFilterOperator(_indexSegment, _queryContext, predicate, numDocs);
          } else {
            // TODO: ExpressionFilterOperator does not support predicate types without PredicateEvaluator (IS_NULL,
            //       IS_NOT_NULL, TEXT_MATCH)
            return new ExpressionFilterOperator(_indexSegment, _queryContext, predicate, numDocs);
          }
        } else {
          String column = lhs.getIdentifierName();
          DataSource dataSource = _indexSegment.getDataSource(column);
          PredicateEvaluator predicateEvaluator;
          switch (predicate.getType()) {
            case TEXT_CONTAINS:
              TextIndexReader textIndexReader = dataSource.getTextIndex();
              if (!(textIndexReader instanceof NativeTextIndexReader)
                  && !(textIndexReader instanceof NativeMutableTextIndex)) {
                throw new UnsupportedOperationException("TEXT_CONTAINS is supported only on native text index");
              }
              return new TextContainsFilterOperator(textIndexReader, (TextContainsPredicate) predicate, numDocs);
            case TEXT_MATCH:
              textIndexReader = dataSource.getTextIndex();
              Preconditions.checkState(textIndexReader != null,
                  "Cannot apply TEXT_MATCH on column: %s without text index", column);
              // We could check for real time and segment Lucene reader, but easier to check the other way round
              if (textIndexReader instanceof NativeTextIndexReader
                  || textIndexReader instanceof NativeMutableTextIndex) {
                throw new UnsupportedOperationException("TEXT_MATCH is not supported on native text index");
              }
              return new TextMatchFilterOperator(textIndexReader, (TextMatchPredicate) predicate, numDocs);
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
              _predicateEvaluators.add(Pair.of(predicate, predicateEvaluator));
              return FilterOperatorUtils.getLeafFilterOperator(predicateEvaluator, dataSource, numDocs,
                  _queryContext.isNullHandlingEnabled());
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
                  PredicateEvaluatorProvider.getPredicateEvaluator(predicate, dataSource, _queryContext);
              _predicateEvaluators.add(Pair.of(predicate, predicateEvaluator));
              return FilterOperatorUtils.getLeafFilterOperator(predicateEvaluator, dataSource, numDocs,
                  _queryContext.isNullHandlingEnabled());
          }
        }
      default:
        throw new IllegalStateException();
    }
  }
}
