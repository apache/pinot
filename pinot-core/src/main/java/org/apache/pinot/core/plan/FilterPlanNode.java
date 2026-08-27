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
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.request.context.predicate.JsonMatchPredicate;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.common.request.context.predicate.TextMatchPredicate;
import org.apache.pinot.common.request.context.predicate.VectorSimilarityPredicate;
import org.apache.pinot.common.request.context.predicate.VectorSimilarityRadiusPredicate;
import org.apache.pinot.core.geospatial.transform.function.StDistanceFunction;
import org.apache.pinot.core.operator.filter.BaseFilterOperator;
import org.apache.pinot.core.operator.filter.BitmapBasedFilterOperator;
import org.apache.pinot.core.operator.filter.BitmapCollection;
import org.apache.pinot.core.operator.filter.EmptyFilterOperator;
import org.apache.pinot.core.operator.filter.ExactVectorScanFilterOperator;
import org.apache.pinot.core.operator.filter.ExpressionFilterOperator;
import org.apache.pinot.core.operator.filter.FilterOperatorUtils;
import org.apache.pinot.core.operator.filter.H3InclusionIndexFilterOperator;
import org.apache.pinot.core.operator.filter.H3IndexFilterOperator;
import org.apache.pinot.core.operator.filter.JsonMatchFilterOperator;
import org.apache.pinot.core.operator.filter.MapFilterOperator;
import org.apache.pinot.core.operator.filter.MatchAllFilterOperator;
import org.apache.pinot.core.operator.filter.TextMatchFilterOperator;
import org.apache.pinot.core.operator.filter.VectorDistanceUtils;
import org.apache.pinot.core.operator.filter.VectorRadiusFilterOperator;
import org.apache.pinot.core.operator.filter.VectorSearchMode;
import org.apache.pinot.core.operator.filter.VectorSearchParams;
import org.apache.pinot.core.operator.filter.VectorSearchStrategy;
import org.apache.pinot.core.operator.filter.VectorSimilarityFilterOperator;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluatorProvider;
import org.apache.pinot.core.operator.transform.function.ItemTransformFunction;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentContext;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.IndexService;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.creator.VectorBackendType;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;
import org.apache.pinot.segment.spi.index.multicolumntext.MultiColumnTextMetadata;
import org.apache.pinot.segment.spi.index.reader.FilterAwareVectorIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.JsonIndexReader;
import org.apache.pinot.segment.spi.index.reader.NullValueVectorReader;
import org.apache.pinot.segment.spi.index.reader.TextIndexReader;
import org.apache.pinot.segment.spi.index.reader.VectorIndexReader;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


public class FilterPlanNode implements PlanNode {
  private final IndexSegment _indexSegment;
  private final SegmentContext _segmentContext;
  private final QueryContext _queryContext;
  private final FilterContext _filter;
  /// Documents a vector predicate may consider as candidates for this query, or null when the query places no such
  /// restriction. Held by reference; the snapshot is not modified while the plan executes.
  @Nullable
  private ImmutableRoaringBitmap _requiredVectorDocIds;

  // Cache the predicate evaluators
  private final List<Pair<Predicate, PredicateEvaluator>> _predicateEvaluators = new ArrayList<>(4);

  public FilterPlanNode(SegmentContext segmentContext, QueryContext queryContext) {
    this(segmentContext, queryContext, null);
  }

  public FilterPlanNode(SegmentContext segmentContext, QueryContext queryContext, @Nullable FilterContext filter) {
    _indexSegment = segmentContext.getIndexSegment();
    _segmentContext = segmentContext;
    _queryContext = queryContext;
    _filter = filter != null ? filter : _queryContext.getFilter();
  }

  @Override
  public BaseFilterOperator run() {
    // Read the snapshot before the document count, never the other way round. A consuming segment publishes a row by
    // adding it, then raising the count, then marking it valid, so reading the count last guarantees every document in
    // the snapshot is below it. See FilterPlanNodeTest#testConsistentSnapshot.
    MutableRoaringBitmap docIdsSnapshot = _segmentContext.getDocIdsSnapshot();
    int numDocs = _indexSegment.getSegmentMetadata().getTotalDocs();
    // Vector top-K is not monotonic, so a restricted visible-document set must reach candidate generation instead of
    // only being intersected with the result. Test the snapshot first so ordinary queries skip the filter tree walk.
    _requiredVectorDocIds = docIdsSnapshot != null && _filter != null && containsVectorPredicate(_filter)
        ? docIdsSnapshot : null;

    // Candidate generation and the outer valid-document AND must observe the same document set for this query.
    ImmutableRoaringBitmap outerDocIdsSnapshot = docIdsSnapshot;

    if (_filter != null) {
      BaseFilterOperator filterOperator = constructPhysicalOperator(_filter, numDocs);
      if (outerDocIdsSnapshot != null) {
        BaseFilterOperator validDocFilter =
            new BitmapBasedFilterOperator(outerDocIdsSnapshot, false, numDocs);
        return FilterOperatorUtils.getAndFilterOperator(_queryContext, Arrays.asList(filterOperator, validDocFilter),
            numDocs);
      } else {
        return filterOperator;
      }
    } else if (outerDocIdsSnapshot != null) {
      return new BitmapBasedFilterOperator(outerDocIdsSnapshot, false, numDocs);
    } else {
      return new MatchAllFilterOperator(numDocs);
    }
  }

  /// Returns a mapping from predicates to their evaluators.
  public List<Pair<Predicate, PredicateEvaluator>> getPredicateEvaluators() {
    return _predicateEvaluators;
  }

  /// H3 index can be applied on ST_Distance iff:
  ///
  /// - Predicate is of type RANGE
  /// - Left-hand-side of the predicate is an ST_Distance function
  /// - One argument of the ST_Distance function is an identifier, the other argument is an literal
  /// - The identifier column has H3 index
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
        columnName = argument.getIdentifier();
      } else if (argument.getType() == ExpressionContext.Type.LITERAL) {
        findLiteral = true;
      }
    }
    if (columnName == null || !findLiteral) {
      return false;
    }
    DataSource dataSource = _indexSegment.getDataSourceNullable(columnName);
    return dataSource != null && dataSource.getH3Index() != null && _queryContext.isIndexUseAllowed(columnName,
        FieldConfig.IndexType.H3);
  }

  /// H3 index can be applied for inclusion check iff:
  ///
  /// - Predicate is of type EQ
  /// - Left-hand-side of the predicate is an ST_Within or ST_Contains function
  /// - For ST_Within, the first argument is an identifier, the second argument is literal
  /// - For ST_Contains function the first argument is literal, the second argument is an identifier
  /// - The identifier column has H3 index
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
        String columnName = arguments.get(0).getIdentifier();
        DataSource dataSource = _indexSegment.getDataSourceNullable(columnName);
        return dataSource != null && dataSource.getH3Index() != null && _queryContext.isIndexUseAllowed(columnName,
            FieldConfig.IndexType.H3);
      }
      return false;
    } else {
      if (arguments.get(1).getType() == ExpressionContext.Type.IDENTIFIER
          && arguments.get(0).getType() == ExpressionContext.Type.LITERAL) {
        String columnName = arguments.get(1).getIdentifier();
        DataSource dataSource = _indexSegment.getDataSourceNullable(columnName);
        return dataSource != null && dataSource.getH3Index() != null && _queryContext.isIndexUseAllowed(columnName,
            FieldConfig.IndexType.H3);
      }
      return false;
    }
  }

  private boolean canApplyMapFilter(Predicate predicate) {
    // Get column name and key name from function arguments
    FunctionContext function = predicate.getLhs().getFunction();

    // Check if the function is an ItemTransformFunction
    return function.getFunctionName().equals(ItemTransformFunction.FUNCTION_NAME);
  }

  /// Helper method to build the operator tree from the filter.
  private BaseFilterOperator constructPhysicalOperator(FilterContext filter, int numDocs) {
    List<FilterContext> childFilters;
    List<BaseFilterOperator> childFilterOperators;
    switch (filter.getType()) {
      case AND:
        childFilters = filter.getChildren();
        childFilterOperators = new ArrayList<>(childFilters.size());
        List<FilterContext> retainedChildFilters = new ArrayList<>(childFilters.size());
        for (FilterContext childFilter : childFilters) {
          BaseFilterOperator childFilterOperator;
          if (isVectorSimilarityFilter(childFilter) && hasSafeMetadataSibling(childFilters)) {
            // Pass filtered context so vector operator reports correct execution mode
            childFilterOperator = constructFilteredVectorOperator(childFilter, numDocs);
          } else {
            childFilterOperator = constructPhysicalOperator(childFilter, numDocs);
          }
          if (childFilterOperator.isResultEmpty()) {
            // Return empty filter operator if any of the child filter operator's result is empty
            return EmptyFilterOperator.getInstance();
          } else if (!childFilterOperator.isResultMatchingAll()) {
            // Remove child filter operators that match all records
            childFilterOperators.add(childFilterOperator);
            retainedChildFilters.add(childFilter);
          }
        }
        // Wire pre-filter bitmaps for filter-aware ANN: if an AND contains a
        // VectorSimilarityFilterOperator alongside other filter children, evaluate the
        // non-vector filters first and pass the resulting bitmap to the vector operator
        // so it can restrict HNSW graph traversal to the pre-filtered document set.
        wirePreFilterForVectorOperators(retainedChildFilters, childFilterOperators, numDocs);
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
          } else if (canApplyMapFilter(predicate)) {
            return new MapFilterOperator(_indexSegment, predicate, _queryContext, numDocs);
          } else {
            // TODO: ExpressionFilterOperator does not support predicate types without PredicateEvaluator (TEXT_MATCH)
            return new ExpressionFilterOperator(_indexSegment, _queryContext, predicate, numDocs);
          }
        } else {
          String column = lhs.getIdentifier();
          DataSource dataSource = _indexSegment.getDataSource(column, _queryContext.getSchema());
          PredicateEvaluator predicateEvaluator;
          TextIndexReader textIndexReader;
          switch (predicate.getType()) {
            case TEXT_MATCH:
              textIndexReader = dataSource.getTextIndex();
              if (textIndexReader == null) {
                MultiColumnTextMetadata meta = _indexSegment.getSegmentMetadata().getMultiColumnTextMetadata();
                if (meta != null && meta.getColumns().contains(column)) {
                  textIndexReader = _indexSegment.getMultiColumnTextIndex();
                }
              }

              Preconditions.checkState(textIndexReader != null,
                  "Cannot apply TEXT_MATCH on column: %s without text index", column);

              if (textIndexReader.isMultiColumn()) {
                return new TextMatchFilterOperator(column, textIndexReader, (TextMatchPredicate) predicate, numDocs);
              } else {
                return new TextMatchFilterOperator(textIndexReader, (TextMatchPredicate) predicate, numDocs);
              }
            case REGEXP_LIKE:
              // PredicateEvaluatorProvider handles FST/IFST upgrade internally when the dictionary is usable for
              // filtering and a matching text index exists on the data source.
              predicateEvaluator =
                  PredicateEvaluatorProvider.getPredicateEvaluator(predicate, dataSource, _queryContext);
              _predicateEvaluators.add(Pair.of(predicate, predicateEvaluator));
              return FilterOperatorUtils.getLeafFilterOperator(_queryContext, predicateEvaluator, dataSource, numDocs);
            case JSON_MATCH:
              JsonIndexReader jsonIndex = dataSource.getJsonIndex();
              if (jsonIndex == null) { //TODO: rework
                Optional<IndexType<?, ?, ?>> compositeIndex =
                    IndexService.getInstance().getOptional("composite_json_index");
                if (compositeIndex.isPresent()) {
                  jsonIndex =
                      (JsonIndexReader) dataSource.getIndex(compositeIndex.get());
                }
              }
              Preconditions.checkState(jsonIndex != null, "Cannot apply JSON_MATCH on column: %s without json index",
                  column);
              return new JsonMatchFilterOperator(jsonIndex, (JsonMatchPredicate) predicate, numDocs);
            case VECTOR_SIMILARITY:
              return constructVectorSimilarityOperator(dataSource, (VectorSimilarityPredicate) predicate, column,
                  numDocs, false);
            case VECTOR_SIMILARITY_RADIUS:
              return constructVectorRadiusOperator(dataSource,
                  (VectorSimilarityRadiusPredicate) predicate, column, numDocs);
            case IS_NULL: {
              NullValueVectorReader nullValueVector = dataSource.getNullValueVector();
              if (nullValueVector != null) {
                return new BitmapBasedFilterOperator(nullValueVector.getNullBitmap(), false, numDocs);
              } else {
                return EmptyFilterOperator.getInstance();
              }
            }
            case IS_NOT_NULL: {
              NullValueVectorReader nullValueVector = dataSource.getNullValueVector();
              if (nullValueVector != null) {
                return new BitmapBasedFilterOperator(nullValueVector.getNullBitmap(), true, numDocs);
              } else {
                return new MatchAllFilterOperator(numDocs);
              }
            }
            default:
              predicateEvaluator =
                  PredicateEvaluatorProvider.getPredicateEvaluator(predicate, dataSource, _queryContext);
              _predicateEvaluators.add(Pair.of(predicate, predicateEvaluator));
              return FilterOperatorUtils.getLeafFilterOperator(_queryContext, predicateEvaluator, dataSource, numDocs);
          }
        }
      case CONSTANT:
        return filter.isConstantTrue() ? new MatchAllFilterOperator(numDocs) : EmptyFilterOperator.getInstance();
      default:
        throw new IllegalStateException();
    }
  }

  /// Constructs the appropriate vector similarity filter operator based on index availability.
  ///
  /// Decision tree:
  ///
  /// 1. If the segment has a vector index for the column, use [VectorSimilarityFilterOperator]
  ///       with query options (nprobe, rerank, maxCandidates).
  /// 2. If no vector index exists, fall back to [ExactVectorScanFilterOperator] which
  ///       performs brute-force scan of the forward index.
  ///
  /// @param hasMetadataFilter true if this vector predicate is combined with metadata filters (AND)
  private BaseFilterOperator constructVectorSimilarityOperator(DataSource dataSource,
      VectorSimilarityPredicate predicate, String column, int numDocs, boolean hasMetadataFilter) {
    VectorIndexReader vectorIndex = dataSource.getVectorIndex();
    VectorIndexConfig vectorIndexConfig = dataSource.getVectorIndexConfig();
    boolean isMutableSegment = _indexSegment.getSegmentMetadata().isMutableSegment();
    VectorSearchParams searchParams = VectorSearchParams.fromQueryOptions(_queryContext.getQueryOptions());

    // Nothing in this segment is visible to the query, so no candidate generation is needed and no reader
    // capability is required. The outer valid-document AND would discard any result anyway.
    if (_requiredVectorDocIds != null && _requiredVectorDocIds.isEmpty()) {
      return EmptyFilterOperator.getInstance();
    }

    if (vectorIndex != null) {
      // A required candidate scope can only be honored by a reader that does filtered search. When it cannot, the
      // ANN index is unusable for this query and an exact scan over the allowed documents is the correct plan --
      // decided here, where both the reader capability and the forward index availability are known.
      if (_requiredVectorDocIds != null && !supportsPreFilter(vectorIndex)) {
        ForwardIndexReader<?> exactScanReader = dataSource.getForwardIndex();
        Preconditions.checkState(exactScanReader != null,
            "Cannot honor required candidate doc IDs on vector column: %s -- vector index reader does not support "
                + "filtered search and no forward index is available", column);
        return new ExactVectorScanFilterOperator(exactScanReader, predicate, column, numDocs, vectorIndexConfig,
            getFilteredSearchUnsupportedReason(isMutableSegment), searchParams, _requiredVectorDocIds);
      }

      // ANN index path: pass the forward index when rerank or threshold needs exact distances.
      ForwardIndexReader<?> forwardIndexReader = null;
      VectorBackendType backendType = VectorDistanceUtils.resolveBackendType(vectorIndexConfig);
      if (searchParams.isExactRerank(backendType) || searchParams.hasDistanceThreshold()) {
        forwardIndexReader = dataSource.getForwardIndex();
        Preconditions.checkState(!searchParams.hasDistanceThreshold() || forwardIndexReader != null,
            "Cannot apply vectorDistanceThreshold on column: %s -- forward index required for threshold refinement",
            column);
      }
      return new VectorSimilarityFilterOperator(vectorIndex, predicate, numDocs, searchParams, forwardIndexReader,
          vectorIndexConfig, hasMetadataFilter, _requiredVectorDocIds);
    }

    // Exact scan fallback: no vector index on this segment
    ForwardIndexReader<?> forwardIndexReader = dataSource.getForwardIndex();
    Preconditions.checkState(forwardIndexReader != null,
        "Cannot apply VECTOR_SIMILARITY on column: %s -- no vector index and no forward index available", column);
    return new ExactVectorScanFilterOperator(forwardIndexReader, predicate, column, numDocs, vectorIndexConfig,
        getVectorFallbackReason(vectorIndexConfig, isMutableSegment), searchParams, _requiredVectorDocIds);
  }

  /// Constructs a vector operator for a VECTOR_SIMILARITY predicate that is part of an AND
  /// with metadata filters. This sets the hasMetadataFilter flag so the operator reports
  /// the correct filtered ANN execution mode.
  private BaseFilterOperator constructFilteredVectorOperator(FilterContext filter, int numDocs) {
    Predicate predicate = filter.getPredicate();
    String column = predicate.getLhs().getIdentifier();
    DataSource dataSource = _indexSegment.getDataSource(column, _queryContext.getSchema());
    return constructVectorSimilarityOperator(dataSource, (VectorSimilarityPredicate) predicate, column,
        numDocs, true);
  }

  /// Returns true if the child list contains at least one sibling subtree with no vector predicate.
  private static boolean hasSafeMetadataSibling(List<FilterContext> childFilters) {
    for (FilterContext child : childFilters) {
      if (!containsVectorPredicate(child)) {
        return true;
      }
    }
    return false;
  }

  /// Returns true if the filter is a VECTOR_SIMILARITY predicate.
  private static boolean isVectorSimilarityFilter(FilterContext filter) {
    return filter.getType() == FilterContext.Type.PREDICATE
        && filter.getPredicate().getType() == Predicate.Type.VECTOR_SIMILARITY;
  }

  /// Returns true when any node in the subtree is a vector top-K predicate. Such a subtree must never be
  /// materialized as metadata for another vector predicate because doing so executes candidate generation out of its
  /// original boolean context and can incorrectly push candidate results into a sibling.
  private static boolean containsVectorPredicate(FilterContext filter) {
    if (filter.getType() == FilterContext.Type.PREDICATE) {
      return filter.getPredicate().getType() == Predicate.Type.VECTOR_SIMILARITY;
    }
    if (filter.getType() == FilterContext.Type.AND || filter.getType() == FilterContext.Type.OR
        || filter.getType() == FilterContext.Type.NOT) {
      for (FilterContext child : filter.getChildren()) {
        if (containsVectorPredicate(child)) {
          return true;
        }
      }
    }
    return false;
  }

  /// Constructs the vector radius filter operator based on index availability.
  ///
  /// The radius operator always needs the forward index for exact distance computation.
  /// When a vector index is available, it is used for candidate retrieval before exact filtering.
  private BaseFilterOperator constructVectorRadiusOperator(DataSource dataSource,
      VectorSimilarityRadiusPredicate predicate, String column, int numDocs) {
    ForwardIndexReader<?> forwardIndexReader = dataSource.getForwardIndex();
    Preconditions.checkState(forwardIndexReader != null,
        "Cannot apply VECTOR_SIMILARITY_RADIUS on column: %s -- no forward index available", column);
    VectorIndexReader vectorIndex = dataSource.getVectorIndex();
    VectorIndexConfig vectorIndexConfig = dataSource.getVectorIndexConfig();
    return new VectorRadiusFilterOperator(forwardIndexReader, vectorIndex, predicate, column, numDocs,
        vectorIndexConfig);
  }

  /// Wires metadata bitmaps when an AND node contains both vector similarity operators and non-vector filters.
  ///
  /// The existing adaptive behavior is preserved: only bitmap-producing filters are considered, and
  /// [VectorSearchStrategy] decides whether the filter-aware reader should receive the bitmap.
  ///
  /// **Trade-off: eager filter evaluation.** The non-vector filter predicates are materialized
  /// into bitmaps before the vector search begins. This is intentional because the filter bitmap must
  /// be fully materialized before it can be passed to the vector index for pre-filtered ANN search.
  /// The [VectorSearchStrategy] selectivity check below ensures queries only pay this cost when the estimated
  /// cardinality suggests pre-filtering is worthwhile.
  ///
  /// If no vector operators are found, this method is a no-op. A query whose reader does not support pre-filtering
  /// retains the default post-filter path.
  ///
  /// @param childOperators the list of child filter operators under an AND node
  /// @param numDocs total documents in the segment
  private void wirePreFilterForVectorOperators(List<FilterContext> childFilters,
      List<BaseFilterOperator> childOperators, int numDocs) {
    if (childOperators.size() < 2) {
      return;
    }

    // Find indexed vector similarity operators and non-vector metadata siblings.
    List<VectorSimilarityFilterOperator> vectorOps = new ArrayList<>();
    List<BaseFilterOperator> nonVectorOps = new ArrayList<>();
    for (int i = 0; i < childOperators.size(); i++) {
      FilterContext childFilter = childFilters.get(i);
      BaseFilterOperator op = childOperators.get(i);
      if (isVectorSimilarityFilter(childFilter) && op instanceof VectorSimilarityFilterOperator) {
        vectorOps.add((VectorSimilarityFilterOperator) op);
      } else if (!containsVectorPredicate(childFilter)) {
        nonVectorOps.add(op);
      }
    }

    if (vectorOps.isEmpty() || nonVectorOps.isEmpty()) {
      return;
    }

    // Early exit: only proceed if at least one vector operator actually supports pre-filtering.
    // This avoids eagerly materializing non-vector filter bitmaps when they can't be used.
    boolean anySupportsPreFilter = false;
    for (VectorSimilarityFilterOperator vectorOp : vectorOps) {
      if (vectorOp.supportsPreFilter()) {
        anySupportsPreFilter = true;
        break;
      }
    }
    if (!anySupportsPreFilter) {
      return;
    }

    // Evaluate non-vector filters and combine their bitmaps to produce a pre-filter.
    // Only do this if the non-vector operators can produce bitmaps efficiently.
    boolean allCanProduceBitmaps = true;
    for (BaseFilterOperator op : nonVectorOps) {
      if (!op.canProduceBitmaps()) {
        allCanProduceBitmaps = false;
        break;
      }
    }

    if (!allCanProduceBitmaps) {
      return;
    }

    // Combine non-vector filter bitmaps via AND.
    // Bitmap-producing filters are cheap to evaluate again when the final AND executes.
    MutableRoaringBitmap combinedBitmap = null;
    for (BaseFilterOperator op : nonVectorOps) {
      BitmapCollection bitmapCollection = op.getBitmaps();
      org.roaringbitmap.buffer.ImmutableRoaringBitmap reduced = bitmapCollection.reduce();
      if (combinedBitmap == null) {
        combinedBitmap = reduced.toMutableRoaringBitmap();
      } else {
        combinedBitmap.and(reduced);
      }
    }

    if (combinedBitmap == null || combinedBitmap.isEmpty()) {
      return;
    }

    // Use VectorSearchStrategy to decide whether pre-filtering is worthwhile based on
    // the estimated selectivity. Only pass the bitmap if the strategy recommends
    // FILTER_THEN_ANN; otherwise fall back to the default post-filter path.
    int estimatedFilteredDocs = combinedBitmap.getCardinality();
    // backendType and searchParams are passed as null here because at the pre-filter wiring
    // stage we are deciding whether to activate pre-filtering at all, not per-backend tuning.
    // The strategy currently uses only selectivity (numDocs, estimatedFilteredDocs) for this
    // decision. Per-backend and per-query-option tuning is handled later inside the operator.
    VectorSearchStrategy.Decision decision = VectorSearchStrategy.decide(
        numDocs, estimatedFilteredDocs,
        /* hasVectorIndex= */ true,
        /* indexSupportsPreFilter= */ true,
        /* isMutableSegment= */ false,
        /* backendType= */ null,
        /* searchParams= */ null);

    if (decision.getMode() != VectorSearchMode.FILTER_THEN_ANN) {
      return;
    }

    // Pass the pre-filter bitmap only to vector operators that support pre-filtering
    for (VectorSimilarityFilterOperator vectorOp : vectorOps) {
      if (vectorOp.supportsPreFilter()) {
        vectorOp.setPreFilterBitmap(combinedBitmap);
      }
    }
  }

  private static boolean supportsPreFilter(VectorIndexReader vectorIndex) {
    return vectorIndex instanceof FilterAwareVectorIndexReader
        && ((FilterAwareVectorIndexReader) vectorIndex).supportsPreFilter();
  }

  /// Reason reported by [ExactVectorScanFilterOperator] when a vector index exists but cannot restrict its search to
  /// the documents the query is allowed to see.
  private static String getFilteredSearchUnsupportedReason(boolean isMutableSegment) {
    return isMutableSegment ? "mutable_vector_index_not_filter_aware"
        : "vector_index_not_filter_aware";
  }

  private static String getVectorFallbackReason(@Nullable VectorIndexConfig vectorIndexConfig,
      boolean isMutableSegment) {
    if (vectorIndexConfig == null || vectorIndexConfig.isDisabled()) {
      return isMutableSegment ? "vector_index_missing_on_mutable_segment" : "vector_index_missing";
    }
    VectorBackendType backendType = vectorIndexConfig.resolveBackendType();
    if (isMutableSegment && !backendType.supportsMutableSegments()) {
      return backendType.name().toLowerCase() + "_mutable_segment_unavailable";
    }
    return backendType.supportsMutableSegments() ? "vector_index_missing"
        : backendType.name().toLowerCase() + "_index_unavailable";
  }
}
