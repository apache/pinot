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
import org.apache.pinot.core.operator.filter.VectorSearchSpec;
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

  // Cache the predicate evaluators
  private final List<Pair<Predicate, PredicateEvaluator>> _predicateEvaluators = new ArrayList<>(4);

  // Defensive copy of the upsert doc-ids snapshot, populated in run() only when the filter tree contains a
  // VECTOR_SIMILARITY predicate. Vector operators must restrict candidate generation to this bitmap so that
  // upsert-obsoleted rows never consume top-K candidate slots (the outer bitmap AND in run() only removes
  // them after the candidate budget has been spent).
  @Nullable
  private ImmutableRoaringBitmap _vectorRequiredDocIds;

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
    MutableRoaringBitmap docIdsSnapshot = _segmentContext.getDocIdsSnapshot();
    int numDocs = _indexSegment.getSegmentMetadata().getTotalDocs();

    if (docIdsSnapshot != null && docIdsSnapshot.isEmpty() && _filter != null
        && containsVectorSearchPredicate(_filter)) {
      // No queryable docs in this segment (e.g. every row is upsert-obsoleted): return an empty result without
      // invoking any ANN vector search. Non-vector queries keep the pre-existing plan shape (outer bitmap AND
      // with the empty snapshot) so their planning side effects are unchanged.
      return EmptyFilterOperator.getInstance();
    }

    if (_filter != null) {
      if (docIdsSnapshot != null && containsVectorSearchPredicate(_filter)) {
        // Vector operators iterate this bitmap during candidate generation (including on the asynchronous
        // searcher-pool hand-off of the mutable index), so they need an instance that is stable for the
        // query's lifetime and that this planner can safely mutate. The snapshot contract requires the
        // published bitmap to be quiescent at planning time (the built-in upsert metadata managers hand out
        // per-query clones or replace-only shared views); the clone insulates the operators from any
        // post-planning replacement and preserves the query-scoped semantics (skipUpsert / skipUpsertDelete
        // / tombstones) captured at snapshot time.
        MutableRoaringBitmap requiredDocIds = docIdsSnapshot.clone();
        // Clamp to the planned doc range: under upsert ConsistencyMode.NONE the snapshot can already
        // contain a doc id whose row data is still being written by the ingestion thread. The exact-scan
        // paths guard the bound while iterating, but filtered ANN (in particular the mutable index's
        // near-real-time reader) could otherwise surface such a doc to the rerank / threshold refinement,
        // which reads the forward index without a bound check.
        requiredDocIds.remove(numDocs, 1L << 32);
        _vectorRequiredDocIds = requiredDocIds;
      }
      BaseFilterOperator filterOperator = constructPhysicalOperator(_filter, numDocs);
      if (docIdsSnapshot != null) {
        // Keep the outer bitmap AND as defense in depth even though vector operators already restrict their
        // candidate generation to the snapshot. Use the same cloned instance the vector operators hold (when
        // one was taken) so both consumers observe an identical view of the snapshot.
        BaseFilterOperator validDocFilter = new BitmapBasedFilterOperator(
            _vectorRequiredDocIds != null ? _vectorRequiredDocIds : docIdsSnapshot, false, numDocs);
        return FilterOperatorUtils.getAndFilterOperator(_queryContext, Arrays.asList(filterOperator, validDocFilter),
            numDocs);
      } else {
        return filterOperator;
      }
    } else if (docIdsSnapshot != null) {
      return new BitmapBasedFilterOperator(docIdsSnapshot, false, numDocs);
    } else {
      return new MatchAllFilterOperator(numDocs);
    }
  }

  /// Returns true if the filter tree contains at least one vector search predicate (VECTOR_SIMILARITY or
  /// VECTOR_SIMILARITY_RADIUS) at any depth.
  ///
  /// For VECTOR_SIMILARITY the required doc-ids filter is a correctness constraint (obsolete rows must not
  /// consume the per-segment top-K candidate budget). For VECTOR_SIMILARITY_RADIUS results were already
  /// correct without it ([VectorRadiusFilterOperator] falls back to a complete brute-force scan when its ANN
  /// candidate pool saturates, and the outer snapshot AND removes obsolete matches), but restricting
  /// candidate generation avoids spending the candidate budget -- and triggering the expensive saturation
  /// fallback -- on upsert-obsoleted rows.
  private static boolean containsVectorSearchPredicate(FilterContext filter) {
    switch (filter.getType()) {
      case AND:
      case OR:
      case NOT:
        for (FilterContext child : filter.getChildren()) {
          if (containsVectorSearchPredicate(child)) {
            return true;
          }
        }
        return false;
      case PREDICATE:
        Predicate.Type predicateType = filter.getPredicate().getType();
        return predicateType == Predicate.Type.VECTOR_SIMILARITY
            || predicateType == Predicate.Type.VECTOR_SIMILARITY_RADIUS;
      default:
        return false;
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
        for (FilterContext childFilter : childFilters) {
          BaseFilterOperator childFilterOperator;
          if (isVectorSimilarityFilter(childFilter) && hasNonVectorSibling(childFilters)) {
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
          }
        }
        // Wire pre-filter bitmaps for filter-aware ANN: if an AND contains a
        // VectorSimilarityFilterOperator alongside other filter children, evaluate the
        // non-vector filters first and pass the resulting bitmap to the vector operator
        // so it can restrict HNSW graph traversal to the pre-filtered document set.
        wirePreFilterForVectorOperators(childFilterOperators, numDocs);
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
  /// 1. If the segment has a vector index for the column, and either no upsert doc-ids snapshot needs to be
  ///       enforced or the index can restrict candidate generation to it (implements
  ///       [FilterAwareVectorIndexReader]), use [VectorSimilarityFilterOperator] with query options
  ///       (nprobe, rerank, maxCandidates).
  /// 2. Otherwise fall back to [ExactVectorScanFilterOperator] which performs a brute-force scan of the
  ///       forward index -- over all docs when no snapshot is present, or over only the snapshot docs when one
  ///       is. This correctness-first fallback also applies when a vector index exists but cannot honor the
  ///       required doc-ids bitmap (any reader that is not filter-aware): unfiltered ANN would let
  ///       upsert-obsoleted rows consume the top-K candidate budget. All built-in readers, including the
  ///       mutable HNSW index, are filter-aware.
  ///
  /// @param hasMetadataFilter true if this vector predicate is combined with metadata filters (AND)
  private BaseFilterOperator constructVectorSimilarityOperator(DataSource dataSource,
      VectorSimilarityPredicate predicate, String column, int numDocs, boolean hasMetadataFilter) {
    VectorIndexReader vectorIndex = dataSource.getVectorIndex();
    VectorIndexConfig vectorIndexConfig = dataSource.getVectorIndexConfig();
    boolean isMutableSegment = _indexSegment.getSegmentMetadata().isMutableSegment();
    VectorSearchParams searchParams = VectorSearchParams.fromQueryOptions(_queryContext.getQueryOptions());
    ImmutableRoaringBitmap requiredDocIds = _vectorRequiredDocIds;
    VectorSearchSpec searchSpec = new VectorSearchSpec.Builder()
        .withSearchParams(searchParams)
        .withVectorIndexConfig(vectorIndexConfig)
        .withMetadataFilter(hasMetadataFilter)
        .withRequiredDocIds(requiredDocIds)
        .build();

    if (vectorIndex != null && canHonorRequiredDocIds(vectorIndex, requiredDocIds)) {
      // ANN index path: pass forward index reader if rerank or threshold search requires exact distances
      ForwardIndexReader<?> forwardIndexReader = null;
      VectorBackendType backendType = VectorDistanceUtils.resolveBackendType(vectorIndexConfig);
      if (searchParams.isExactRerank(backendType) || searchParams.hasDistanceThreshold()) {
        forwardIndexReader = dataSource.getForwardIndex();
        Preconditions.checkState(!searchParams.hasDistanceThreshold() || forwardIndexReader != null,
            "Cannot apply vectorDistanceThreshold on column: %s -- forward index required for threshold refinement",
            column);
      }
      return new VectorSimilarityFilterOperator(vectorIndex, predicate, numDocs, forwardIndexReader, searchSpec);
    }

    ForwardIndexReader<?> forwardIndexReader = dataSource.getForwardIndex();
    if (vectorIndex == null) {
      // Exact scan fallback: no vector index on this segment
      Preconditions.checkState(forwardIndexReader != null,
          "Cannot apply VECTOR_SIMILARITY on column: %s -- no vector index and no forward index available", column);
      return new ExactVectorScanFilterOperator(forwardIndexReader, predicate, column, numDocs,
          getVectorFallbackReason(vectorIndexConfig, isMutableSegment), searchSpec);
    }

    // Exact scan fallback: a vector index exists but cannot enforce the required upsert doc-ids bitmap.
    // Never silently run unfiltered ANN in this case -- fail instead when no forward index is available.
    Preconditions.checkState(forwardIndexReader != null,
        "Cannot enforce upsert-consistent VECTOR_SIMILARITY on column: %s -- the vector index does not support the"
            + " required document filter and no forward index is available for exact scan", column);
    return new ExactVectorScanFilterOperator(forwardIndexReader, predicate, column, numDocs,
        getRequiredDocIdsFallbackReason(isMutableSegment), searchSpec);
  }

  /// Returns true if no required doc-ids bitmap needs to be enforced, or the vector index can restrict its
  /// candidate generation to the required bitmap (implements [FilterAwareVectorIndexReader] and supports
  /// pre-filtering). Unlike the optional metadata pre-filter, this check deliberately bypasses the
  /// [VectorSearchStrategy] selectivity heuristics: the upsert snapshot is a correctness constraint, not an
  /// optimization.
  private static boolean canHonorRequiredDocIds(VectorIndexReader vectorIndex,
      @Nullable ImmutableRoaringBitmap requiredDocIds) {
    if (requiredDocIds == null) {
      return true;
    }
    return vectorIndex instanceof FilterAwareVectorIndexReader
        && ((FilterAwareVectorIndexReader) vectorIndex).supportsPreFilter();
  }

  private static String getRequiredDocIdsFallbackReason(boolean isMutableSegment) {
    return ExactVectorScanFilterOperator.UPSERT_SNAPSHOT_FALLBACK_REASON_PREFIX
        + (isMutableSegment ? "_mutable_vector_index_not_filter_aware" : "_vector_index_not_filter_aware");
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

  /// Returns true if the child list contains at least one non-VECTOR_SIMILARITY predicate
  /// (i.e., a real metadata filter sibling).
  private static boolean hasNonVectorSibling(List<FilterContext> childFilters) {
    for (FilterContext child : childFilters) {
      if (!isVectorSimilarityFilter(child)) {
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

  /// Constructs the vector radius filter operator based on index availability.
  ///
  /// The radius operator always needs the forward index for exact distance computation.
  /// When a vector index is available, it is used for candidate retrieval before exact filtering.
  /// The required doc-ids filter (upsert snapshot), when present, restricts every path inside the operator.
  private BaseFilterOperator constructVectorRadiusOperator(DataSource dataSource,
      VectorSimilarityRadiusPredicate predicate, String column, int numDocs) {
    ForwardIndexReader<?> forwardIndexReader = dataSource.getForwardIndex();
    Preconditions.checkState(forwardIndexReader != null,
        "Cannot apply VECTOR_SIMILARITY_RADIUS on column: %s -- no forward index available", column);
    VectorIndexReader vectorIndex = dataSource.getVectorIndex();
    VectorIndexConfig vectorIndexConfig = dataSource.getVectorIndexConfig();
    VectorSearchSpec searchSpec = new VectorSearchSpec.Builder()
        .withVectorIndexConfig(vectorIndexConfig)
        .withRequiredDocIds(_vectorRequiredDocIds)
        .build();
    return new VectorRadiusFilterOperator(forwardIndexReader, vectorIndex, predicate, column, numDocs, searchSpec);
  }

  /// Wires pre-filter bitmaps for filter-aware ANN search when an AND node contains both
  /// vector similarity operators and non-vector filter operators.
  ///
  /// When the vector index reader supports pre-filtering (implements
  /// [org.apache.pinot.segment.spi.index.reader.FilterAwareVectorIndexReader]), the non-vector
  /// siblings are evaluated eagerly to produce a combined bitmap. This bitmap is passed to the
  /// [VectorSimilarityFilterOperator] so that the HNSW graph traversal is restricted to
  /// pre-filtered documents, improving recall for selective filters.
  ///
  /// **Trade-off: eager filter evaluation.** The non-vector filter predicates are materialized
  /// into bitmaps before the vector search begins. This is intentional because the filter bitmap must
  /// be fully materialized before it can be passed to the vector index for pre-filtered ANN search.
  /// The [VectorSearchStrategy] selectivity check below ensures we only pay this cost when the
  /// estimated cardinality suggests pre-filtering is worthwhile.
  ///
  /// If no vector operators are found or the reader does not support pre-filtering,
  /// this method is a no-op and the AND operator falls back to the default post-filter path.
  ///
  /// @param childOperators the list of child filter operators under an AND node
  /// @param numDocs total documents in the segment
  private void wirePreFilterForVectorOperators(List<BaseFilterOperator> childOperators, int numDocs) {
    if (childOperators.size() < 2) {
      return;
    }

    // Find vector similarity operators that support pre-filtering
    List<VectorSimilarityFilterOperator> vectorOps = new ArrayList<>();
    List<BaseFilterOperator> nonVectorOps = new ArrayList<>();
    for (BaseFilterOperator op : childOperators) {
      if (op instanceof VectorSimilarityFilterOperator) {
        vectorOps.add((VectorSimilarityFilterOperator) op);
      } else {
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
    // Note: this eagerly evaluates non-vector filters. BaseFilterOperator subclasses cache
    // their results, so the subsequent evaluation by AndFilterOperator will reuse the cached
    // bitmaps without double-evaluation.
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
    // Pass the real segment mutability: MutableVectorIndex is filter-aware (so mutable segments can reach
    // this point), but the strategy deliberately stays conservative about OPTIONAL pre-filtering on mutable
    // segments because their filtered search opens a near-real-time reader per query. The REQUIRED upsert
    // doc-ids filter is unaffected -- it bypasses this strategy entirely.
    // backendType and searchParams are passed as null here because at the pre-filter wiring
    // stage we are deciding whether to activate pre-filtering at all, not per-backend tuning.
    // The strategy currently uses only selectivity (numDocs, estimatedFilteredDocs) for this
    // decision. Per-backend and per-query-option tuning is handled later inside the operator.
    VectorSearchStrategy.Decision decision = VectorSearchStrategy.decide(
        numDocs, estimatedFilteredDocs,
        /* hasVectorIndex= */ true,
        /* indexSupportsPreFilter= */ true,
        _indexSegment.getSegmentMetadata().isMutableSegment(),
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
