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
package org.apache.pinot.core.operator.filter;

import com.google.common.base.CaseFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.predicate.VectorSimilarityPredicate;
import org.apache.pinot.core.common.BlockDocIdSet;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.ExplainAttributeBuilder;
import org.apache.pinot.core.operator.docidsets.BitmapDocIdSet;
import org.apache.pinot.segment.spi.index.creator.VectorBackendType;
import org.apache.pinot.segment.spi.index.creator.VectorExecutionMode;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;
import org.apache.pinot.segment.spi.index.reader.EfSearchAware;
import org.apache.pinot.segment.spi.index.reader.FilterAwareVectorIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.index.reader.NprobeAware;
import org.apache.pinot.segment.spi.index.reader.VectorIndexReader;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.trace.FilterType;
import org.apache.pinot.spi.trace.InvocationRecording;
import org.apache.pinot.spi.trace.Tracing;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Operator for vector similarity search using an ANN index (HNSW, IVF_FLAT, IVF_PQ, or IVF_ON_DISK).
///
/// This operator supports backend-neutral vector search with the following capabilities:
///
/// - **nprobe dispatch:** If the underlying reader implements [NprobeAware], the
///      `vectorNprobe` query option is applied before search.
/// - **Exact rerank:** When `vectorExactRerank=true`, ANN candidates are re-scored
///      using exact distance from the forward index and re-sorted before final top-K selection.
/// - **maxCandidates:** Controls how many ANN candidates are retrieved before rerank. Only
///      meaningful when rerank is enabled.
/// - **Pre-filter:** For backends that implement FilterAwareVectorIndexReader, an optimizer-selected metadata bitmap
///      can improve search quality under highly selective filters. FULL-upsert queries additionally apply their
///      query-scoped valid-document snapshot before candidate selection.
///
/// When no query options are specified, behavior is identical to the previous HNSW-only path
/// (full backward compatibility).
///
/// This class is NOT thread-safe. Each operator instance is used by a single query thread.
public class VectorSimilarityFilterOperator extends BaseFilterOperator {
  private static final Logger LOGGER = LoggerFactory.getLogger(VectorSimilarityFilterOperator.class);
  private static final String EXPLAIN_NAME = "VECTOR_SIMILARITY_INDEX";

  private final VectorIndexReader _vectorIndexReader;
  private final VectorSimilarityPredicate _predicate;
  private final VectorSearchParams _searchParams;
  private final ForwardIndexReader<?> _forwardIndexReader;
  @Nullable
  private final VectorIndexConfig _vectorIndexConfig;
  private final VectorBackendType _backendType;
  private final VectorIndexConfig.VectorDistanceFunction _distanceFunction;
  private final boolean _requestedExactRerank;
  private final boolean _effectiveExactRerank;
  private final boolean _hasMetadataFilter;
  private final boolean _hasThresholdPredicate;
  private final float _distanceThreshold;
  private final int _effectiveSearchCount;
  @Nullable
  private final ImmutableRoaringBitmap _requiredUpsertCandidateBitmap;
  @Nullable
  private final String _requiredCandidateFallbackReason;
  private volatile VectorExplainContext _vectorExplainContext;
  private volatile int _annCandidateCount;
  private volatile int _rerankedCandidateCount;
  private ImmutableRoaringBitmap _matches;
  @Nullable
  private volatile ImmutableRoaringBitmap _optionalPreFilterBitmap;
  @Nullable
  private volatile ImmutableRoaringBitmap _effectiveAllowedDocIds;
  private volatile VectorSearchMode _vectorSearchMode;
  @Nullable
  private volatile String _runtimeFallbackReason;
  private volatile boolean _candidateGenerationSkipped;

  /// Backward-compatible constructor that uses default search params and no forward index.
  /// Existing callers that do not pass query options continue to work unchanged.
  public VectorSimilarityFilterOperator(VectorIndexReader vectorIndexReader, VectorSimilarityPredicate predicate,
      int numDocs) {
    this(vectorIndexReader, predicate, numDocs, VectorSearchParams.DEFAULT, null, null, false);
  }

  /// Full constructor with query option support.
  ///
  /// @param vectorIndexReader the ANN index reader
  /// @param predicate the vector similarity predicate
  /// @param numDocs total docs in the segment
  /// @param searchParams vector search parameters from query options
  /// @param forwardIndexReader forward index reader for exact rerank (may be null if rerank is not needed)
  public VectorSimilarityFilterOperator(VectorIndexReader vectorIndexReader, VectorSimilarityPredicate predicate,
      int numDocs, VectorSearchParams searchParams, @Nullable ForwardIndexReader<?> forwardIndexReader) {
    this(vectorIndexReader, predicate, numDocs, searchParams, forwardIndexReader, null, false);
  }

  public VectorSimilarityFilterOperator(VectorIndexReader vectorIndexReader, VectorSimilarityPredicate predicate,
      int numDocs, VectorSearchParams searchParams, @Nullable ForwardIndexReader<?> forwardIndexReader,
      @Nullable VectorIndexConfig vectorIndexConfig) {
    this(vectorIndexReader, predicate, numDocs, searchParams, forwardIndexReader, vectorIndexConfig, false);
  }

  /// Full constructor with metadata filter awareness.
  ///
  /// @param vectorIndexReader the ANN index reader
  /// @param predicate the vector similarity predicate
  /// @param numDocs total docs in the segment
  /// @param searchParams vector search parameters from query options
  /// @param forwardIndexReader forward index reader for exact rerank (may be null if rerank is not needed)
  /// @param vectorIndexConfig vector index configuration (may be null)
  /// @param hasMetadataFilter true if this operator is combined with metadata filters in an AND
  public VectorSimilarityFilterOperator(VectorIndexReader vectorIndexReader, VectorSimilarityPredicate predicate,
      int numDocs, VectorSearchParams searchParams, @Nullable ForwardIndexReader<?> forwardIndexReader,
      @Nullable VectorIndexConfig vectorIndexConfig, boolean hasMetadataFilter) {
    this(vectorIndexReader, predicate, numDocs, searchParams, forwardIndexReader, vectorIndexConfig,
        hasMetadataFilter, null);
  }

  /// Full constructor with a mandatory query-owned scope and optional optimizer-selected metadata.
  public VectorSimilarityFilterOperator(VectorIndexReader vectorIndexReader, VectorSimilarityPredicate predicate,
      int numDocs, VectorSearchParams searchParams, @Nullable ForwardIndexReader<?> forwardIndexReader,
      @Nullable VectorIndexConfig vectorIndexConfig, boolean hasMetadataFilter,
      @Nullable VectorCandidateScope candidateScope) {
    super(numDocs, false);
    _vectorIndexReader = vectorIndexReader;
    _predicate = predicate;
    _searchParams = searchParams;
    _forwardIndexReader = forwardIndexReader;
    _vectorIndexConfig = vectorIndexConfig;
    _backendType = VectorDistanceUtils.resolveBackendType(vectorIndexConfig);
    _distanceFunction = VectorDistanceUtils.resolveDistanceFunction(vectorIndexConfig);
    _requestedExactRerank = searchParams.isExactRerank(_backendType);
    _effectiveExactRerank = _requestedExactRerank && forwardIndexReader != null;
    _hasMetadataFilter = hasMetadataFilter;
    _hasThresholdPredicate = searchParams.hasDistanceThreshold();
    _distanceThreshold = searchParams.getDistanceThreshold();
    _requiredUpsertCandidateBitmap = candidateScope != null ? candidateScope.getRequiredDocIds() : null;
    _requiredCandidateFallbackReason = candidateScope != null ? candidateScope.getFallbackReason() : null;
    // Threshold and exact-rerank queries use a larger candidate pool for refinement.
    int baseSearchCount;
    if (_hasThresholdPredicate) {
      // Threshold queries need a larger candidate pool for exact distance refinement.
      // Honor explicit vectorMaxCandidates; otherwise default to topK * 10.
      baseSearchCount = searchParams.getEffectiveMaxCandidates(predicate.getTopK(), numDocs);
    } else if (_effectiveExactRerank) {
      baseSearchCount = searchParams.getEffectiveMaxCandidates(predicate.getTopK(), numDocs);
    } else {
      // For plain top-K and filtered queries (no rerank, no threshold), always ask
      // the ANN index for exactly topK candidates. Over-fetching would change the
      // predicate semantics: vectorSimilarity(col, q, 10) must return at most 10 docs.
      // Adaptive metadata that remains post-filtered may reduce this set further. Required upsert candidates are
      // instead applied before this top-K selection.
      baseSearchCount = predicate.getTopK();
    }
    _effectiveSearchCount = baseSearchCount;
    _annCandidateCount = -1;
    _rerankedCandidateCount = -1;
    _matches = null;
    _optionalPreFilterBitmap = null;
    recomputeEffectiveAllowedDocIds();
    if (!hasMandatoryCandidateScope()) {
      _vectorSearchMode = VectorSearchMode.POST_FILTER_ANN;
      _runtimeFallbackReason = null;
    } else if (_effectiveAllowedDocIds.isEmpty()) {
      _vectorSearchMode = VectorSearchMode.FILTER_THEN_ANN;
      _runtimeFallbackReason = null;
    } else if (readerSupportsPreFilter()) {
      _vectorSearchMode = VectorSearchMode.FILTER_THEN_ANN;
      _runtimeFallbackReason = null;
    } else {
      _vectorSearchMode = VectorSearchMode.EXACT_SCAN;
      _runtimeFallbackReason = _requiredCandidateFallbackReason;
    }
    _candidateGenerationSkipped = hasMandatoryCandidateScope() && _effectiveAllowedDocIds.isEmpty();
    refreshExplainContext();
  }

  /// Sets an optimizer-selected optional metadata bitmap. When a mandatory upsert bitmap is also present, the
  /// operator intersects private copies of the two bitmaps before candidate generation. When no mandatory bitmap is
  /// present, readers that cannot pre-filter retain the existing unfiltered ANN behavior.
  ///
  /// This method must be called before any search execution (getTrues, getBitmaps, etc.).
  ///
  /// @param preFilterBitmap the bitmap of document IDs to restrict the search to
  public void setPreFilterBitmap(@Nullable ImmutableRoaringBitmap preFilterBitmap) {
    _optionalPreFilterBitmap = copyBitmap(preFilterBitmap);
    recomputeEffectiveAllowedDocIds();
    _candidateGenerationSkipped = hasMandatoryCandidateScope() && _effectiveAllowedDocIds.isEmpty();
    refreshExplainContext();
  }

  @Override
  protected BlockDocIdSet getTrues() {
    if (_matches == null) {
      _matches = executeSearch();
    }
    return new BitmapDocIdSet(_matches, _numDocs);
  }

  @Override
  public int getNumMatchingDocs() {
    if (_matches == null) {
      _matches = executeSearch();
    }
    return _matches.getCardinality();
  }

  @Override
  public boolean canProduceBitmaps() {
    return true;
  }

  @Override
  public BitmapCollection getBitmaps() {
    if (_matches == null) {
      _matches = executeSearch();
    }
    record(_matches);
    return new BitmapCollection(_numDocs, false, _matches);
  }

  @Override
  public List<Operator> getChildOperators() {
    return List.of();
  }

  @Override
  public String toExplainString() {
    VectorExplainContext explainContext = _vectorExplainContext;
    StringBuilder sb = new StringBuilder();
    sb.append(EXPLAIN_NAME).append("(indexLookUp:vector_index")
        .append(", operator:").append(_predicate.getType())
        .append(", executionMode:").append(explainContext.getExecutionMode() != null
            ? explainContext.getExecutionMode() : "UNKNOWN")
        .append(", vector identifier:").append(_predicate.getLhs().getIdentifier())
        .append(", backend:").append(explainContext.getBackendType())
        .append(", distanceFunction:").append(explainContext.getDistanceFunction())
        .append(", effectiveNprobe:").append(explainContext.getEffectiveNprobe())
        .append(", effectiveEfSearch:").append(explainContext.getEffectiveEfSearch())
        .append(", effectiveExactRerank:").append(explainContext.isEffectiveExactRerank())
        .append(", effectiveCandidateCount:").append(explainContext.getEffectiveSearchCount())
        .append(", vector literal:").append(Arrays.toString(_predicate.getValue()))
        .append(", topK to search:").append(_predicate.getTopK());
    if (explainContext.getEffectiveHnswUseRelativeDistance() != null) {
      sb.append(", effectiveHnswUseRelativeDistance:").append(explainContext.getEffectiveHnswUseRelativeDistance());
    }
    if (explainContext.getEffectiveHnswUseBoundedQueue() != null) {
      sb.append(", effectiveHnswUseBoundedQueue:").append(explainContext.getEffectiveHnswUseBoundedQueue());
    }
    if (explainContext.getEffectiveThreshold() >= 0) {
      sb.append(", effectiveThreshold:").append(explainContext.getEffectiveThreshold());
    }
    sb.append(", searchMode:").append(_vectorSearchMode);
    if (explainContext.getFilterSelectivity() >= 0) {
      sb.append(", filterSelectivity:").append(String.format("%.4f", explainContext.getFilterSelectivity()));
    }
    sb.append(", upsertCandidateFilterApplied:").append(_requiredUpsertCandidateBitmap != null)
        .append(", upsertCandidateFilterCardinality:").append(getRequiredUpsertCandidateCardinality())
        .append(", effectiveAllowedDocIdsCardinality:").append(getEffectiveAllowedDocIdsCardinality())
        .append(", candidateGenerationSkipped:").append(_candidateGenerationSkipped);
    if (_candidateGenerationSkipped) {
      sb.append(", candidateGenerationSkipReason:").append(getCandidateGenerationSkipReason());
    }
    if (_runtimeFallbackReason != null) {
      sb.append(", fallbackReason:").append(_runtimeFallbackReason);
    }
    sb.append(')');
    return sb.toString();
  }

  @Override
  protected String getExplainName() {
    return CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, EXPLAIN_NAME);
  }

  @Override
  protected void explainAttributes(ExplainAttributeBuilder attributeBuilder) {
    VectorExplainContext explainContext = _vectorExplainContext;
    super.explainAttributes(attributeBuilder);
    attributeBuilder.putString("indexLookUp", "vector_index");
    attributeBuilder.putString("operator", _predicate.getType().name());
    if (explainContext.getExecutionMode() != null) {
      attributeBuilder.putString("executionMode", explainContext.getExecutionMode().name());
    }
    attributeBuilder.putString("vectorIdentifier", _predicate.getLhs().getIdentifier());
    attributeBuilder.putString("backend", explainContext.getBackendType().name());
    attributeBuilder.putString("distanceFunction", explainContext.getDistanceFunction().name());
    attributeBuilder.putLongIdempotent("effectiveNprobe", explainContext.getEffectiveNprobe());
    attributeBuilder.putLongIdempotent("effectiveEfSearch", explainContext.getEffectiveEfSearch());
    attributeBuilder.putBool("effectiveExactRerank", explainContext.isEffectiveExactRerank());
    attributeBuilder.putLongIdempotent("effectiveCandidateCount", explainContext.getEffectiveSearchCount());
    if (explainContext.getEffectiveHnswUseRelativeDistance() != null) {
      attributeBuilder.putBool("effectiveHnswUseRelativeDistance",
          explainContext.getEffectiveHnswUseRelativeDistance());
    }
    if (explainContext.getEffectiveHnswUseBoundedQueue() != null) {
      attributeBuilder.putBool("effectiveHnswUseBoundedQueue",
          explainContext.getEffectiveHnswUseBoundedQueue());
    }
    if (explainContext.getEffectiveThreshold() >= 0) {
      attributeBuilder.putString("effectiveThreshold", String.valueOf(explainContext.getEffectiveThreshold()));
    }
    attributeBuilder.putString("vectorLiteral", Arrays.toString(_predicate.getValue()));
    attributeBuilder.putLongIdempotent("topKtoSearch", _predicate.getTopK());
    if (_annCandidateCount >= 0) {
      attributeBuilder.putLong("annCandidateCount", _annCandidateCount);
    }
    if (_rerankedCandidateCount >= 0) {
      attributeBuilder.putLong("rerankedCandidateCount", _rerankedCandidateCount);
    }
    attributeBuilder.putString("searchMode", _vectorSearchMode.name());
    if (explainContext.getFilterSelectivity() >= 0) {
      attributeBuilder.putString("filterSelectivity", String.format("%.4f", explainContext.getFilterSelectivity()));
    }
    attributeBuilder.putBool("upsertCandidateFilterApplied", _requiredUpsertCandidateBitmap != null);
    attributeBuilder.putLongIdempotent("upsertCandidateFilterCardinality", getRequiredUpsertCandidateCardinality());
    attributeBuilder.putLongIdempotent("effectiveAllowedDocIdsCardinality",
        getEffectiveAllowedDocIdsCardinality());
    attributeBuilder.putBool("candidateGenerationSkipped", _candidateGenerationSkipped);
    if (_candidateGenerationSkipped) {
      attributeBuilder.putString("candidateGenerationSkipReason", getCandidateGenerationSkipReason());
    }
    if (_runtimeFallbackReason != null) {
      attributeBuilder.putString("fallbackReason", _runtimeFallbackReason);
    }
  }

  /// Returns true if the underlying vector index reader supports pre-filter ANN search.
  public boolean supportsPreFilter() {
    return readerSupportsPreFilter();
  }

  /// Executes the vector search with backend-specific parameter dispatch and optional rerank.
  private ImmutableRoaringBitmap executeSearch() {
    String column = _predicate.getLhs().getIdentifier();
    float[] queryVector = _predicate.getValue();
    VectorExplainContext explainContext = _vectorExplainContext;
    boolean backendParamsNeedCleanup = false;
    boolean searchExecuted = false;
    try {
      ImmutableRoaringBitmap effectiveAllowedDocIds = _effectiveAllowedDocIds;
      if (hasMandatoryCandidateScope() && effectiveAllowedDocIds.isEmpty()) {
        _candidateGenerationSkipped = true;
        _annCandidateCount = 0;
        _rerankedCandidateCount = 0;
        return new MutableRoaringBitmap();
      }
      _candidateGenerationSkipped = false;

      if (hasMandatoryCandidateScope() && !readerSupportsPreFilter()) {
        if (_forwardIndexReader == null) {
          throw new IllegalStateException("Cannot honor mandatory vector candidate scope on vector column: "
              + column + " -- vector index reader does not support filtered search and no forward index is available");
        }
        _vectorSearchMode = VectorSearchMode.EXACT_SCAN;
        _runtimeFallbackReason = _requiredCandidateFallbackReason != null ? _requiredCandidateFallbackReason
            : "vector_index_not_filter_aware_for_mandatory_scope";
        VectorSearchMetrics.getInstance().recordFallback(_runtimeFallbackReason);
        LOGGER.warn("Performing exact allowed-document vector scan on column: {} because {}. allowedDocs={}",
            column, _runtimeFallbackReason, effectiveAllowedDocIds.getCardinality());
        Float threshold = _hasThresholdPredicate ? _distanceThreshold : null;
        searchExecuted = true;
        ImmutableRoaringBitmap exactResults = ExactVectorScanFilterOperator.computeExactMatches(
            _forwardIndexReader, queryVector, _predicate.getTopK(), _numDocs, _distanceFunction, threshold,
            effectiveAllowedDocIds, column);
        _annCandidateCount = -1;
        _rerankedCandidateCount = exactResults.getCardinality();
        return exactResults;
      }

      // 1. Configure backend-specific parameters via interfaces
      // Claim cleanup before the first setter because configuration can fail after partially updating reader state.
      backendParamsNeedCleanup = true;
      configureBackendParams(column);
      refreshExplainContext();
      explainContext = _vectorExplainContext;

      // 2. Determine effective search count (higher if rerank is enabled)
      int searchCount = explainContext.getEffectiveSearchCount();

      // 3. Execute ANN search (with pre-filter if available)
      ImmutableRoaringBitmap preFilter = effectiveAllowedDocIds;
      ImmutableRoaringBitmap annResults;
      searchExecuted = true;
      if (preFilter != null && _vectorIndexReader instanceof FilterAwareVectorIndexReader) {
        FilterAwareVectorIndexReader filterAwareReader = (FilterAwareVectorIndexReader) _vectorIndexReader;
        if (filterAwareReader.supportsPreFilter()) {
          _vectorSearchMode = VectorSearchMode.FILTER_THEN_ANN;
          annResults = filterAwareReader.getDocIds(queryVector, searchCount, preFilter);
          LOGGER.debug("Pre-filter ANN search on column: {}, filterCardinality: {}, filterSelectivity: {}",
              column, preFilter.getCardinality(),
              _numDocs > 0 ? (double) preFilter.getCardinality() / _numDocs : 0.0);
        } else {
          if (hasMandatoryCandidateScope()) {
            throw new IllegalStateException("Cannot honor mandatory vector candidate scope on vector column: "
                + column + " -- vector index reader stopped supporting filtered search");
          }
          _vectorSearchMode = VectorSearchMode.POST_FILTER_ANN;
          annResults = _vectorIndexReader.getDocIds(queryVector, searchCount);
        }
      } else {
        _vectorSearchMode = VectorSearchMode.POST_FILTER_ANN;
        annResults = _vectorIndexReader.getDocIds(queryVector, searchCount);
      }
      int annCandidateCount = annResults.getCardinality();
      _annCandidateCount = annCandidateCount;

      LOGGER.debug("Vector search on column: {}, backend: {}, distanceFunction: {}, topK: {}, searchCount: {}, "
              + "annCandidates: {}, exactRerank: {}, searchMode: {}",
          column, explainContext.getBackendType(), explainContext.getDistanceFunction(), _predicate.getTopK(),
          searchCount, annCandidateCount, explainContext.isEffectiveExactRerank(), _vectorSearchMode);

      if (_requestedExactRerank && !explainContext.isEffectiveExactRerank()) {
        LOGGER.warn("Vector exact rerank was requested on column: {} but no forward index reader is available. "
                + "Using ANN topK results only.",
            column);
      }

      // 4. Apply exact rerank if requested (handles both rerank-only and rerank+threshold cases).
      //    When exact rerank is enabled, it subsumes the threshold check: the rerank step scores
      //    candidates exactly and can apply the threshold simultaneously.
      if (_hasThresholdPredicate && _forwardIndexReader == null) {
        throw new IllegalStateException(
            "Vector distance threshold was requested on column: " + column
                + " but no forward index reader is available to apply threshold refinement");
      }
      if (explainContext.isEffectiveExactRerank() && _forwardIndexReader != null && annCandidateCount > 0) {
        Float threshold = _hasThresholdPredicate ? _distanceThreshold : null;
        ImmutableRoaringBitmap reranked = applyExactRerank(annResults, queryVector, _predicate.getTopK(), column,
            threshold);
        _rerankedCandidateCount = reranked.getCardinality();
        LOGGER.debug("Exact rerank on column: {}, candidates: {} -> final: {} (threshold={})",
            column, annCandidateCount, reranked.getCardinality(), threshold);
        return reranked;
      }

      // 5. Apply threshold filter (approximate: only ANN candidates are checked).
      // NOTE: Vectors within the threshold but outside the ANN candidate pool will be missed.
      // For exact threshold search, use a table without a vector index or enable vectorExactRerank=true.
      // Increase vectorMaxCandidates to improve recall at the cost of latency.
      if (_hasThresholdPredicate && annCandidateCount > 0) {
        ImmutableRoaringBitmap thresholded = applyThresholdFilter(
            annResults, queryVector, _distanceThreshold, column);
        _rerankedCandidateCount = thresholded.getCardinality();
        LOGGER.debug("Approximate threshold refinement on column: {}, threshold: {}, "
                + "annCandidates: {} -> thresholded: {} (recall limited by candidate pool size)",
            column, _distanceThreshold, annCandidateCount, thresholded.getCardinality());
        return thresholded;
      }

      return annResults;
    } finally {
      try {
        if (searchExecuted) {
          VectorSearchMetrics.getInstance().recordSearch(_vectorSearchMode, _backendType);
        }
        // Refresh explain context with the final search mode decided during execution
        refreshExplainContext();
      } finally {
        if (backendParamsNeedCleanup) {
          clearBackendParams(column);
        }
      }
    }
  }

  /// Configures backend-specific search parameters on the reader if it supports them.
  private void configureBackendParams(String column) {
    if (_vectorIndexReader instanceof NprobeAware) {
      int nprobe = _searchParams.getNprobe();
      ((NprobeAware) _vectorIndexReader).setNprobe(nprobe);
      LOGGER.debug("Set nprobe={} on {} reader for column: {}", nprobe, getBackendName(), column);
    }
    if (_vectorIndexReader instanceof EfSearchAware) {
      EfSearchAware efSearchAware = (EfSearchAware) _vectorIndexReader;
      Integer efSearch = _searchParams.getEfSearch();
      if (efSearch != null) {
        efSearchAware.setEfSearch(efSearch);
        VectorSearchMetrics.getInstance().recordEfSearchOverride();
      }
      Boolean useRelativeDistance = _searchParams.getHnswUseRelativeDistance();
      if (useRelativeDistance != null) {
        efSearchAware.setUseRelativeDistance(useRelativeDistance);
      }
      Boolean useBoundedQueue = _searchParams.getHnswUseBoundedQueue();
      if (useBoundedQueue != null) {
        efSearchAware.setUseBoundedQueue(useBoundedQueue);
      }
    }
  }

  private void clearBackendParams(String column) {
    if (_vectorIndexReader instanceof NprobeAware) {
      ((NprobeAware) _vectorIndexReader).clearNprobe();
      LOGGER.debug("Cleared nprobe on {} reader for column: {}", getBackendName(), column);
    }
    if (_vectorIndexReader instanceof EfSearchAware) {
      EfSearchAware efSearchAware = (EfSearchAware) _vectorIndexReader;
      efSearchAware.clearEfSearch();
      efSearchAware.clearUseRelativeDistance();
      efSearchAware.clearUseBoundedQueue();
      LOGGER.debug("Cleared efSearch on {} reader for column: {}", getBackendName(), column);
    }
  }

  /// Applies exact distance threshold refinement to ANN candidates.
  /// Returns only candidates whose exact distance is within the threshold.
  @SuppressWarnings("unchecked")
  private ImmutableRoaringBitmap applyThresholdFilter(ImmutableRoaringBitmap annResults, float[] queryVector,
      float threshold, String column) {
    MutableRoaringBitmap result = new MutableRoaringBitmap();
    VectorExplainContext explainContext = _vectorExplainContext;
    ForwardIndexReader rawReader = _forwardIndexReader;
    try (ForwardIndexReaderContext context = rawReader.createContext()) {
      org.roaringbitmap.IntIterator it = annResults.getIntIterator();
      while (it.hasNext()) {
        int docId = it.next();
        float[] docVector = rawReader.getFloatMV(docId, context);
        if (docVector == null || docVector.length == 0) {
          continue;
        }
        float distance = VectorDistanceUtils.computeDistance(queryVector, docVector,
            explainContext.getDistanceFunction());
        if (distance <= threshold) {
          result.add(docId);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Error during threshold refinement on column: " + column, e);
    }
    return result;
  }

  /// Re-scores ANN candidates using exact distance from the forward index and returns top-K.
  /// If a distance threshold is provided, additionally filters out candidates beyond the threshold.
  ///
  /// @param annResults the ANN candidate bitmap
  /// @param queryVector the query vector
  /// @param topK the number of results to return
  /// @param column the column name (for error messages)
  /// @param threshold optional distance threshold; if non-null, only candidates within this distance are kept
  @SuppressWarnings("unchecked")
  private ImmutableRoaringBitmap applyExactRerank(ImmutableRoaringBitmap annResults, float[] queryVector,
      int topK, String column, @Nullable Float threshold) {
    // Max-heap: largest distance on top for efficient eviction
    PriorityQueue<DocDistance> maxHeap = new PriorityQueue<>(topK + 1,
        (a, b) -> Float.compare(b._distance, a._distance));

    ForwardIndexReader rawReader = _forwardIndexReader;
    try (ForwardIndexReaderContext context = rawReader.createContext()) {
      org.roaringbitmap.IntIterator it = annResults.getIntIterator();
      while (it.hasNext()) {
        int docId = it.next();
        float[] docVector = rawReader.getFloatMV(docId, context);
        if (docVector == null || docVector.length == 0) {
          continue;
        }
        float distance = VectorDistanceUtils.computeDistance(queryVector, docVector,
            _vectorExplainContext.getDistanceFunction());
        // Apply vectorThreshold cutoff if specified
        if (threshold != null && distance > threshold) {
          continue;
        }
        if (maxHeap.size() < topK) {
          maxHeap.add(new DocDistance(docId, distance));
        } else if (distance < maxHeap.peek()._distance) {
          maxHeap.poll();
          maxHeap.add(new DocDistance(docId, distance));
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Error during exact rerank on column: " + column, e);
    }

    MutableRoaringBitmap result = new MutableRoaringBitmap();
    for (DocDistance dd : maxHeap) {
      result.add(dd._docId);
    }
    return result;
  }


  /// Returns a human-readable name for the backend (for logging).
  private String getBackendName() {
    return _backendType.name();
  }

  private void refreshExplainContext() {
    VectorExecutionMode executionMode;
    if (_vectorSearchMode == VectorSearchMode.EXACT_SCAN) {
      executionMode = VectorExecutionMode.EXACT_SCAN;
    } else if (_vectorSearchMode == VectorSearchMode.FILTER_THEN_ANN) {
      executionMode = VectorExecutionMode.FILTER_THEN_ANN;
    } else {
      executionMode = VectorQueryExecutionContext.selectExecutionMode(
          true, _hasMetadataFilter, _hasThresholdPredicate, _effectiveExactRerank);
    }
    ImmutableRoaringBitmap preFilter = _effectiveAllowedDocIds;
    double filterSelectivity = (preFilter != null && _numDocs > 0)
        ? (double) preFilter.getCardinality() / _numDocs : -1.0;
    Map<String, Object> indexDebugInfo = !_candidateGenerationSkipped && _backendType.supportsNprobe()
        ? _vectorIndexReader.getIndexDebugInfo() : Map.of();
    int effectiveEfSearch =
        resolveEffectiveEfSearch(_backendType, _searchParams);
    Boolean effectiveHnswUseRelativeDistance =
        resolveEffectiveHnswUseRelativeDistance(_backendType, _searchParams);
    Boolean effectiveHnswUseBoundedQueue =
        resolveEffectiveHnswUseBoundedQueue(_backendType, _searchParams);
    Float threshold = _hasThresholdPredicate ? _distanceThreshold : null;
    float effectiveThreshold = threshold != null ? threshold : -1f;
    _vectorExplainContext = new VectorExplainContext(_backendType, _distanceFunction, executionMode,
        resolveEffectiveNprobe(_backendType, _searchParams, indexDebugInfo),
        _effectiveExactRerank, _effectiveSearchCount, _runtimeFallbackReason, null,
        effectiveEfSearch, effectiveThreshold, _vectorSearchMode, filterSelectivity,
        effectiveHnswUseRelativeDistance, effectiveHnswUseBoundedQueue);
  }

  private boolean readerSupportsPreFilter() {
    return _vectorIndexReader instanceof FilterAwareVectorIndexReader
        && ((FilterAwareVectorIndexReader) _vectorIndexReader).supportsPreFilter();
  }

  private boolean hasMandatoryCandidateScope() {
    return _requiredUpsertCandidateBitmap != null;
  }

  private void recomputeEffectiveAllowedDocIds() {
    if (_requiredUpsertCandidateBitmap == null) {
      _effectiveAllowedDocIds = _optionalPreFilterBitmap;
      return;
    }
    if (_optionalPreFilterBitmap == null) {
      _effectiveAllowedDocIds = _requiredUpsertCandidateBitmap;
      return;
    }
    MutableRoaringBitmap effective = _requiredUpsertCandidateBitmap.toMutableRoaringBitmap();
    effective.and(_optionalPreFilterBitmap);
    _effectiveAllowedDocIds = effective.toImmutableRoaringBitmap();
  }

  private int getRequiredUpsertCandidateCardinality() {
    return _requiredUpsertCandidateBitmap != null ? _requiredUpsertCandidateBitmap.getCardinality() : -1;
  }

  private int getEffectiveAllowedDocIdsCardinality() {
    return _effectiveAllowedDocIds != null ? _effectiveAllowedDocIds.getCardinality() : -1;
  }

  private String getCandidateGenerationSkipReason() {
    if (_requiredUpsertCandidateBitmap != null && _requiredUpsertCandidateBitmap.isEmpty()) {
      return "empty_upsert_candidate_filter";
    }
    return "empty_effective_candidate_filter";
  }

  @Nullable
  private static ImmutableRoaringBitmap copyBitmap(@Nullable ImmutableRoaringBitmap bitmap) {
    if (bitmap == null) {
      return null;
    }
    return bitmap.toMutableRoaringBitmap().toImmutableRoaringBitmap();
  }

  private static int resolveEffectiveNprobe(VectorBackendType backendType, VectorSearchParams searchParams,
      Map<String, Object> indexDebugInfo) {
    if (!backendType.supportsNprobe()) {
      return 0;
    }

    Integer nlist = parseInteger(indexDebugInfo.get("nlist"));
    if (nlist != null) {
      return Math.min(searchParams.getNprobe(), Math.max(nlist, 0));
    }

    Integer effectiveNprobe = parseInteger(indexDebugInfo.get("effectiveNprobe"));
    return effectiveNprobe != null ? effectiveNprobe : searchParams.getNprobe();
  }

  private static int resolveEffectiveEfSearch(VectorBackendType backendType, VectorSearchParams searchParams) {
    if (backendType != VectorBackendType.HNSW) {
      return 0;
    }
    Integer efSearch = searchParams.getEfSearch();
    return efSearch != null ? efSearch : 0;
  }

  @Nullable
  private static Boolean resolveEffectiveHnswUseRelativeDistance(VectorBackendType backendType,
      VectorSearchParams searchParams) {
    if (backendType != VectorBackendType.HNSW) {
      return null;
    }
    Boolean value = searchParams.getHnswUseRelativeDistance();
    return value != null ? value : Boolean.TRUE;
  }

  @Nullable
  private static Boolean resolveEffectiveHnswUseBoundedQueue(VectorBackendType backendType,
      VectorSearchParams searchParams) {
    if (backendType != VectorBackendType.HNSW) {
      return null;
    }
    Boolean value = searchParams.getHnswUseBoundedQueue();
    return value != null ? value : Boolean.TRUE;
  }

  @Nullable
  private static Integer parseInteger(@Nullable Object value) {
    if (value instanceof Number) {
      return ((Number) value).intValue();
    }
    if (value instanceof String) {
      try {
        return Integer.parseInt((String) value);
      } catch (NumberFormatException e) {
        return null;
      }
    }
    return null;
  }

  private void record(ImmutableRoaringBitmap matches) {
    InvocationRecording recording = Tracing.activeRecording();
    if (recording.isEnabled()) {
      recording.setNumDocsMatchingAfterFilter(matches.getCardinality());
      recording.setColumnName(_predicate.getLhs().getIdentifier());
      recording.setFilter(FilterType.INDEX, "VECTOR_SIMILARITY");
      recording.setInputDataType(FieldSpec.DataType.FLOAT, false);
    }
  }

  /// Simple holder for document ID and its exact distance during rerank.
  private static final class DocDistance {
    final int _docId;
    final float _distance;

    DocDistance(int docId, float distance) {
      _docId = docId;
      _distance = distance;
    }
  }
}
