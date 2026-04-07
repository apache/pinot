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
import java.util.Collections;
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


/**
 * Operator for vector similarity search using an ANN index (HNSW or IVF_FLAT).
 *
 * <p>This operator supports backend-neutral vector search with the following capabilities:</p>
 * <ul>
 *   <li><b>nprobe dispatch:</b> If the underlying reader implements {@link NprobeAware}, the
 *       {@code vectorNprobe} query option is applied before search.</li>
 *   <li><b>Exact rerank:</b> When {@code vectorExactRerank=true}, ANN candidates are re-scored
 *       using exact distance from the forward index and re-sorted before final top-K selection.</li>
 *   <li><b>maxCandidates:</b> Controls how many ANN candidates are retrieved before rerank. Only
 *       meaningful when rerank is enabled.</li>
 * </ul>
 *
 * <p>When no query options are specified, behavior is identical to the previous HNSW-only path
 * (full backward compatibility).</p>
 *
 * <p>This class is thread-safe for single-threaded execution per query.</p>
 */
public class VectorSimilarityFilterOperator extends BaseFilterOperator {
  private static final Logger LOGGER = LoggerFactory.getLogger(VectorSimilarityFilterOperator.class);
  private static final String EXPLAIN_NAME = "VECTOR_SIMILARITY_INDEX";
  /** Default over-fetch factor for filtered ANN queries to compensate for post-filter loss. */
  static final int FILTERED_OVERFETCH_FACTOR = 2;

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
  private volatile VectorExplainContext _vectorExplainContext;
  private volatile int _annCandidateCount;
  private volatile int _rerankedCandidateCount;
  private ImmutableRoaringBitmap _matches;

  /**
   * Backward-compatible constructor that uses default search params and no forward index.
   * Existing callers that do not pass query options continue to work unchanged.
   */
  public VectorSimilarityFilterOperator(VectorIndexReader vectorIndexReader, VectorSimilarityPredicate predicate,
      int numDocs) {
    this(vectorIndexReader, predicate, numDocs, VectorSearchParams.DEFAULT, null, null, false);
  }

  /**
   * Full constructor with query option support.
   *
   * @param vectorIndexReader the ANN index reader
   * @param predicate the vector similarity predicate
   * @param numDocs total docs in the segment
   * @param searchParams vector search parameters from query options
   * @param forwardIndexReader forward index reader for exact rerank (may be null if rerank is not needed)
   */
  public VectorSimilarityFilterOperator(VectorIndexReader vectorIndexReader, VectorSimilarityPredicate predicate,
      int numDocs, VectorSearchParams searchParams, @Nullable ForwardIndexReader<?> forwardIndexReader) {
    this(vectorIndexReader, predicate, numDocs, searchParams, forwardIndexReader, null, false);
  }

  public VectorSimilarityFilterOperator(VectorIndexReader vectorIndexReader, VectorSimilarityPredicate predicate,
      int numDocs, VectorSearchParams searchParams, @Nullable ForwardIndexReader<?> forwardIndexReader,
      @Nullable VectorIndexConfig vectorIndexConfig) {
    this(vectorIndexReader, predicate, numDocs, searchParams, forwardIndexReader, vectorIndexConfig, false);
  }

  /**
   * Full constructor with metadata filter awareness.
   *
   * @param vectorIndexReader the ANN index reader
   * @param predicate the vector similarity predicate
   * @param numDocs total docs in the segment
   * @param searchParams vector search parameters from query options
   * @param forwardIndexReader forward index reader for exact rerank (may be null if rerank is not needed)
   * @param vectorIndexConfig vector index configuration (may be null)
   * @param hasMetadataFilter true if this operator is combined with metadata filters in an AND
   */
  public VectorSimilarityFilterOperator(VectorIndexReader vectorIndexReader, VectorSimilarityPredicate predicate,
      int numDocs, VectorSearchParams searchParams, @Nullable ForwardIndexReader<?> forwardIndexReader,
      @Nullable VectorIndexConfig vectorIndexConfig, boolean hasMetadataFilter) {
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
    // When metadata filter is present, over-fetch ANN candidates to compensate for filter loss.
    // Default over-fetch factor is 2x topK for filtered queries without explicit maxCandidates.
    // For threshold queries, use a larger candidate pool since we need distance refinement.
    int baseSearchCount;
    if (_hasThresholdPredicate && !searchParams.isMaxCandidatesExplicit()) {
      // Threshold queries need a larger candidate pool for exact distance refinement.
      // Use topK * 10 by default, similar to rerank mode.
      baseSearchCount = Math.min(predicate.getTopK() * 10, numDocs);
    } else if (_effectiveExactRerank) {
      baseSearchCount = searchParams.getEffectiveMaxCandidates(predicate.getTopK(), numDocs);
    } else if (hasMetadataFilter && !searchParams.isMaxCandidatesExplicit()) {
      baseSearchCount = Math.min(predicate.getTopK() * FILTERED_OVERFETCH_FACTOR, numDocs);
    } else {
      baseSearchCount = predicate.getTopK();
    }
    _effectiveSearchCount = baseSearchCount;
    refreshExplainContext(null);
    _annCandidateCount = -1;
    _rerankedCandidateCount = -1;
    _matches = null;
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
    return Collections.emptyList();
  }

  @Override
  public String toExplainString() {
    VectorExplainContext explainContext = _vectorExplainContext;
    return EXPLAIN_NAME + "(indexLookUp:vector_index"
        + ", operator:" + _predicate.getType()
        + ", executionMode:" + (explainContext.getExecutionMode() != null
            ? explainContext.getExecutionMode() : "UNKNOWN")
        + ", vector identifier:" + _predicate.getLhs().getIdentifier()
        + ", backend:" + explainContext.getBackendType()
        + ", distanceFunction:" + explainContext.getDistanceFunction()
        + ", effectiveNprobe:" + explainContext.getEffectiveNprobe()
        + ", effectiveExactRerank:" + explainContext.isEffectiveExactRerank()
        + ", effectiveCandidateCount:" + explainContext.getEffectiveSearchCount()
        + ", vector literal:" + Arrays.toString(_predicate.getValue())
        + ", topK to search:" + _predicate.getTopK()
        + ')';
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
    attributeBuilder.putBool("effectiveExactRerank", explainContext.isEffectiveExactRerank());
    attributeBuilder.putLongIdempotent("effectiveCandidateCount", explainContext.getEffectiveSearchCount());
    attributeBuilder.putString("vectorLiteral", Arrays.toString(_predicate.getValue()));
    attributeBuilder.putLongIdempotent("topKtoSearch", _predicate.getTopK());
    if (_annCandidateCount >= 0) {
      attributeBuilder.putLong("annCandidateCount", _annCandidateCount);
    }
    if (_rerankedCandidateCount >= 0) {
      attributeBuilder.putLong("rerankedCandidateCount", _rerankedCandidateCount);
    }
  }

  /**
   * Executes the vector search with backend-specific parameter dispatch and optional rerank.
   */
  private ImmutableRoaringBitmap executeSearch() {
    String column = _predicate.getLhs().getIdentifier();
    float[] queryVector = _predicate.getValue();
    VectorExplainContext explainContext = _vectorExplainContext;
    try {
      // 1. Configure backend-specific parameters via interfaces
      configureBackendParams(column);
      refreshExplainContext(null);
      explainContext = _vectorExplainContext;

      // 2. Determine effective search count (higher if rerank is enabled)
      int searchCount = explainContext.getEffectiveSearchCount();

      // 3. Execute ANN search
      ImmutableRoaringBitmap annResults = _vectorIndexReader.getDocIds(queryVector, searchCount);
      int annCandidateCount = annResults.getCardinality();
      _annCandidateCount = annCandidateCount;

      LOGGER.debug("Vector search on column: {}, backend: {}, distanceFunction: {}, topK: {}, searchCount: {}, "
              + "annCandidates: {}, exactRerank: {}",
          column, explainContext.getBackendType(), explainContext.getDistanceFunction(), _predicate.getTopK(),
          searchCount, annCandidateCount, explainContext.isEffectiveExactRerank());

      if (_requestedExactRerank && !explainContext.isEffectiveExactRerank()) {
        LOGGER.warn("Vector exact rerank was requested on column: {} but no forward index reader is available. "
                + "Using ANN topK results only.",
            column);
      }

      // 4. Apply threshold refinement if distance threshold is set
      if (_hasThresholdPredicate && _forwardIndexReader != null && annCandidateCount > 0) {
        ImmutableRoaringBitmap thresholded = applyThresholdRefinement(
            annResults, queryVector, _distanceThreshold, column);
        _rerankedCandidateCount = thresholded.getCardinality();
        LOGGER.debug("Threshold refinement on column: {}, threshold: {}, candidates: {} -> final: {}",
            column, _distanceThreshold, annCandidateCount, thresholded.getCardinality());
        return thresholded;
      }

      // 5. Apply exact rerank if requested
      if (explainContext.isEffectiveExactRerank() && _forwardIndexReader != null && annCandidateCount > 0) {
        ImmutableRoaringBitmap reranked = applyExactRerank(annResults, queryVector, _predicate.getTopK(), column);
        _rerankedCandidateCount = reranked.getCardinality();
        LOGGER.debug("Exact rerank on column: {}, candidates: {} -> final: {}",
            column, annCandidateCount, reranked.getCardinality());
        return reranked;
      }
      return annResults;
    } finally {
      clearBackendParams(column);
    }
  }

  /**
   * Configures backend-specific search parameters on the reader if it supports them.
   */
  private void configureBackendParams(String column) {
    if (_vectorIndexReader instanceof NprobeAware) {
      int nprobe = _searchParams.getNprobe();
      ((NprobeAware) _vectorIndexReader).setNprobe(nprobe);
      LOGGER.debug("Set nprobe={} on {} reader for column: {}", nprobe, getBackendName(), column);
    }
  }

  private void clearBackendParams(String column) {
    if (_vectorIndexReader instanceof NprobeAware) {
      ((NprobeAware) _vectorIndexReader).clearNprobe();
      LOGGER.debug("Cleared nprobe on {} reader for column: {}", getBackendName(), column);
    }
  }

  /**
   * Applies exact distance threshold refinement to ANN candidates.
   * Returns only candidates whose exact distance is within the threshold.
   */
  @SuppressWarnings("unchecked")
  private ImmutableRoaringBitmap applyThresholdRefinement(ImmutableRoaringBitmap annResults, float[] queryVector,
      float threshold, String column) {
    MutableRoaringBitmap result = new MutableRoaringBitmap();
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
        if (distance <= threshold) {
          result.add(docId);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Error during threshold refinement on column: " + column, e);
    }
    return result;
  }

  /**
   * Re-scores ANN candidates using exact distance from the forward index and returns top-K.
   */
  @SuppressWarnings("unchecked")
  private ImmutableRoaringBitmap applyExactRerank(ImmutableRoaringBitmap annResults, float[] queryVector,
      int topK, String column) {
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

  /**
   * Returns a human-readable name for the backend (for logging).
   */
  private String getBackendName() {
    return _backendType.name();
  }

  private void refreshExplainContext(@Nullable String fallbackReason) {
    VectorExecutionMode executionMode = VectorQueryExecutionContext.selectExecutionMode(
        true, _hasMetadataFilter, _hasThresholdPredicate, _effectiveExactRerank,
        _backendType.getCapabilities());
    _vectorExplainContext = new VectorExplainContext(_backendType, _distanceFunction, executionMode,
        resolveEffectiveNprobe(_backendType, _searchParams, _vectorIndexReader.getIndexDebugInfo()),
        _effectiveExactRerank, _effectiveSearchCount, fallbackReason);
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
      recording.setNumDocsMatchingAfterFilter(matches.getCardinality());
    }
  }

  /**
   * Simple holder for document ID and its exact distance during rerank.
   */
  private static final class DocDistance {
    final int _docId;
    final float _distance;

    DocDistance(int docId, float distance) {
      _docId = docId;
      _distance = distance;
    }
  }
}
