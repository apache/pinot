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
import java.util.PriorityQueue;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.predicate.VectorSimilarityPredicate;
import org.apache.pinot.core.common.BlockDocIdSet;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.ExplainAttributeBuilder;
import org.apache.pinot.core.operator.docidsets.BitmapDocIdSet;
import org.apache.pinot.segment.spi.index.creator.VectorBackendType;
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

  private final VectorIndexReader _vectorIndexReader;
  private final VectorSimilarityPredicate _predicate;
  private final VectorSearchParams _searchParams;
  private final ForwardIndexReader<?> _forwardIndexReader;
  @Nullable
  private final VectorIndexConfig _vectorIndexConfig;
  private final boolean _requestedExactRerank;
  private final VectorExplainContext _vectorExplainContext;
  private volatile int _annCandidateCount;
  private volatile int _rerankedCandidateCount;
  private ImmutableRoaringBitmap _matches;

  /**
   * Backward-compatible constructor that uses default search params and no forward index.
   * Existing callers that do not pass query options continue to work unchanged.
   */
  public VectorSimilarityFilterOperator(VectorIndexReader vectorIndexReader, VectorSimilarityPredicate predicate,
      int numDocs) {
    this(vectorIndexReader, predicate, numDocs, VectorSearchParams.DEFAULT, null, null);
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
    this(vectorIndexReader, predicate, numDocs, searchParams, forwardIndexReader, null);
  }

  public VectorSimilarityFilterOperator(VectorIndexReader vectorIndexReader, VectorSimilarityPredicate predicate,
      int numDocs, VectorSearchParams searchParams, @Nullable ForwardIndexReader<?> forwardIndexReader,
      @Nullable VectorIndexConfig vectorIndexConfig) {
    super(numDocs, false);
    _vectorIndexReader = vectorIndexReader;
    _predicate = predicate;
    _searchParams = searchParams;
    _forwardIndexReader = forwardIndexReader;
    _vectorIndexConfig = vectorIndexConfig;
    VectorBackendType backendType = VectorDistanceUtils.resolveBackendType(vectorIndexConfig);
    _requestedExactRerank = searchParams.isExactRerank(backendType);
    boolean effectiveExactRerank = _requestedExactRerank && forwardIndexReader != null;
    int effectiveSearchCount = effectiveExactRerank
        ? searchParams.getEffectiveMaxCandidates(predicate.getTopK(), numDocs)
        : predicate.getTopK();
    _vectorExplainContext = new VectorExplainContext(backendType,
        VectorDistanceUtils.resolveDistanceFunction(vectorIndexConfig),
        backendType.supportsNprobe() ? searchParams.getNprobe() : 0, effectiveExactRerank, effectiveSearchCount,
        null);
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

    // 1. Configure backend-specific parameters via interfaces
    configureBackendParams(column);

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

    // 4. Apply exact rerank if requested
    if (explainContext.isEffectiveExactRerank() && _forwardIndexReader != null && annCandidateCount > 0) {
      ImmutableRoaringBitmap reranked = applyExactRerank(annResults, queryVector, _predicate.getTopK(), column);
      _rerankedCandidateCount = reranked.getCardinality();
      LOGGER.debug("Exact rerank on column: {}, candidates: {} -> final: {}",
          column, annCandidateCount, reranked.getCardinality());
      return reranked;
    }
    return annResults;
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
    return VectorDistanceUtils.resolveBackendType(_vectorIndexConfig).name();
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
