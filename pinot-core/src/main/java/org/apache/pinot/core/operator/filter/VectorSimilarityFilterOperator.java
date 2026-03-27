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
  private ImmutableRoaringBitmap _matches;

  /**
   * Backward-compatible constructor that uses default search params and no forward index.
   * Existing callers that do not pass query options continue to work unchanged.
   */
  public VectorSimilarityFilterOperator(VectorIndexReader vectorIndexReader, VectorSimilarityPredicate predicate,
      int numDocs) {
    this(vectorIndexReader, predicate, numDocs, VectorSearchParams.DEFAULT, null);
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
    super(numDocs, false);
    _vectorIndexReader = vectorIndexReader;
    _predicate = predicate;
    _searchParams = searchParams;
    _forwardIndexReader = forwardIndexReader;
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
    return EXPLAIN_NAME + "(indexLookUp:vector_index"
        + ", operator:" + _predicate.getType()
        + ", vector identifier:" + _predicate.getLhs().getIdentifier()
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
    super.explainAttributes(attributeBuilder);
    attributeBuilder.putString("indexLookUp", "vector_index");
    attributeBuilder.putString("operator", _predicate.getType().name());
    attributeBuilder.putString("vectorIdentifier", _predicate.getLhs().getIdentifier());
    attributeBuilder.putString("vectorLiteral", Arrays.toString(_predicate.getValue()));
    attributeBuilder.putLongIdempotent("topKtoSearch", _predicate.getTopK());
    if (_searchParams.isExactRerank()) {
      attributeBuilder.putString("exactRerank", "true");
    }
  }

  /**
   * Executes the vector search with backend-specific parameter dispatch and optional rerank.
   */
  private ImmutableRoaringBitmap executeSearch() {
    String column = _predicate.getLhs().getIdentifier();
    float[] queryVector = _predicate.getValue();
    int topK = _predicate.getTopK();

    // 1. Configure backend-specific parameters via interfaces
    configureBackendParams(column);

    // 2. Determine effective search count (higher if rerank is enabled)
    int searchCount = topK;
    if (_searchParams.isExactRerank()) {
      searchCount = _searchParams.getEffectiveMaxCandidates(topK);
    }

    // 3. Execute ANN search
    ImmutableRoaringBitmap annResults = _vectorIndexReader.getDocIds(queryVector, searchCount);
    int annCandidateCount = annResults.getCardinality();

    LOGGER.debug("Vector search on column: {}, backend: {}, topK: {}, searchCount: {}, annCandidates: {}",
        column, getBackendName(), topK, searchCount, annCandidateCount);

    // 4. Apply exact rerank if requested
    if (_searchParams.isExactRerank() && _forwardIndexReader != null && annCandidateCount > 0) {
      ImmutableRoaringBitmap reranked = applyExactRerank(annResults, queryVector, topK, column);
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
    // Set nprobe on IVF_FLAT readers
    if (_vectorIndexReader instanceof NprobeAware) {
      int nprobe = _searchParams.getNprobe();
      ((NprobeAware) _vectorIndexReader).setNprobe(nprobe);
      LOGGER.debug("Set nprobe={} on IVF_FLAT reader for column: {}", nprobe, column);
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
        // TODO: derive distance function from segment's vector index config instead of hardcoding L2.
        //  Currently correct for EUCLIDEAN/L2; may produce suboptimal rerank ordering for COSINE/DOT_PRODUCT.
        float distance = ExactVectorScanFilterOperator.computeL2SquaredDistance(queryVector, docVector);
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
    if (_vectorIndexReader instanceof NprobeAware) {
      return "IVF_FLAT";
    }
    return "HNSW";
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
