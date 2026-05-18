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
import org.apache.pinot.segment.spi.index.creator.VectorExecutionMode;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.trace.FilterType;
import org.apache.pinot.spi.trace.InvocationRecording;
import org.apache.pinot.spi.trace.Tracing;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Fallback operator that performs exact brute-force vector similarity search by scanning the forward index.
 *
 * <p>This operator is used when no ANN vector index exists on a segment for the target column
 * (e.g., the segment was built before the vector index was added, or the index type is not
 * supported). It reads all vectors from the forward index, computes exact distances to the
 * query vector, and returns the top-K closest document IDs.</p>
 *
 * <p>The distance computation uses L2 (Euclidean) squared distance. For COSINE similarity,
 * vectors should be pre-normalized. This matches the behavior of Lucene's HNSW implementation.</p>
 *
 * <p>This operator is intentionally simple and correct rather than fast -- it is a safety net.
 * A warning is logged when this operator is used because it scans all documents in the segment.</p>
 *
 * <p>This class is thread-safe for single-threaded execution per query (same as other filter operators).</p>
 */
public class ExactVectorScanFilterOperator extends BaseFilterOperator {
  private static final Logger LOGGER = LoggerFactory.getLogger(ExactVectorScanFilterOperator.class);
  private static final String EXPLAIN_NAME = "VECTOR_SIMILARITY_EXACT_SCAN";

  private final ForwardIndexReader<?> _forwardIndexReader;
  private final VectorSimilarityPredicate _predicate;
  private final String _column;
  private final VectorExplainContext _vectorExplainContext;
  private final boolean _hasDistanceThreshold;
  private final float _distanceThreshold;
  private ImmutableRoaringBitmap _matches;

  /**
   * Creates an exact scan operator.
   *
   * @param forwardIndexReader the forward index reader for the vector column
   * @param predicate the vector similarity predicate containing query vector and top-K
   * @param column the column name (for logging and explain)
   * @param numDocs the total number of documents in the segment
   */
  public ExactVectorScanFilterOperator(ForwardIndexReader<?> forwardIndexReader,
      VectorSimilarityPredicate predicate, String column, int numDocs) {
    this(forwardIndexReader, predicate, column, numDocs, null, "vector_index_missing",
        VectorSearchParams.DEFAULT);
  }

  public ExactVectorScanFilterOperator(ForwardIndexReader<?> forwardIndexReader,
      VectorSimilarityPredicate predicate, String column, int numDocs, @Nullable VectorIndexConfig vectorIndexConfig,
      String fallbackReason) {
    this(forwardIndexReader, predicate, column, numDocs, vectorIndexConfig, fallbackReason,
        VectorSearchParams.DEFAULT);
  }

  public ExactVectorScanFilterOperator(ForwardIndexReader<?> forwardIndexReader,
      VectorSimilarityPredicate predicate, String column, int numDocs, @Nullable VectorIndexConfig vectorIndexConfig,
      String fallbackReason, VectorSearchParams searchParams) {
    super(numDocs, false);
    _forwardIndexReader = forwardIndexReader;
    _predicate = predicate;
    _column = column;
    _hasDistanceThreshold = searchParams.hasDistanceThreshold();
    _distanceThreshold = searchParams.getDistanceThreshold();
    float effectiveThreshold = _hasDistanceThreshold ? _distanceThreshold : -1f;
    _vectorExplainContext = new VectorExplainContext(VectorDistanceUtils.resolveBackendType(vectorIndexConfig),
        VectorDistanceUtils.resolveDistanceFunction(vectorIndexConfig), VectorExecutionMode.EXACT_SCAN,
        VectorSearchParams.DEFAULT_NPROBE, false, predicate.getTopK(), fallbackReason, null, 0, effectiveThreshold,
        VectorSearchMode.EXACT_SCAN, -1.0, null, null);
  }

  @Override
  protected BlockDocIdSet getTrues() {
    if (_matches == null) {
      _matches = computeExactTopK();
    }
    return new BitmapDocIdSet(_matches, _numDocs);
  }

  @Override
  public int getNumMatchingDocs() {
    if (_matches == null) {
      _matches = computeExactTopK();
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
      _matches = computeExactTopK();
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
    return EXPLAIN_NAME + "(indexLookUp:exact_scan"
        + ", operator:" + _predicate.getType()
        + ", executionMode:" + VectorExecutionMode.EXACT_SCAN
        + ", vector identifier:" + _column
        + ", backend:" + _vectorExplainContext.getBackendType()
        + ", distanceFunction:" + _vectorExplainContext.getDistanceFunction()
        + ", vector literal:" + Arrays.toString(_predicate.getValue())
        + ", topK to search:" + _predicate.getTopK()
        + ", fallbackReason:" + _vectorExplainContext.getFallbackReason()
        + ')';
  }

  @Override
  protected String getExplainName() {
    return CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, EXPLAIN_NAME);
  }

  @Override
  protected void explainAttributes(ExplainAttributeBuilder attributeBuilder) {
    super.explainAttributes(attributeBuilder);
    attributeBuilder.putString("indexLookUp", "exact_scan");
    attributeBuilder.putString("operator", _predicate.getType().name());
    attributeBuilder.putString("executionMode", VectorExecutionMode.EXACT_SCAN.name());
    attributeBuilder.putString("vectorIdentifier", _column);
    attributeBuilder.putString("backend", _vectorExplainContext.getBackendType().name());
    attributeBuilder.putString("distanceFunction", _vectorExplainContext.getDistanceFunction().name());
    attributeBuilder.putString("vectorLiteral", Arrays.toString(_predicate.getValue()));
    attributeBuilder.putString("fallbackReason", _vectorExplainContext.getFallbackReason());
    attributeBuilder.putLongIdempotent("topKtoSearch", _predicate.getTopK());
  }

  /**
   * Performs brute-force exact search over all documents in the segment.
   * When a distance threshold is set, returns all vectors within the threshold.
   * Otherwise uses a max-heap to maintain the top-K closest vectors.
   */
  @SuppressWarnings("unchecked")
  private ImmutableRoaringBitmap computeExactTopK() {
    LOGGER.warn("Performing exact vector scan fallback on column: {} for segment with {} docs. "
            + "reason={}, distanceFunction={}, hasThreshold={}. "
            + "This is expensive -- consider adding a vector index.",
        _column, _numDocs, _vectorExplainContext.getFallbackReason(),
        _vectorExplainContext.getDistanceFunction(), _hasDistanceThreshold);

    float[] queryVector = _predicate.getValue();

    if (_hasDistanceThreshold) {
      return computeExactThreshold(queryVector);
    }

    int topK = _predicate.getTopK();

    // Max-heap: entry with largest distance is at the top so we can efficiently evict it
    PriorityQueue<DocDistance> maxHeap = new PriorityQueue<>(topK + 1,
        (a, b) -> Float.compare(b._distance, a._distance));

    ForwardIndexReader rawReader = _forwardIndexReader;
    try (ForwardIndexReaderContext context = rawReader.createContext()) {
      for (int docId = 0; docId < _numDocs; docId++) {
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
      throw new RuntimeException("Error during exact vector scan on column: " + _column, e);
    }

    MutableRoaringBitmap result = new MutableRoaringBitmap();
    for (DocDistance dd : maxHeap) {
      result.add(dd._docId);
    }

    LOGGER.debug("Exact vector scan on column: {} returned {} results from {} docs",
        _column, result.getCardinality(), _numDocs);

    return result;
  }

  /**
   * Performs brute-force threshold scan: returns all vectors within the distance threshold.
   */
  @SuppressWarnings("unchecked")
  private ImmutableRoaringBitmap computeExactThreshold(float[] queryVector) {
    MutableRoaringBitmap result = new MutableRoaringBitmap();
    ForwardIndexReader rawReader = _forwardIndexReader;
    try (ForwardIndexReaderContext context = rawReader.createContext()) {
      for (int docId = 0; docId < _numDocs; docId++) {
        float[] docVector = rawReader.getFloatMV(docId, context);
        if (docVector == null || docVector.length == 0) {
          continue;
        }
        float distance = VectorDistanceUtils.computeDistance(queryVector, docVector,
            _vectorExplainContext.getDistanceFunction());
        if (distance <= _distanceThreshold) {
          result.add(docId);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Error during exact threshold scan on column: " + _column, e);
    }

    LOGGER.debug("Exact threshold scan on column: {} returned {} results from {} docs (threshold={})",
        _column, result.getCardinality(), _numDocs, _distanceThreshold);

    return result.toImmutableRoaringBitmap();
  }

  /**
   * Computes the squared L2 (Euclidean) distance between two vectors.
   * Delegates to {@link VectorFunctions#euclideanDistance(float[], float[])} which returns
   * the sum of squared differences (no sqrt), sufficient for ranking.
   */
  static float computeL2SquaredDistance(float[] a, float[] b) {
    return VectorDistanceUtils.computeDistance(a, b, VectorIndexConfig.VectorDistanceFunction.L2);
  }

  private void record(ImmutableRoaringBitmap matches) {
    InvocationRecording recording = Tracing.activeRecording();
    if (recording.isEnabled()) {
      recording.setNumDocsMatchingAfterFilter(matches.getCardinality());
      recording.setColumnName(_column);
      recording.setFilter(FilterType.INDEX, "VECTOR_SIMILARITY_EXACT_SCAN");
      recording.setInputDataType(FieldSpec.DataType.FLOAT, false);
      recording.setNumDocsMatchingAfterFilter(matches.getCardinality());
    }
  }

  /**
   * Simple holder for document ID and its distance to the query vector.
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
