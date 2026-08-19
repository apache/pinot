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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.CaseFormat;
import java.util.Arrays;
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
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Fallback operator that performs exact brute-force vector similarity search by scanning the forward index.
///
/// This operator is used when no ANN vector index exists on a segment for the target column
/// (e.g., the segment was built before the vector index was added, or the index type is not
/// supported), or when a required doc-ids filter (the upsert doc-ids snapshot) must be enforced
/// and the vector index cannot restrict its candidate generation to it. It reads vectors from
/// the forward index, computes exact distances to the query vector, and returns the top-K
/// closest document IDs.
///
/// When a required doc-ids bitmap is present, only the documents in the bitmap are scanned and
/// eligible for selection -- top-K is selected from the allowed set directly, never computed on
/// all physical rows and intersected afterwards.
///
/// The distance computation uses L2 (Euclidean) squared distance. For COSINE similarity,
/// vectors should be pre-normalized. This matches the behavior of Lucene's HNSW implementation.
///
/// This operator is intentionally simple and correct rather than fast -- it is a safety net.
/// It logs when used because it linearly scans the eligible documents (all docs in the segment, or only
/// the required doc ids when present).
///
/// This class is thread-safe for single-threaded execution per query (same as other filter operators).
public class ExactVectorScanFilterOperator extends BaseFilterOperator {
  /// Prefix shared by every fallback reason that stems from enforcing the upsert doc-ids snapshot.
  /// FilterPlanNode builds those reasons from this constant, and [#logScanStart()] uses it to log the
  /// expected upsert steady-state scans at DEBUG instead of WARN -- keep the two in sync through this
  /// constant only.
  public static final String UPSERT_SNAPSHOT_FALLBACK_REASON_PREFIX = "upsert_snapshot";

  private static final Logger LOGGER = LoggerFactory.getLogger(ExactVectorScanFilterOperator.class);
  private static final String EXPLAIN_NAME = "VECTOR_SIMILARITY_EXACT_SCAN";

  private final ForwardIndexReader<?> _forwardIndexReader;
  private final VectorSimilarityPredicate _predicate;
  private final String _column;
  private final VectorExplainContext _vectorExplainContext;
  private final boolean _hasDistanceThreshold;
  private final float _distanceThreshold;
  // Mandatory candidate filter (upsert doc-ids snapshot): when non-null, only these doc ids are scanned
  @Nullable
  private final ImmutableRoaringBitmap _requiredDocIds;
  private ImmutableRoaringBitmap _matches;

  /// Creates an exact scan operator with defaults (no index config, no required filter).
  ///
  /// @param forwardIndexReader the forward index reader for the vector column
  /// @param predicate the vector similarity predicate containing query vector and top-K
  /// @param column the column name (for logging and explain)
  /// @param numDocs the total number of documents in the segment
  public ExactVectorScanFilterOperator(ForwardIndexReader<?> forwardIndexReader,
      VectorSimilarityPredicate predicate, String column, int numDocs) {
    this(forwardIndexReader, predicate, column, numDocs, "vector_index_missing", VectorSearchSpec.DEFAULT);
  }

  @VisibleForTesting
  ExactVectorScanFilterOperator(ForwardIndexReader<?> forwardIndexReader,
      VectorSimilarityPredicate predicate, String column, int numDocs, @Nullable VectorIndexConfig vectorIndexConfig,
      String fallbackReason) {
    this(forwardIndexReader, predicate, column, numDocs, fallbackReason,
        new VectorSearchSpec.Builder().withVectorIndexConfig(vectorIndexConfig).build());
  }

  @VisibleForTesting
  ExactVectorScanFilterOperator(ForwardIndexReader<?> forwardIndexReader,
      VectorSimilarityPredicate predicate, String column, int numDocs, @Nullable VectorIndexConfig vectorIndexConfig,
      String fallbackReason, VectorSearchParams searchParams) {
    this(forwardIndexReader, predicate, column, numDocs, fallbackReason,
        new VectorSearchSpec.Builder().withVectorIndexConfig(vectorIndexConfig).withSearchParams(searchParams)
            .build());
  }

  @VisibleForTesting
  ExactVectorScanFilterOperator(ForwardIndexReader<?> forwardIndexReader,
      VectorSimilarityPredicate predicate, String column, int numDocs, @Nullable VectorIndexConfig vectorIndexConfig,
      String fallbackReason, VectorSearchParams searchParams, @Nullable ImmutableRoaringBitmap requiredDocIds) {
    this(forwardIndexReader, predicate, column, numDocs, fallbackReason,
        new VectorSearchSpec.Builder().withVectorIndexConfig(vectorIndexConfig).withSearchParams(searchParams)
            .withRequiredDocIds(requiredDocIds).build());
  }

  /// Primary constructor.
  ///
  /// @param forwardIndexReader the forward index reader for the vector column (the scan source)
  /// @param predicate the vector similarity predicate containing query vector and top-K
  /// @param column the column name (for logging and explain)
  /// @param numDocs the total number of documents in the segment
  /// @param fallbackReason why the exact scan was chosen (surfaced in explain output)
  /// @param spec construction-time context: search params, vector index config and the required doc-ids
  ///             filter (upsert doc-ids snapshot); when the required filter is non-null, only those doc ids
  ///             are scanned and eligible for top-K / threshold selection
  public ExactVectorScanFilterOperator(ForwardIndexReader<?> forwardIndexReader,
      VectorSimilarityPredicate predicate, String column, int numDocs, String fallbackReason,
      VectorSearchSpec spec) {
    super(numDocs, false);
    VectorSearchParams searchParams = spec.getSearchParams();
    VectorIndexConfig vectorIndexConfig = spec.getVectorIndexConfig();
    _forwardIndexReader = forwardIndexReader;
    _predicate = predicate;
    _column = column;
    _hasDistanceThreshold = searchParams.hasDistanceThreshold();
    _distanceThreshold = searchParams.getDistanceThreshold();
    _requiredDocIds = spec.getRequiredDocIds();
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
    return List.of();
  }

  @Override
  public String toExplainString() {
    StringBuilder sb = new StringBuilder();
    sb.append(EXPLAIN_NAME).append("(indexLookUp:exact_scan")
        .append(", operator:").append(_predicate.getType())
        .append(", executionMode:").append(VectorExecutionMode.EXACT_SCAN)
        .append(", vector identifier:").append(_column)
        .append(", backend:").append(_vectorExplainContext.getBackendType())
        .append(", distanceFunction:").append(_vectorExplainContext.getDistanceFunction())
        .append(", vector literal:").append(Arrays.toString(_predicate.getValue()))
        .append(", topK to search:").append(_predicate.getTopK())
        .append(", fallbackReason:").append(_vectorExplainContext.getFallbackReason());
    if (_requiredDocIds != null) {
      sb.append(", upsertRequiredDocIdsCardinality:").append(_requiredDocIds.getCardinality());
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
    if (_requiredDocIds != null) {
      attributeBuilder.putBool("upsertRequiredDocIdsApplied", true);
      attributeBuilder.putLong("upsertRequiredDocIdsCardinality", _requiredDocIds.getCardinality());
    }
  }

  /// Performs brute-force exact search over the scanned documents: all documents in the segment, or only the
  /// required doc ids when a required filter is present.
  /// When a distance threshold is set, returns all scanned vectors within the threshold.
  /// Otherwise uses a max-heap to maintain the top-K closest vectors.
  @SuppressWarnings("unchecked")
  private ImmutableRoaringBitmap computeExactTopK() {
    logScanStart();

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
      if (_requiredDocIds == null) {
        for (int docId = 0; docId < _numDocs; docId++) {
          considerDocForTopK(rawReader, context, docId, queryVector, maxHeap, topK);
        }
      } else {
        IntIterator docIdIterator = _requiredDocIds.getIntIterator();
        int docId;
        // Bitmaps iterate in ascending order, so the first out-of-range doc id ends the scan
        while (docIdIterator.hasNext() && (docId = docIdIterator.next()) < _numDocs) {
          considerDocForTopK(rawReader, context, docId, queryVector, maxHeap, topK);
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

  @SuppressWarnings("unchecked")
  private void considerDocForTopK(ForwardIndexReader rawReader, ForwardIndexReaderContext context, int docId,
      float[] queryVector, PriorityQueue<DocDistance> maxHeap, int topK) {
    float[] docVector = rawReader.getFloatMV(docId, context);
    if (docVector == null || docVector.length == 0) {
      return;
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

  /// Performs brute-force threshold scan: returns all scanned vectors within the distance threshold.
  @SuppressWarnings("unchecked")
  private ImmutableRoaringBitmap computeExactThreshold(float[] queryVector) {
    MutableRoaringBitmap result = new MutableRoaringBitmap();
    ForwardIndexReader rawReader = _forwardIndexReader;
    try (ForwardIndexReaderContext context = rawReader.createContext()) {
      if (_requiredDocIds == null) {
        for (int docId = 0; docId < _numDocs; docId++) {
          considerDocForThreshold(rawReader, context, docId, queryVector, result);
        }
      } else {
        IntIterator docIdIterator = _requiredDocIds.getIntIterator();
        int docId;
        // Bitmaps iterate in ascending order, so the first out-of-range doc id ends the scan
        while (docIdIterator.hasNext() && (docId = docIdIterator.next()) < _numDocs) {
          considerDocForThreshold(rawReader, context, docId, queryVector, result);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Error during exact threshold scan on column: " + _column, e);
    }

    LOGGER.debug("Exact threshold scan on column: {} returned {} results from {} docs (threshold={})",
        _column, result.getCardinality(), _numDocs, _distanceThreshold);

    return result.toImmutableRoaringBitmap();
  }

  @SuppressWarnings("unchecked")
  private void considerDocForThreshold(ForwardIndexReader rawReader, ForwardIndexReaderContext context, int docId,
      float[] queryVector, MutableRoaringBitmap result) {
    float[] docVector = rawReader.getFloatMV(docId, context);
    if (docVector == null || docVector.length == 0) {
      return;
    }
    float distance = VectorDistanceUtils.computeDistance(queryVector, docVector,
        _vectorExplainContext.getDistanceFunction());
    if (distance <= _distanceThreshold) {
      result.add(docId);
    }
  }

  /// The exact scan is the expected path when the upsert doc-ids snapshot must be enforced and the vector
  /// index cannot honor it (a pluggable/custom reader that is not filter-aware; all built-in readers,
  /// including the mutable HNSW index, are filter-aware and take the filtered-ANN path instead) -- log
  /// that at DEBUG. Everything else (genuinely missing or unusable index) keeps the actionable WARN.
  private void logScanStart() {
    String fallbackReason = _vectorExplainContext.getFallbackReason();
    if (fallbackReason != null && fallbackReason.startsWith(UPSERT_SNAPSHOT_FALLBACK_REASON_PREFIX)) {
      LOGGER.debug("Performing exact vector scan restricted to {} required docs on column: {} ({} docs total), "
              + "reason={}, distanceFunction={}, hasThreshold={}",
          _requiredDocIds != null ? _requiredDocIds.getCardinality() : "all", _column, _numDocs, fallbackReason,
          _vectorExplainContext.getDistanceFunction(), _hasDistanceThreshold);
    } else {
      LOGGER.warn("Performing exact vector scan fallback on column: {} for segment with {} docs. "
              + "reason={}, distanceFunction={}, hasThreshold={}, requiredDocIdsCardinality={}. "
              + "This is expensive -- consider adding a vector index.",
          _column, _numDocs, fallbackReason, _vectorExplainContext.getDistanceFunction(), _hasDistanceThreshold,
          _requiredDocIds != null ? _requiredDocIds.getCardinality() : "all");
    }
  }

  /// Computes the squared L2 (Euclidean) distance between two vectors.
  /// Delegates to [VectorFunctions#euclideanDistance(float[], float[])] which returns
  /// the sum of squared differences (no sqrt), sufficient for ranking.
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

  /// Simple holder for document ID and its distance to the query vector.
  private static final class DocDistance {
    final int _docId;
    final float _distance;

    DocDistance(int docId, float distance) {
      _docId = docId;
      _distance = distance;
    }
  }
}
