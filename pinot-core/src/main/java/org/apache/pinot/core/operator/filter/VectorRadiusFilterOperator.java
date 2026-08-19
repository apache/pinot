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
import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.predicate.VectorSimilarityRadiusPredicate;
import org.apache.pinot.core.common.BlockDocIdSet;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.ExplainAttributeBuilder;
import org.apache.pinot.core.operator.docidsets.BitmapDocIdSet;
import org.apache.pinot.segment.spi.index.creator.VectorBackendType;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;
import org.apache.pinot.segment.spi.index.reader.ApproximateRadiusVectorIndexReader;
import org.apache.pinot.segment.spi.index.reader.FilterAwareVectorIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.index.reader.VectorIndexReader;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.trace.FilterType;
import org.apache.pinot.spi.trace.InvocationRecording;
import org.apache.pinot.spi.trace.Tracing;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Operator for vector similarity radius/threshold search.
///
/// This operator returns all documents whose vector distance from the query vector is within
/// the specified threshold. It uses a two-phase approach:
///
/// 1. If a vector index is available, retrieves a large candidate set from the ANN index
///       (using an internal safety limit).
/// 2. Computes exact distance for each candidate using the forward index, and returns only
///       documents within the threshold distance.
///
/// If no vector index is available, falls back to brute-force scanning of all documents
/// via the forward index.
///
/// When a required doc-ids filter (the upsert doc-ids snapshot) is present, every path is restricted to it:
/// index-assisted candidate retrieval uses the filtered search of [FilterAwareVectorIndexReader] (or falls
/// back to a brute-force scan over only the required doc ids when the reader cannot honor the filter), and
/// the brute-force scan iterates only the required doc ids. Radius results were already correct without this
/// (the candidate-saturation fallback guarantees completeness and the planner ANDs the snapshot on top), but
/// restricting candidate generation avoids wasting the candidate budget -- and the saturation latency cliff
/// -- on upsert-obsoleted rows.
///
/// This class is thread-safe for single-threaded execution per query.
public class VectorRadiusFilterOperator extends BaseFilterOperator {
  private static final Logger LOGGER = LoggerFactory.getLogger(VectorRadiusFilterOperator.class);
  private static final String EXPLAIN_NAME = "VECTOR_SIMILARITY_RADIUS";

  private final VectorSimilarityRadiusPredicate _predicate;
  private final ForwardIndexReader<?> _forwardIndexReader;
  @Nullable
  private final VectorIndexReader _vectorIndexReader;
  private final String _column;
  private final VectorBackendType _backendType;
  private final VectorIndexConfig.VectorDistanceFunction _distanceFunction;
  private final boolean _hasVectorIndex;
  private final boolean _backendSupportsApproximateRadius;
  private final boolean _readerSupportsApproximateRadius;
  private final boolean _useApproximateRadiusPath;
  // Mandatory candidate filter (upsert doc-ids snapshot): when non-null, every path only considers these docs
  @Nullable
  private final ImmutableRoaringBitmap _requiredDocIds;
  private ImmutableRoaringBitmap _matches;

  @VisibleForTesting
  VectorRadiusFilterOperator(ForwardIndexReader<?> forwardIndexReader,
      @Nullable VectorIndexReader vectorIndexReader, VectorSimilarityRadiusPredicate predicate,
      String column, int numDocs, @Nullable VectorIndexConfig vectorIndexConfig,
      @Nullable ImmutableRoaringBitmap requiredDocIds) {
    this(forwardIndexReader, vectorIndexReader, predicate, column, numDocs,
        new VectorSearchSpec.Builder().withVectorIndexConfig(vectorIndexConfig).withRequiredDocIds(requiredDocIds)
            .build());
  }

  /// Primary constructor.
  ///
  /// @param forwardIndexReader the forward index reader for exact distance computation (required)
  /// @param vectorIndexReader the ANN index reader for candidate retrieval (may be null for brute-force)
  /// @param predicate the vector similarity radius predicate
  /// @param column the column name
  /// @param numDocs the total number of documents in the segment
  /// @param spec construction-time context: vector index config and the required doc-ids filter (upsert
  ///             doc-ids snapshot)
  public VectorRadiusFilterOperator(ForwardIndexReader<?> forwardIndexReader,
      @Nullable VectorIndexReader vectorIndexReader, VectorSimilarityRadiusPredicate predicate,
      String column, int numDocs, VectorSearchSpec spec) {
    super(numDocs, false);
    VectorIndexConfig vectorIndexConfig = spec.getVectorIndexConfig();
    _forwardIndexReader = forwardIndexReader;
    _vectorIndexReader = vectorIndexReader;
    _predicate = predicate;
    _column = column;
    _backendType = VectorDistanceUtils.resolveBackendType(vectorIndexConfig);
    _distanceFunction = VectorDistanceUtils.resolveDistanceFunction(vectorIndexConfig);
    _hasVectorIndex = vectorIndexReader != null;
    _backendSupportsApproximateRadius = _backendType.getCapabilities().supportsApproximateRadius();
    _readerSupportsApproximateRadius = vectorIndexReader instanceof ApproximateRadiusVectorIndexReader;
    _useApproximateRadiusPath =
        _hasVectorIndex && _backendSupportsApproximateRadius && _readerSupportsApproximateRadius;
    _requiredDocIds = spec.getRequiredDocIds();
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
    StringBuilder sb = new StringBuilder();
    sb.append(EXPLAIN_NAME).append("(indexLookUp:").append(getIndexLookupMode())
        .append(", operator:").append(_predicate.getType())
        .append(", vector identifier:").append(_column)
        .append(", backend:").append(_backendType)
        .append(", distanceFunction:").append(_distanceFunction)
        .append(", approximateRadiusPath:").append(_useApproximateRadiusPath && _requiredDocIds == null)
        .append(", vector literal:").append(Arrays.toString(_predicate.getValue()))
        .append(", threshold:").append(_predicate.getThreshold());
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
    attributeBuilder.putString("indexLookUp", getIndexLookupMode());
    attributeBuilder.putString("operator", _predicate.getType().name());
    attributeBuilder.putString("vectorIdentifier", _column);
    attributeBuilder.putString("backend", _backendType.name());
    attributeBuilder.putString("distanceFunction", _distanceFunction.name());
    attributeBuilder.putBool("approximateRadiusPath", _useApproximateRadiusPath && _requiredDocIds == null);
    attributeBuilder.putString("vectorLiteral", Arrays.toString(_predicate.getValue()));
    attributeBuilder.putString("threshold", String.valueOf(_predicate.getThreshold()));
    if (_requiredDocIds != null) {
      attributeBuilder.putBool("upsertRequiredDocIdsApplied", true);
      attributeBuilder.putLong("upsertRequiredDocIdsCardinality", _requiredDocIds.getCardinality());
    }
  }

  /// Executes the radius search. If a vector index is available, uses it to get a candidate set
  /// and then filters by exact distance. Otherwise, performs a full brute-force scan.
  /// When a required doc-ids filter is present, every path is restricted to the required doc ids; a reader
  /// that cannot honor the filter is bypassed in favor of a brute-force scan over only the required docs.
  private ImmutableRoaringBitmap executeSearch() {
    float[] queryVector = _predicate.getValue();
    float threshold = _predicate.getThreshold();

    VectorSearchMetrics.getInstance().recordRadiusSearch();

    if (_requiredDocIds != null && _requiredDocIds.isEmpty()) {
      // Nothing is allowed to match: return empty without touching any index
      return new MutableRoaringBitmap();
    }

    if (_hasVectorIndex) {
      if (_requiredDocIds == null) {
        return executeIndexAssistedSearch(queryVector, threshold);
      }
      if (_vectorIndexReader instanceof FilterAwareVectorIndexReader
          && ((FilterAwareVectorIndexReader) _vectorIndexReader).supportsPreFilter()) {
        return executeFilteredIndexAssistedSearch(queryVector, threshold);
      }
      // The index cannot honor the required doc filter: scan only the required docs instead of running
      // unfiltered ANN whose candidate budget would be consumed by disallowed rows
      VectorSearchMetrics.getInstance().recordFallback("radius_required_filter_not_supported_by_index");
      return executeBruteForceScan(queryVector, threshold);
    }
    VectorSearchMetrics.getInstance().recordFallback("radius_no_vector_index");
    return executeBruteForceScan(queryVector, threshold);
  }

  /// Uses the ANN index to get a large candidate set, then filters by exact distance threshold.
  private ImmutableRoaringBitmap executeIndexAssistedSearch(float[] queryVector, float threshold) {
    int internalLimit = Math.min(VectorSimilarityRadiusPredicate.DEFAULT_INTERNAL_LIMIT, _numDocs);
    ImmutableRoaringBitmap candidates;
    if (_useApproximateRadiusPath) {
      candidates =
          ((ApproximateRadiusVectorIndexReader) _vectorIndexReader).getDocIdsWithinApproximateRadius(queryVector,
              threshold, internalLimit);
    } else {
      candidates = _vectorIndexReader.getDocIds(queryVector, internalLimit);
    }

    int candidateCount = candidates.getCardinality();
    LOGGER.debug("Vector radius search on column: {}, candidates from index: {}, threshold: {}, source: {}",
        _column, candidateCount, threshold, _useApproximateRadiusPath ? "approximate_radius_path" : "topk_path");

    if (candidateCount >= internalLimit) {
      // ANN candidate pool is saturated — fall back to exact brute-force scan to guarantee
      // completeness. A radius predicate must return ALL matching docs, not a truncated subset.
      LOGGER.info("Vector radius search on column: {} hit ANN candidate limit ({} >= {}). "
              + "Falling back to exact brute-force scan to guarantee complete results.",
          _column, candidateCount, internalLimit);
      VectorSearchMetrics.getInstance().recordFallback("radius_candidate_limit_saturated");
      return executeBruteForceScan(queryVector, threshold);
    }

    return filterByThreshold(candidates, queryVector, threshold);
  }

  /// Uses the filter-aware ANN search restricted to the required doc ids, then filters by exact distance.
  /// The approximate-radius path is not used here because it has no filtered variant; the filtered top-K
  /// path preserves the completeness guarantee through the same saturation fallback (restricted to the
  /// required docs).
  private ImmutableRoaringBitmap executeFilteredIndexAssistedSearch(float[] queryVector, float threshold) {
    int internalLimit = Math.min(VectorSimilarityRadiusPredicate.DEFAULT_INTERNAL_LIMIT, _numDocs);
    ImmutableRoaringBitmap candidates =
        ((FilterAwareVectorIndexReader) _vectorIndexReader).getDocIds(queryVector, internalLimit, _requiredDocIds);

    int candidateCount = candidates.getCardinality();
    LOGGER.debug("Filtered vector radius search on column: {}, candidates from index: {}, threshold: {}, "
        + "requiredDocIdsCardinality: {}", _column, candidateCount, threshold, _requiredDocIds.getCardinality());

    if (candidateCount >= internalLimit) {
      LOGGER.info("Filtered vector radius search on column: {} hit ANN candidate limit ({} >= {}). "
              + "Falling back to exact scan over the required docs to guarantee complete results.",
          _column, candidateCount, internalLimit);
      VectorSearchMetrics.getInstance().recordFallback("radius_candidate_limit_saturated");
      return executeBruteForceScan(queryVector, threshold);
    }

    return filterByThreshold(candidates, queryVector, threshold);
  }

  private String getIndexLookupMode() {
    if (!_hasVectorIndex) {
      return "exact_scan";
    }
    if (_requiredDocIds != null) {
      // The required doc-ids filter bypasses the approximate-radius path (no filtered variant exists) and
      // bypasses the index entirely for non-filter-aware readers -- report the mode actually executed
      return _vectorIndexReader instanceof FilterAwareVectorIndexReader
          && ((FilterAwareVectorIndexReader) _vectorIndexReader).supportsPreFilter()
          ? "vector_index_filtered_topk_with_scan" : "exact_scan_required_docs";
    }
    if (_useApproximateRadiusPath) {
      return "vector_index_approx_radius_with_scan";
    }
    return "vector_index_topk_with_scan";
  }

  /// Scans the eligible documents (all docs, or only the required doc ids when present) and returns those
  /// within the distance threshold.
  private ImmutableRoaringBitmap executeBruteForceScan(float[] queryVector, float threshold) {
    LOGGER.warn("Performing exact vector radius scan on column: {} for segment with {} docs (scanned: {}). "
            + "distanceFunction={}. This is expensive -- consider adding a vector index.",
        _column, _numDocs, _requiredDocIds != null ? _requiredDocIds.getCardinality() : _numDocs,
        _distanceFunction);

    MutableRoaringBitmap result = new MutableRoaringBitmap();
    ForwardIndexReader rawReader = _forwardIndexReader;
    try (ForwardIndexReaderContext context = rawReader.createContext()) {
      if (_requiredDocIds == null) {
        for (int docId = 0; docId < _numDocs; docId++) {
          considerDocForRadius(rawReader, context, docId, queryVector, threshold, result);
        }
      } else {
        IntIterator docIdIterator = _requiredDocIds.getIntIterator();
        int docId;
        // Bitmaps iterate in ascending order, so the first out-of-range doc id ends the scan
        while (docIdIterator.hasNext() && (docId = docIdIterator.next()) < _numDocs) {
          considerDocForRadius(rawReader, context, docId, queryVector, threshold, result);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Error during exact vector radius scan on column: " + _column, e);
    }

    LOGGER.debug("Exact vector radius scan on column: {} returned {} results from {} docs",
        _column, result.getCardinality(), _numDocs);

    return result;
  }

  @SuppressWarnings("unchecked")
  private void considerDocForRadius(ForwardIndexReader rawReader, ForwardIndexReaderContext context, int docId,
      float[] queryVector, float threshold, MutableRoaringBitmap result) {
    float[] docVector = rawReader.getFloatMV(docId, context);
    // Skip null or empty vectors. Note: vector columns are typically not nullable, but
    // the forward index may return an empty array for unset rows. This is consistent with
    // the null handling in VectorSimilarityFilterOperator's rerank/threshold paths.
    if (docVector == null || docVector.length == 0) {
      return;
    }
    float distance = VectorDistanceUtils.computeDistance(queryVector, docVector, _distanceFunction);
    if (distance <= threshold) {
      result.add(docId);
    }
  }

  /// Filters ANN candidates by computing exact distance and checking against the threshold.
  @SuppressWarnings("unchecked")
  private ImmutableRoaringBitmap filterByThreshold(ImmutableRoaringBitmap candidates,
      float[] queryVector, float threshold) {
    MutableRoaringBitmap result = new MutableRoaringBitmap();
    ForwardIndexReader rawReader = _forwardIndexReader;
    try (ForwardIndexReaderContext context = rawReader.createContext()) {
      org.roaringbitmap.IntIterator it = candidates.getIntIterator();
      while (it.hasNext()) {
        int docId = it.next();
        float[] docVector = rawReader.getFloatMV(docId, context);
        if (docVector == null || docVector.length == 0) {
          continue;
        }
        float distance = VectorDistanceUtils.computeDistance(queryVector, docVector, _distanceFunction);
        if (distance <= threshold) {
          result.add(docId);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Error during vector radius filtering on column: " + _column, e);
    }
    return result;
  }

  private void record(ImmutableRoaringBitmap matches) {
    InvocationRecording recording = Tracing.activeRecording();
    if (recording.isEnabled()) {
      recording.setNumDocsMatchingAfterFilter(matches.getCardinality());
      recording.setColumnName(_column);
      recording.setFilter(FilterType.INDEX, "VECTOR_SIMILARITY_RADIUS");
      recording.setInputDataType(FieldSpec.DataType.FLOAT, false);
    }
  }
}
