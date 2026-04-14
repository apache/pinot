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

import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.index.creator.VectorBackendType;


/**
 * Selectivity-aware planner that decides the optimal vector search execution mode.
 *
 * <p>The planner considers filter selectivity, index capabilities (pre-filter support),
 * segment type (mutable vs immutable), and query shape to choose between:</p>
 * <ul>
 *   <li>{@link VectorSearchMode#POST_FILTER_ANN} — default, best for low-selectivity filters</li>
 *   <li>{@link VectorSearchMode#FILTER_THEN_ANN} — best for highly selective filters</li>
 *   <li>{@link VectorSearchMode#EXACT_SCAN} — best when no index or extremely selective filter</li>
 * </ul>
 *
 * <p>All decisions are explainable: the {@link Decision} object carries the chosen mode,
 * the reason string, and the selectivity ratio for explain/debug output.</p>
 *
 * <h3>Selectivity thresholds</h3>
 * <ul>
 *   <li>selectivity &lt; 0.01 (1%) → FILTER_THEN_ANN or EXACT_SCAN</li>
 *   <li>selectivity &gt; 0.20 (20%) → POST_FILTER_ANN</li>
 *   <li>Middle range → cost-model comparison</li>
 * </ul>
 *
 * <p>Thread-safe: all methods are stateless and static.</p>
 */
public final class VectorSearchStrategy {

  /** Below this selectivity ratio, prefer pre-filter ANN or exact scan. */
  static final double HIGH_SELECTIVITY_THRESHOLD = 0.01;

  /** Above this selectivity ratio, prefer post-filter ANN. */
  static final double LOW_SELECTIVITY_THRESHOLD = 0.20;

  /** Below this number of filtered docs, exact scan is cheaper than ANN overhead. */
  static final int EXACT_SCAN_THRESHOLD = 1000;

  /** Minimum segment size where ANN provides benefit over linear scan. */
  static final int MIN_ANN_SEGMENT_SIZE = 500;

  private VectorSearchStrategy() {
  }

  /**
   * Decides the optimal search mode for a vector query with an optional filter.
   *
   * @param numDocs total docs in the segment
   * @param estimatedFilteredDocs estimated docs passing the filter (numDocs if no filter)
   * @param hasVectorIndex whether a vector index exists for this column
   * @param indexSupportsPreFilter whether the index implements FilterAwareVectorIndexReader
   * @param isMutableSegment whether this is a mutable (realtime) segment
   * @param backendType the vector backend type
   * @param searchParams the query search parameters
   * @return the decision with mode, reason, and metadata
   */
  public static Decision decide(int numDocs, int estimatedFilteredDocs, boolean hasVectorIndex,
      boolean indexSupportsPreFilter, boolean isMutableSegment, @Nullable VectorBackendType backendType,
      @Nullable VectorSearchParams searchParams) {

    // No index → always exact scan
    if (!hasVectorIndex) {
      return new Decision(VectorSearchMode.EXACT_SCAN, -1.0,
          "no_vector_index", numDocs, estimatedFilteredDocs);
    }

    // Very small segment → exact scan is cheaper
    if (numDocs < MIN_ANN_SEGMENT_SIZE) {
      return new Decision(VectorSearchMode.EXACT_SCAN, -1.0,
          "segment_too_small (numDocs=" + numDocs + " < " + MIN_ANN_SEGMENT_SIZE + ")",
          numDocs, estimatedFilteredDocs);
    }

    // No filter → standard ANN
    boolean hasFilter = estimatedFilteredDocs < numDocs;
    if (!hasFilter) {
      return new Decision(VectorSearchMode.POST_FILTER_ANN, 1.0,
          "no_filter", numDocs, estimatedFilteredDocs);
    }

    double selectivity = numDocs > 0 ? (double) estimatedFilteredDocs / numDocs : 1.0;

    // Very few docs pass filter → exact scan on filtered set
    if (estimatedFilteredDocs < EXACT_SCAN_THRESHOLD) {
      return new Decision(VectorSearchMode.EXACT_SCAN, selectivity,
          "filter_too_selective (filteredDocs=" + estimatedFilteredDocs + " < " + EXACT_SCAN_THRESHOLD + ")",
          numDocs, estimatedFilteredDocs);
    }

    // Highly selective filter → pre-filter ANN if supported
    if (selectivity < HIGH_SELECTIVITY_THRESHOLD) {
      if (indexSupportsPreFilter && !isMutableSegment) {
        return new Decision(VectorSearchMode.FILTER_THEN_ANN, selectivity,
            "high_selectivity (ratio=" + String.format("%.4f", selectivity) + " < "
                + HIGH_SELECTIVITY_THRESHOLD + ")",
            numDocs, estimatedFilteredDocs);
      }
      // Pre-filter not supported → fall back to exact scan on filtered set
      return new Decision(VectorSearchMode.EXACT_SCAN, selectivity,
          "high_selectivity_no_prefilter_support (ratio=" + String.format("%.4f", selectivity) + ")",
          numDocs, estimatedFilteredDocs);
    }

    // Low selectivity → post-filter ANN
    if (selectivity > LOW_SELECTIVITY_THRESHOLD) {
      return new Decision(VectorSearchMode.POST_FILTER_ANN, selectivity,
          "low_selectivity (ratio=" + String.format("%.4f", selectivity) + " > "
              + LOW_SELECTIVITY_THRESHOLD + ")",
          numDocs, estimatedFilteredDocs);
    }

    // Middle range: cost-model comparison
    // Cost of POST_FILTER_ANN: ANN(topK*overshoot) + filter intersection
    // Cost of FILTER_THEN_ANN: filter evaluation + ANN(topK) on filtered set
    // Heuristic: pre-filter is better when the ANN has to oversample significantly
    // to compensate for filter loss
    if (indexSupportsPreFilter && !isMutableSegment) {
      // In the middle range, prefer pre-filter when selectivity is in the lower half
      double midpoint = (HIGH_SELECTIVITY_THRESHOLD + LOW_SELECTIVITY_THRESHOLD) / 2;
      if (selectivity < midpoint) {
        return new Decision(VectorSearchMode.FILTER_THEN_ANN, selectivity,
            "cost_model_prefilter (ratio=" + String.format("%.4f", selectivity) + " < midpoint="
                + String.format("%.4f", midpoint) + ")",
            numDocs, estimatedFilteredDocs);
      }
    }

    return new Decision(VectorSearchMode.POST_FILTER_ANN, selectivity,
        "cost_model_postfilter (ratio=" + String.format("%.4f", selectivity) + ")",
        numDocs, estimatedFilteredDocs);
  }

  /**
   * Immutable decision result from the adaptive planner.
   * Carries the chosen mode, reason, and metadata for explain output.
   */
  public static final class Decision {
    private final VectorSearchMode _mode;
    private final double _filterSelectivity;
    private final String _reason;
    private final int _numDocs;
    private final int _estimatedFilteredDocs;

    public Decision(VectorSearchMode mode, double filterSelectivity, String reason,
        int numDocs, int estimatedFilteredDocs) {
      _mode = mode;
      _filterSelectivity = filterSelectivity;
      _reason = reason;
      _numDocs = numDocs;
      _estimatedFilteredDocs = estimatedFilteredDocs;
    }

    public VectorSearchMode getMode() {
      return _mode;
    }

    /**
     * Returns the filter selectivity ratio (0.0–1.0), or -1 if no filter is present.
     */
    public double getFilterSelectivity() {
      return _filterSelectivity;
    }

    /**
     * Returns a human-readable reason explaining why this mode was chosen.
     */
    public String getReason() {
      return _reason;
    }

    public int getNumDocs() {
      return _numDocs;
    }

    public int getEstimatedFilteredDocs() {
      return _estimatedFilteredDocs;
    }

    @Override
    public String toString() {
      return "Decision{mode=" + _mode + ", selectivity=" + _filterSelectivity
          + ", reason='" + _reason + "', numDocs=" + _numDocs
          + ", filteredDocs=" + _estimatedFilteredDocs + '}';
    }
  }
}
