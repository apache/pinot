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
package org.apache.pinot.segment.spi.index.creator;


/**
 * Declares the query-time capabilities of a vector index backend.
 *
 * <p>This model allows the query planner and runtime to reason about what a backend can do
 * without backend-specific branching. Each {@link VectorBackendType} returns its own
 * capabilities instance via {@link VectorBackendType#getCapabilities()}.</p>
 *
 * <p>Capability flags are intentionally conservative: a backend should only declare a
 * capability if it can deliver correct results for that mode. The runtime uses these
 * flags to select execution modes and to decide when an exact-scan fallback is required.</p>
 *
 * <p>This class is immutable and thread-safe.</p>
 */
public final class VectorBackendCapabilities {

  private final boolean _supportsTopKAnn;
  private final boolean _supportsFilterAwareSearch;
  private final boolean _supportsApproximateRadius;
  private final boolean _supportsExactRerank;
  private final boolean _supportsRuntimeSearchParams;

  private VectorBackendCapabilities(boolean supportsTopKAnn, boolean supportsFilterAwareSearch,
      boolean supportsApproximateRadius, boolean supportsExactRerank, boolean supportsRuntimeSearchParams) {
    _supportsTopKAnn = supportsTopKAnn;
    _supportsFilterAwareSearch = supportsFilterAwareSearch;
    _supportsApproximateRadius = supportsApproximateRadius;
    _supportsExactRerank = supportsExactRerank;
    _supportsRuntimeSearchParams = supportsRuntimeSearchParams;
  }

  /**
   * Whether the backend can answer top-K approximate nearest-neighbor queries.
   * All current backends support this.
   */
  public boolean supportsTopKAnn() {
    return _supportsTopKAnn;
  }

  /**
   * Whether the backend can internally prune candidates using a metadata filter bitmap
   * during ANN search. When false, filtering must happen as a separate post-ANN step.
   *
   * <p>Currently no backend supports this; the flag exists to enable future backends
   * (e.g., Lucene HNSW with filtered search, or a future IVF variant) without API changes.</p>
   */
  public boolean supportsFilterAwareSearch() {
    return _supportsFilterAwareSearch;
  }

  /**
   * Whether the backend can natively answer approximate radius/threshold queries
   * (returning all vectors within a distance threshold rather than top-K).
   *
   * <p>When false, the runtime must use ANN candidate generation followed by exact
   * threshold refinement.</p>
   */
  public boolean supportsApproximateRadius() {
    return _supportsApproximateRadius;
  }

  /**
   * Whether the backend's ANN results can be refined using exact distance from the
   * forward index. All current backends support this when a forward index is available.
   */
  public boolean supportsExactRerank() {
    return _supportsExactRerank;
  }

  /**
   * Whether the backend accepts runtime search parameters (e.g., nprobe).
   * True for IVF_FLAT and IVF_PQ; false for HNSW.
   */
  public boolean supportsRuntimeSearchParams() {
    return _supportsRuntimeSearchParams;
  }

  @Override
  public String toString() {
    return "VectorBackendCapabilities{"
        + "topKAnn=" + _supportsTopKAnn
        + ", filterAwareSearch=" + _supportsFilterAwareSearch
        + ", approximateRadius=" + _supportsApproximateRadius
        + ", exactRerank=" + _supportsExactRerank
        + ", runtimeSearchParams=" + _supportsRuntimeSearchParams
        + '}';
  }

  /**
   * Builder for constructing capability instances.
   */
  public static final class Builder {
    private boolean _supportsTopKAnn;
    private boolean _supportsFilterAwareSearch;
    private boolean _supportsApproximateRadius;
    private boolean _supportsExactRerank;
    private boolean _supportsRuntimeSearchParams;

    public Builder supportsTopKAnn(boolean value) {
      _supportsTopKAnn = value;
      return this;
    }

    public Builder supportsFilterAwareSearch(boolean value) {
      _supportsFilterAwareSearch = value;
      return this;
    }

    public Builder supportsApproximateRadius(boolean value) {
      _supportsApproximateRadius = value;
      return this;
    }

    public Builder supportsExactRerank(boolean value) {
      _supportsExactRerank = value;
      return this;
    }

    public Builder supportsRuntimeSearchParams(boolean value) {
      _supportsRuntimeSearchParams = value;
      return this;
    }

    public VectorBackendCapabilities build() {
      return new VectorBackendCapabilities(_supportsTopKAnn, _supportsFilterAwareSearch,
          _supportsApproximateRadius, _supportsExactRerank, _supportsRuntimeSearchParams);
    }
  }
}
