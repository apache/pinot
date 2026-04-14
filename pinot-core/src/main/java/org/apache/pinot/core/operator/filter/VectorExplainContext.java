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
import org.apache.pinot.segment.spi.index.creator.VectorExecutionMode;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;


/**
 * Runtime-visible vector metadata for explain and debug output.
 *
 * <p>Includes the {@link VectorExecutionMode} so that explain output makes the execution
 * strategy explicit and understandable.</p>
 */
public final class VectorExplainContext {
  private final VectorBackendType _backendType;
  private final VectorIndexConfig.VectorDistanceFunction _distanceFunction;
  private final VectorExecutionMode _executionMode;
  private final int _effectiveNprobe;
  private final boolean _effectiveExactRerank;
  private final int _effectiveSearchCount;
  @Nullable
  private final String _fallbackReason;
  @Nullable
  private final String _searchMode;
  private final int _effectiveEfSearch;
  private final float _effectiveThreshold;
  private final VectorSearchMode _vectorSearchMode;
  private final double _filterSelectivity;
  @Nullable
  private final Boolean _effectiveHnswUseRelativeDistance;
  @Nullable
  private final Boolean _effectiveHnswUseBoundedQueue;

  /**
   * Full constructor with all fields including HNSW runtime control metadata.
   */
  public VectorExplainContext(VectorBackendType backendType,
      VectorIndexConfig.VectorDistanceFunction distanceFunction, @Nullable VectorExecutionMode executionMode,
      int effectiveNprobe, boolean effectiveExactRerank, int effectiveSearchCount,
      @Nullable String fallbackReason, @Nullable String searchMode,
      int effectiveEfSearch, float effectiveThreshold, VectorSearchMode vectorSearchMode,
      double filterSelectivity, @Nullable Boolean effectiveHnswUseRelativeDistance,
      @Nullable Boolean effectiveHnswUseBoundedQueue) {
    _backendType = backendType;
    _distanceFunction = distanceFunction;
    _executionMode = executionMode;
    _effectiveNprobe = effectiveNprobe;
    _effectiveExactRerank = effectiveExactRerank;
    _effectiveSearchCount = effectiveSearchCount;
    _fallbackReason = fallbackReason;
    _searchMode = searchMode;
    _effectiveEfSearch = effectiveEfSearch;
    _effectiveThreshold = effectiveThreshold;
    _vectorSearchMode = vectorSearchMode;
    _filterSelectivity = filterSelectivity;
    _effectiveHnswUseRelativeDistance = effectiveHnswUseRelativeDistance;
    _effectiveHnswUseBoundedQueue = effectiveHnswUseBoundedQueue;
  }

  public VectorBackendType getBackendType() {
    return _backendType;
  }

  public VectorIndexConfig.VectorDistanceFunction getDistanceFunction() {
    return _distanceFunction;
  }

  /**
   * Returns the execution mode selected for this query, or null if not yet determined.
   */
  @Nullable
  public VectorExecutionMode getExecutionMode() {
    return _executionMode;
  }

  public int getEffectiveNprobe() {
    return _effectiveNprobe;
  }

  public boolean isEffectiveExactRerank() {
    return _effectiveExactRerank;
  }

  public int getEffectiveSearchCount() {
    return _effectiveSearchCount;
  }

  @Nullable
  public String getFallbackReason() {
    return _fallbackReason;
  }

  /**
   * Returns the search mode label for the adaptive planner, or {@code null} if not set.
   */
  @Nullable
  public String getSearchMode() {
    return _searchMode;
  }

  /**
   * Returns the effective efSearch value. Returns 0 if not applicable (non-HNSW backends).
   */
  public int getEffectiveEfSearch() {
    return _effectiveEfSearch;
  }

  /**
   * Returns the effective distance threshold, or -1 if not set.
   */
  public float getEffectiveThreshold() {
    return _effectiveThreshold;
  }

  /**
   * Returns the vector search mode enum describing how ANN interacts with filters.
   */
  public VectorSearchMode getVectorSearchMode() {
    return _vectorSearchMode;
  }

  /**
   * Returns the filter selectivity as a ratio (0.0 to 1.0), or -1 if no filter is applied.
   * A selectivity of 0.1 means 10% of documents pass the filter.
   */
  public double getFilterSelectivity() {
    return _filterSelectivity;
  }

  /**
   * Returns the effective HNSW relative-distance check mode, or null if not applicable.
   */
  @Nullable
  public Boolean getEffectiveHnswUseRelativeDistance() {
    return _effectiveHnswUseRelativeDistance;
  }

  /**
   * Returns the effective HNSW bounded queue mode, or null if not applicable.
   */
  @Nullable
  public Boolean getEffectiveHnswUseBoundedQueue() {
    return _effectiveHnswUseBoundedQueue;
  }
}
