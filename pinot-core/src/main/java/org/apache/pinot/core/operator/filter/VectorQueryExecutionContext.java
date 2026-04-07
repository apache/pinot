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
import org.apache.pinot.segment.spi.index.creator.VectorBackendCapabilities;
import org.apache.pinot.segment.spi.index.creator.VectorBackendType;
import org.apache.pinot.segment.spi.index.creator.VectorExecutionMode;


/**
 * Captures the runtime execution plan for a vector query within a single segment.
 *
 * <p>This context is populated by the query planner (in {@code FilterPlanNode}) and carried
 * through to the filter operator so that execution decisions are explicit and reportable.
 * It is also surfaced in explain/debug output.</p>
 *
 * <p>The context is immutable once created. Use the {@link Builder} to construct instances.</p>
 */
public final class VectorQueryExecutionContext {

  private final VectorExecutionMode _executionMode;
  private final VectorBackendType _backendType;
  private final VectorBackendCapabilities _capabilities;
  private final boolean _exactRerank;
  private final boolean _hasMetadataFilter;
  private final boolean _hasThresholdPredicate;
  private final int _topK;
  private final int _candidateBudget;
  private final float _distanceThreshold;
  @Nullable
  private final String _fallbackReason;

  private VectorQueryExecutionContext(Builder builder) {
    _executionMode = builder._executionMode;
    _backendType = builder._backendType;
    _capabilities = builder._capabilities;
    _exactRerank = builder._exactRerank;
    _hasMetadataFilter = builder._hasMetadataFilter;
    _hasThresholdPredicate = builder._hasThresholdPredicate;
    _topK = builder._topK;
    _candidateBudget = builder._candidateBudget;
    _distanceThreshold = builder._distanceThreshold;
    _fallbackReason = builder._fallbackReason;
  }

  public VectorExecutionMode getExecutionMode() {
    return _executionMode;
  }

  public VectorBackendType getBackendType() {
    return _backendType;
  }

  public VectorBackendCapabilities getCapabilities() {
    return _capabilities;
  }

  public boolean isExactRerank() {
    return _exactRerank;
  }

  public boolean hasMetadataFilter() {
    return _hasMetadataFilter;
  }

  public boolean hasThresholdPredicate() {
    return _hasThresholdPredicate;
  }

  public int getTopK() {
    return _topK;
  }

  public int getCandidateBudget() {
    return _candidateBudget;
  }

  public float getDistanceThreshold() {
    return _distanceThreshold;
  }

  @Nullable
  public String getFallbackReason() {
    return _fallbackReason;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("VectorQueryExecutionContext{");
    sb.append("mode=").append(_executionMode);
    sb.append(", backend=").append(_backendType);
    sb.append(", topK=").append(_topK);
    if (_hasMetadataFilter) {
      sb.append(", hasFilter=true");
    }
    if (_hasThresholdPredicate) {
      sb.append(", threshold=").append(_distanceThreshold);
    }
    if (_exactRerank) {
      sb.append(", rerank=true, candidateBudget=").append(_candidateBudget);
    }
    if (_fallbackReason != null) {
      sb.append(", fallbackReason=").append(_fallbackReason);
    }
    sb.append('}');
    return sb.toString();
  }

  /**
   * Selects the execution mode for a vector query based on query shape.
   *
   * <p>This is the centralized decision point. The rules are conservative and explicit:</p>
   * <ol>
   *   <li>No vector index -> EXACT_SCAN</li>
   *   <li>Threshold query + filter -> ANN_THRESHOLD_THEN_FILTER</li>
   *   <li>Threshold query (no filter) -> ANN_THRESHOLD_SCAN</li>
   *   <li>Filter + rerank -> ANN_THEN_FILTER_THEN_RERANK</li>
   *   <li>Filter (no rerank) -> ANN_THEN_FILTER</li>
   *   <li>Rerank (no filter) -> ANN_TOP_K_WITH_RERANK</li>
   *   <li>Plain top-K -> ANN_TOP_K</li>
   * </ol>
   */
  public static VectorExecutionMode selectExecutionMode(boolean hasVectorIndex,
      boolean hasMetadataFilter, boolean hasThresholdPredicate, boolean exactRerank) {
    if (!hasVectorIndex) {
      return VectorExecutionMode.EXACT_SCAN;
    }
    if (hasThresholdPredicate && hasMetadataFilter) {
      return VectorExecutionMode.ANN_THRESHOLD_THEN_FILTER;
    }
    if (hasThresholdPredicate) {
      return VectorExecutionMode.ANN_THRESHOLD_SCAN;
    }
    if (hasMetadataFilter && exactRerank) {
      return VectorExecutionMode.ANN_THEN_FILTER_THEN_RERANK;
    }
    if (hasMetadataFilter) {
      return VectorExecutionMode.ANN_THEN_FILTER;
    }
    if (exactRerank) {
      return VectorExecutionMode.ANN_TOP_K_WITH_RERANK;
    }
    return VectorExecutionMode.ANN_TOP_K;
  }

  public static final class Builder {
    private VectorExecutionMode _executionMode = VectorExecutionMode.ANN_TOP_K;
    private VectorBackendType _backendType = VectorBackendType.HNSW;
    private VectorBackendCapabilities _capabilities;
    private boolean _exactRerank;
    private boolean _hasMetadataFilter;
    private boolean _hasThresholdPredicate;
    private int _topK;
    private int _candidateBudget;
    private float _distanceThreshold = Float.NaN;
    private String _fallbackReason;

    public Builder executionMode(VectorExecutionMode mode) {
      _executionMode = mode;
      return this;
    }

    public Builder backendType(VectorBackendType type) {
      _backendType = type;
      _capabilities = type.getCapabilities();
      return this;
    }

    public Builder capabilities(VectorBackendCapabilities capabilities) {
      _capabilities = capabilities;
      return this;
    }

    public Builder exactRerank(boolean value) {
      _exactRerank = value;
      return this;
    }

    public Builder hasMetadataFilter(boolean value) {
      _hasMetadataFilter = value;
      return this;
    }

    public Builder hasThresholdPredicate(boolean value) {
      _hasThresholdPredicate = value;
      return this;
    }

    public Builder topK(int value) {
      _topK = value;
      return this;
    }

    public Builder candidateBudget(int value) {
      _candidateBudget = value;
      return this;
    }

    public Builder distanceThreshold(float value) {
      _distanceThreshold = value;
      return this;
    }

    public Builder fallbackReason(String reason) {
      _fallbackReason = reason;
      return this;
    }

    public VectorQueryExecutionContext build() {
      if (_capabilities == null && _backendType != null) {
        _capabilities = _backendType.getCapabilities();
      }
      return new VectorQueryExecutionContext(this);
    }
  }
}
