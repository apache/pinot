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

  public VectorExplainContext(VectorBackendType backendType,
      VectorIndexConfig.VectorDistanceFunction distanceFunction, int effectiveNprobe, boolean effectiveExactRerank,
      int effectiveSearchCount, @Nullable String fallbackReason) {
    this(backendType, distanceFunction, null, effectiveNprobe, effectiveExactRerank, effectiveSearchCount,
        fallbackReason);
  }

  public VectorExplainContext(VectorBackendType backendType,
      VectorIndexConfig.VectorDistanceFunction distanceFunction, @Nullable VectorExecutionMode executionMode,
      int effectiveNprobe, boolean effectiveExactRerank, int effectiveSearchCount,
      @Nullable String fallbackReason) {
    _backendType = backendType;
    _distanceFunction = distanceFunction;
    _executionMode = executionMode;
    _effectiveNprobe = effectiveNprobe;
    _effectiveExactRerank = effectiveExactRerank;
    _effectiveSearchCount = effectiveSearchCount;
    _fallbackReason = fallbackReason;
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
}
