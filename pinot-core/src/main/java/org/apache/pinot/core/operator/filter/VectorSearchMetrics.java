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

import java.util.Collections;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.index.creator.VectorBackendType;


/**
 * Collects aggregate vector search metrics for observability and debugging.
 *
 * <p>Tracks counters for:</p>
 * <ul>
 *   <li>Search mode usage (POST_FILTER_ANN, FILTER_THEN_ANN, EXACT_SCAN)</li>
 *   <li>Backend type usage (HNSW, IVF_FLAT, IVF_PQ, IVF_ON_DISK)</li>
 *   <li>Fallback events by reason</li>
 *   <li>Candidate and rerank budget distributions</li>
 *   <li>Threshold/radius search counts</li>
 *   <li>HNSW efSearch usage</li>
 * </ul>
 *
 * <p>Thread-safe. Uses LongAdder for high-contention counters.</p>
 *
 * <p>This class is a singleton per server instance. Metrics are cumulative since server start
 * and intended to be scraped by a metrics reporter (e.g., Dropwizard/Yammer).</p>
 */
public final class VectorSearchMetrics {

  private static final VectorSearchMetrics INSTANCE = new VectorSearchMetrics();

  /** Maximum number of distinct fallback reasons to track (prevents unbounded map growth). */
  private static final int MAX_FALLBACK_REASONS = 100;

  // Search mode counters
  private final EnumMap<VectorSearchMode, LongAdder> _modeCounts;

  // Backend type counters
  private final EnumMap<VectorBackendType, LongAdder> _backendCounts;

  // Fallback counters by reason
  private final ConcurrentHashMap<String, LongAdder> _fallbackCounts;

  // Aggregate counters
  private final LongAdder _totalSearches = new LongAdder();
  private final LongAdder _totalFallbacks = new LongAdder();
  private final LongAdder _radiusSearches = new LongAdder();
  private final LongAdder _efSearchOverrides = new LongAdder();
  private final LongAdder _preFilterSearches = new LongAdder();

  // Budget tracking (running sums for average computation)
  private final LongAdder _candidateBudgetSum = new LongAdder();
  private final LongAdder _candidateBudgetCount = new LongAdder();
  private final LongAdder _rerankBudgetSum = new LongAdder();
  private final LongAdder _rerankBudgetCount = new LongAdder();

  private VectorSearchMetrics() {
    _modeCounts = new EnumMap<>(VectorSearchMode.class);
    for (VectorSearchMode mode : VectorSearchMode.values()) {
      _modeCounts.put(mode, new LongAdder());
    }

    _backendCounts = new EnumMap<>(VectorBackendType.class);
    for (VectorBackendType backend : VectorBackendType.values()) {
      _backendCounts.put(backend, new LongAdder());
    }

    _fallbackCounts = new ConcurrentHashMap<>();
  }

  public static VectorSearchMetrics getInstance() {
    return INSTANCE;
  }

  // -----------------------------------------------------------------------
  // Recording methods
  // -----------------------------------------------------------------------

  /**
   * Records a vector search execution.
   *
   * @param mode the search mode used
   * @param backendType the backend type (may be null for exact scan fallback)
   */
  public void recordSearch(VectorSearchMode mode, @Nullable VectorBackendType backendType) {
    _totalSearches.increment();
    _modeCounts.get(mode).increment();
    if (backendType != null) {
      _backendCounts.get(backendType).increment();
    }
    if (mode == VectorSearchMode.FILTER_THEN_ANN) {
      _preFilterSearches.increment();
    }
  }

  /**
   * Records a fallback to exact scan.
   *
   * @param reason the fallback reason
   */
  public void recordFallback(String reason) {
    _totalFallbacks.increment();
    if (_fallbackCounts.size() < MAX_FALLBACK_REASONS) {
      _fallbackCounts.computeIfAbsent(reason, k -> new LongAdder()).increment();
    } else {
      // Cap reached — only increment existing keys to prevent unbounded growth
      LongAdder existing = _fallbackCounts.get(reason);
      if (existing != null) {
        existing.increment();
      }
    }
  }

  /**
   * Records a radius/threshold search.
   */
  public void recordRadiusSearch() {
    _radiusSearches.increment();
  }

  /**
   * Records an efSearch override.
   */
  public void recordEfSearchOverride() {
    _efSearchOverrides.increment();
  }

  /**
   * Records candidate and rerank budgets for a search.
   */
  public void recordBudgets(int candidateBudget, int rerankBudget) {
    _candidateBudgetSum.add(candidateBudget);
    _candidateBudgetCount.increment();
    if (rerankBudget > 0) {
      _rerankBudgetSum.add(rerankBudget);
      _rerankBudgetCount.increment();
    }
  }

  // -----------------------------------------------------------------------
  // Reading methods
  // -----------------------------------------------------------------------

  public long getTotalSearches() {
    return _totalSearches.sum();
  }

  public long getTotalFallbacks() {
    return _totalFallbacks.sum();
  }

  public long getRadiusSearches() {
    return _radiusSearches.sum();
  }

  public long getEfSearchOverrides() {
    return _efSearchOverrides.sum();
  }

  public long getPreFilterSearches() {
    return _preFilterSearches.sum();
  }

  public long getModeCount(VectorSearchMode mode) {
    return _modeCounts.get(mode).sum();
  }

  public long getBackendCount(VectorBackendType backend) {
    return _backendCounts.get(backend).sum();
  }

  public Map<String, LongAdder> getFallbackCounts() {
    return Collections.unmodifiableMap(_fallbackCounts);
  }

  public double getAverageCandidateBudget() {
    long count = _candidateBudgetCount.sum();
    return count > 0 ? (double) _candidateBudgetSum.sum() / count : 0.0;
  }

  public double getAverageRerankBudget() {
    long count = _rerankBudgetCount.sum();
    return count > 0 ? (double) _rerankBudgetSum.sum() / count : 0.0;
  }

  /**
   * Returns a snapshot of all metrics as a map (for debug endpoints).
   */
  public Map<String, Object> toMap() {
    Map<String, Object> map = new LinkedHashMap<>();
    map.put("totalSearches", getTotalSearches());
    map.put("totalFallbacks", getTotalFallbacks());
    map.put("radiusSearches", getRadiusSearches());
    map.put("efSearchOverrides", getEfSearchOverrides());
    map.put("preFilterSearches", getPreFilterSearches());
    map.put("avgCandidateBudget", getAverageCandidateBudget());
    map.put("avgRerankBudget", getAverageRerankBudget());

    Map<String, Long> modeMap = new LinkedHashMap<>();
    for (VectorSearchMode mode : VectorSearchMode.values()) {
      modeMap.put(mode.name(), getModeCount(mode));
    }
    map.put("modeCounts", modeMap);

    Map<String, Long> backendMap = new LinkedHashMap<>();
    for (VectorBackendType backend : VectorBackendType.values()) {
      backendMap.put(backend.name(), getBackendCount(backend));
    }
    map.put("backendCounts", backendMap);

    Map<String, Long> fallbackMap = new LinkedHashMap<>();
    for (Map.Entry<String, LongAdder> entry : _fallbackCounts.entrySet()) {
      fallbackMap.put(entry.getKey(), entry.getValue().sum());
    }
    map.put("fallbackCounts", fallbackMap);
    return map;
  }

  /**
   * Resets all counters. Intended for testing only.
   */
  @com.google.common.annotations.VisibleForTesting
  public void reset() {
    _totalSearches.reset();
    _totalFallbacks.reset();
    _radiusSearches.reset();
    _efSearchOverrides.reset();
    _preFilterSearches.reset();
    _candidateBudgetSum.reset();
    _candidateBudgetCount.reset();
    _rerankBudgetSum.reset();
    _rerankBudgetCount.reset();
    _modeCounts.values().forEach(LongAdder::reset);
    _backendCounts.values().forEach(LongAdder::reset);
    _fallbackCounts.clear();
  }
}
