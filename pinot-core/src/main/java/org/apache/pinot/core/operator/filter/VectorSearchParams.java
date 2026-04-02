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

import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;


/**
 * Immutable parameter object carrying vector search query options from the broker through to
 * segment-level execution. Constructed once per query from {@code QueryContext.getQueryOptions()}.
 *
 * <p>All fields have sensible defaults so that queries without vector-specific options behave
 * identically to the existing HNSW path (backward compatible).</p>
 *
 * <p>This class is thread-safe (immutable).</p>
 */
public final class VectorSearchParams {

  /** Default nprobe for IVF_FLAT when the query option is not set. */
  public static final int DEFAULT_NPROBE = 4;

  /** Singleton instance with all defaults, used when no query options are specified. */
  public static final VectorSearchParams DEFAULT = new VectorSearchParams(null, false, null);

  private final int _nprobe;
  private final boolean _exactRerank;
  private final int _maxCandidates;
  private final boolean _maxCandidatesExplicit;

  /**
   * Constructs search params from raw query option values.
   *
   * @param nprobe number of IVF probes, or null for default
   * @param exactRerank whether to re-score ANN candidates with exact distance
   * @param maxCandidates max candidates before final top-K, or null for default (topK * 10)
   */
  public VectorSearchParams(@Nullable Integer nprobe, boolean exactRerank, @Nullable Integer maxCandidates) {
    _nprobe = nprobe != null ? nprobe : DEFAULT_NPROBE;
    _exactRerank = exactRerank;
    _maxCandidates = maxCandidates != null ? maxCandidates : 0;
    _maxCandidatesExplicit = maxCandidates != null;
  }

  /**
   * Creates a {@link VectorSearchParams} from the query options map, extracting all
   * vector-specific options. Returns {@link #DEFAULT} if no vector options are present.
   *
   * @param queryOptions the query options map (may be null or empty)
   * @return the search params
   */
  public static VectorSearchParams fromQueryOptions(@Nullable Map<String, String> queryOptions) {
    if (queryOptions == null || queryOptions.isEmpty()) {
      return DEFAULT;
    }

    Integer nprobe = QueryOptionsUtils.getVectorNprobe(queryOptions);
    boolean exactRerank = QueryOptionsUtils.isVectorExactRerank(queryOptions);
    Integer maxCandidates = QueryOptionsUtils.getVectorMaxCandidates(queryOptions);

    if (nprobe == null && !exactRerank && maxCandidates == null) {
      return DEFAULT;
    }

    return new VectorSearchParams(nprobe, exactRerank, maxCandidates);
  }

  /**
   * Returns the nprobe value for IVF_FLAT index search.
   */
  public int getNprobe() {
    return _nprobe;
  }

  /**
   * Returns whether exact rerank is enabled.
   */
  public boolean isExactRerank() {
    return _exactRerank;
  }

  /**
   * Returns the effective max candidates for a given top-K value.
   * If maxCandidates was explicitly set, returns that value.
   * Otherwise, returns {@code topK * 10} as the default.
   *
   * @param topK the top-K value from the predicate
   * @return effective max candidates
   */
  public int getEffectiveMaxCandidates(int topK) {
    if (_maxCandidatesExplicit) {
      return Math.max(_maxCandidates, topK);
    }
    return topK * 10;
  }

  /**
   * Returns whether maxCandidates was explicitly set by the user.
   */
  public boolean isMaxCandidatesExplicit() {
    return _maxCandidatesExplicit;
  }

  @Override
  public String toString() {
    return "VectorSearchParams{nprobe=" + _nprobe + ", exactRerank=" + _exactRerank
        + ", maxCandidates=" + (_maxCandidatesExplicit ? _maxCandidates : "default(topK*10)") + '}';
  }
}
