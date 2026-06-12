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
package org.apache.pinot.segment.spi.index.reader;

/**
 * Marker interface for vector index readers that support configurable efSearch parameter.
 *
 * <p>HNSW indexes use the {@code efSearch} parameter to control the search quality at query time.
 * Higher values improve recall at the cost of latency by exploring more nodes in the graph.</p>
 *
 * <p>Implementations should treat {@link #setEfSearch(int)} as query-scoped state and support
 * {@link #clearEfSearch()} after the query finishes. Pinot's vector operators set efSearch once
 * before the ANN lookup and clear it in a {@code finally} block.</p>
 */
public interface EfSearchAware {

  /**
   * Sets the efSearch parameter for the next search.
   *
   * @param efSearch number of candidates to track during search (must be &gt;= 1)
   * @throws IllegalArgumentException if efSearch is less than 1
   */
  void setEfSearch(int efSearch);

  /**
   * Clears any query-scoped efSearch override and restores the reader's default behavior.
   *
   * <p>The default implementation is a no-op for readers that do not retain per-query state.</p>
   */
  default void clearEfSearch() {
  }

  /**
   * Enables or disables relative-distance competitive checks for the next HNSW search.
   *
   * <p>When enabled, HNSW traversal can prune candidate exploration based on the current
   * competitive similarity threshold. When disabled, no score-threshold pruning is applied.</p>
   */
  default void setUseRelativeDistance(boolean useRelativeDistance) {
  }

  /**
   * Clears any query-scoped override set via {@link #setUseRelativeDistance(boolean)}.
   */
  default void clearUseRelativeDistance() {
  }

  /**
   * Enables or disables bounded queue behavior for the next HNSW search.
   *
   * <p>Bounded queue mode keeps only the current top-K candidates; unbounded mode retains all
   * collected candidates up to the visit limit.</p>
   */
  default void setUseBoundedQueue(boolean useBoundedQueue) {
  }

  /**
   * Clears any query-scoped override set via {@link #setUseBoundedQueue(boolean)}.
   */
  default void clearUseBoundedQueue() {
  }
}
