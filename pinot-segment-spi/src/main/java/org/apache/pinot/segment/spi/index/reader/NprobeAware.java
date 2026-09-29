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

/// Marker interface for vector index readers that support configurable nprobe parameter.
///
/// IVF_FLAT indexes partition vectors into `nlist` Voronoi cells during build time.
/// At query time, the `nprobe` parameter controls how many cells are searched. Higher
/// values improve recall at the cost of latency.
///
/// Implementations should treat [#setNprobe(int)] as query-scoped state and support
/// [#clearNprobe()] after the query finishes. Pinot's vector operators set nprobe once
/// before the ANN lookup and clear it in a `finally` block.
public interface NprobeAware {

  /// Sets the number of inverted-list probes for the next search.
  ///
  /// @param nprobe number of cells to probe (must be &gt;= 1)
  /// @throws IllegalArgumentException if nprobe is less than 1
  void setNprobe(int nprobe);

  /// Clears any query-scoped nprobe override and restores the reader's default behavior.
  ///
  /// The default implementation is a no-op for readers that do not retain per-query state.
  default void clearNprobe() {
  }
}
