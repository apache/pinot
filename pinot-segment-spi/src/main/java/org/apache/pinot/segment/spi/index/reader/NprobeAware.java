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
 * Marker interface for vector index readers that support configurable nprobe parameter.
 *
 * <p>IVF_FLAT indexes partition vectors into {@code nlist} Voronoi cells during build time.
 * At query time, the {@code nprobe} parameter controls how many cells are searched. Higher
 * values improve recall at the cost of latency.</p>
 *
 * <p>Implementations should be thread-safe with respect to concurrent calls to
 * {@link VectorIndexReader#getDocIds(float[], int)} after {@link #setNprobe(int)} has been called.
 * In practice, nprobe is set once per query before any search call.</p>
 */
public interface NprobeAware {

  /**
   * Sets the number of inverted-list probes for the next search.
   *
   * @param nprobe number of cells to probe (must be &gt;= 1)
   * @throws IllegalArgumentException if nprobe is less than 1
   */
  void setNprobe(int nprobe);
}
