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
 * Enumerates the supported vector index backend types.
 *
 * <p>Each backend type corresponds to a different approximate nearest-neighbor (ANN) algorithm
 * with its own configuration properties, build characteristics, and query behavior.</p>
 *
 * <ul>
 *   <li>{@link #HNSW} - Hierarchical Navigable Small World graph (Lucene-based). Supports both
 *       mutable and immutable segments.</li>
 *   <li>{@link #IVF_FLAT} - Inverted File with flat (uncompressed) vectors. Supported for
 *       immutable/offline segments only in phase 1.</li>
 * </ul>
 */
public enum VectorBackendType {

  /**
   * Hierarchical Navigable Small World graph index, backed by Apache Lucene.
   *
   * <p>Backend-specific properties:</p>
   * <ul>
   *   <li>{@code maxCon} - maximum connections per node (default: 16)</li>
   *   <li>{@code beamWidth} - beam width during construction (default: 100)</li>
   *   <li>{@code maxDimensions} - maximum vector dimensions</li>
   *   <li>{@code maxBufferSizeMB} - Lucene RAM buffer size</li>
   *   <li>{@code useCompoundFile} - whether to use compound file format</li>
   *   <li>{@code mode} - Lucene codec mode (BEST_SPEED or BEST_COMPRESSION)</li>
   * </ul>
   */
  HNSW("Hierarchical Navigable Small World graph (Lucene-based)"),

  /**
   * Inverted File with flat (uncompressed) vectors.
   *
   * <p>Backend-specific properties:</p>
   * <ul>
   *   <li>{@code nlist} - number of Voronoi cells/clusters (default: 128)</li>
   *   <li>{@code trainSampleSize} - number of vectors sampled for training (default: max(nlist * 40, 10000))</li>
   *   <li>{@code trainingSeed} - random seed for reproducible training</li>
   *   <li>{@code minRowsForIndex} - minimum rows required to build the index</li>
   * </ul>
   */
  IVF_FLAT("Inverted File with flat vectors");

  private final String _description;

  VectorBackendType(String description) {
    _description = description;
  }

  public String getDescription() {
    return _description;
  }

  /**
   * Parses a backend type string in a case-insensitive manner.
   *
   * @param value the string to parse (e.g., "HNSW", "hnsw", "IVF_FLAT")
   * @return the corresponding {@link VectorBackendType}
   * @throws IllegalArgumentException if the value does not match any known backend type
   */
  public static VectorBackendType fromString(String value) {
    if (value == null) {
      throw new IllegalArgumentException("Vector backend type must not be null");
    }
    try {
      return valueOf(value.toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Unknown vector backend type: '" + value + "'. Supported types: HNSW, IVF_FLAT");
    }
  }

  /**
   * Returns true if the given string is a recognized backend type (case-insensitive).
   *
   * @param value the string to check
   * @return true if recognized, false otherwise
   */
  public static boolean isValid(String value) {
    if (value == null) {
      return false;
    }
    try {
      valueOf(value.toUpperCase());
      return true;
    } catch (IllegalArgumentException e) {
      return false;
    }
  }
}
