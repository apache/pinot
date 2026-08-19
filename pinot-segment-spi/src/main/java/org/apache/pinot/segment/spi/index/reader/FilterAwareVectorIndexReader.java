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

import org.roaringbitmap.buffer.ImmutableRoaringBitmap;


/// Extension of [VectorIndexReader] that supports pre-filter ANN search.
///
/// When a filter bitmap is provided, the ANN search is restricted to only the
/// documents present in the bitmap, improving recall for selective filters compared
/// to the default POST_FILTER_ANN approach (where ANN runs independently and results
/// are intersected with the filter afterward).
///
/// **The filter is a hard correctness contract, not a best-effort optimization.** The query engine relies
/// on filtered search to enforce the upsert doc-ids snapshot: on upsert tables, documents outside the
/// bitmap are obsolete row versions that must never consume top-K candidate slots. An implementation whose
/// [#supportsPreFilter()] returns true therefore commits to returning a strict subset of the given bitmap
/// on every filtered call -- it must never heuristically degrade to unfiltered search (e.g. above some
/// selectivity cutoff). Implementations that can only honor the filter conditionally must return false
/// from [#supportsPreFilter()], in which case the engine falls back to an exact scan.
///
/// Implementations should ensure that the unfiltered [#getDocIds(float[], int)]
/// method continues to work unchanged for backward compatibility.
public interface FilterAwareVectorIndexReader extends VectorIndexReader {

  /// Returns the bitmap of top-K closest vectors from the given vector,
  /// restricted to documents present in the preFilterBitmap.
  ///
  /// The result MUST be a subset of the preFilterBitmap (see the class-level contract).
  ///
  /// @param vector the query vector
  /// @param topK number of closest vectors to return
  /// @param preFilterBitmap bitmap of document IDs to restrict the search to;
  ///                        must not be null (use [#getDocIds(float[], int)] for unfiltered search)
  /// @return bitmap of top-K closest vectors from the filtered document set
  ImmutableRoaringBitmap getDocIds(float[] vector, int topK, ImmutableRoaringBitmap preFilterBitmap);

  /// Returns true if this reader always honors the filter bitmap of
  /// [#getDocIds(float[], int, ImmutableRoaringBitmap)] -- a hard commitment, since the engine uses
  /// filtered search to enforce upsert correctness (see the class-level contract). Return false when the
  /// filter can only be honored for certain filter selectivities or index configurations.
  ///
  /// @return true if filtered search is unconditionally supported
  default boolean supportsPreFilter() {
    return true;
  }
}
