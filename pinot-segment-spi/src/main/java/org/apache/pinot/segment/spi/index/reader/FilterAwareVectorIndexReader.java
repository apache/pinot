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


/**
 * Extension of {@link VectorIndexReader} that supports pre-filter ANN search.
 *
 * <p>When a filter bitmap is provided, the ANN search is restricted to only the
 * documents present in the bitmap, improving recall for selective filters compared
 * to the default POST_FILTER_ANN approach (where ANN runs independently and results
 * are intersected with the filter afterward).</p>
 *
 * <p>Implementations should ensure that the unfiltered {@link #getDocIds(float[], int)}
 * method continues to work unchanged for backward compatibility.</p>
 */
public interface FilterAwareVectorIndexReader extends VectorIndexReader {

  /**
   * Returns the bitmap of top-K closest vectors from the given vector,
   * restricted to documents present in the preFilterBitmap.
   *
   * @param vector the query vector
   * @param topK number of closest vectors to return
   * @param preFilterBitmap bitmap of document IDs to restrict the search to;
   *                        must not be null (use {@link #getDocIds(float[], int)} for unfiltered search)
   * @return bitmap of top-K closest vectors from the filtered document set
   */
  ImmutableRoaringBitmap getDocIds(float[] vector, int topK, ImmutableRoaringBitmap preFilterBitmap);

  /**
   * Returns true if this reader supports efficient pre-filter search.
   * Some implementations may support the interface but only for certain
   * filter selectivities or index configurations.
   *
   * @return true if pre-filter search is supported
   */
  default boolean supportsPreFilter() {
    return true;
  }
}
