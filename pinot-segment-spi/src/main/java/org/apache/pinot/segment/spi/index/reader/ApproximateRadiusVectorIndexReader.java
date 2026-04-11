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
 * Extension of {@link VectorIndexReader} for backends that can evaluate a distance threshold
 * directly during ANN candidate generation.
 *
 * <p>The returned document set is approximate and backend-specific. Implementations may still miss
 * true matches due to ANN pruning (for example: HNSW graph traversal limits or IVF nprobe limits).
 * Callers that require exact radius semantics must refine candidates with exact distance checks and
 * apply a correctness fallback when candidate limits are saturated.</p>
 */
public interface ApproximateRadiusVectorIndexReader extends VectorIndexReader {

  /**
   * Returns candidate document IDs whose backend-computed distance is within the threshold.
   *
   * <p>The backend may cap the number of candidates at {@code maxCandidates} for safety.</p>
   *
   * @param vector query vector
   * @param threshold distance threshold
   * @param maxCandidates maximum number of candidate docs to return
   * @return candidate doc IDs within threshold according to backend approximation
   */
  ImmutableRoaringBitmap getDocIdsWithinApproximateRadius(float[] vector, float threshold, int maxCandidates);
}
