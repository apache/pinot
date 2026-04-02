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
package org.apache.pinot.segment.local.segment.creator.impl.vector.pq;

/**
 * Binary file format for IVF_PQ vector indexes.
 *
 * <p>Layout (all values little-endian):
 * <pre>
 *   HEADER:
 *     magic (4 bytes)          = 0x49505100 ("IPQ\0")
 *     version (4 bytes)        = 1
 *     dimension (4 bytes)
 *     nlist (4 bytes)          = number of IVF coarse centroids
 *     pqM (4 bytes)            = number of PQ subspaces
 *     pqNbits (4 bytes)        = bits per PQ code
 *     distanceFunction (4 bytes) = 0=COSINE, 1=EUCLIDEAN, 2=INNER_PRODUCT
 *     numVectors (4 bytes)     = total vectors indexed
 *
 *   COARSE CENTROIDS:
 *     float[nlist][dimension]   = coarse centroid vectors
 *
 *   PQ CODEBOOKS:
 *     float[pqM][ksub][dsub]   = PQ sub-quantizer centroids
 *       where ksub = 2^pqNbits, dsub = dimension/pqM
 *
 *   INVERTED LISTS:
 *     For each list i in [0, nlist):
 *       listSize (4 bytes)     = number of vectors in this list
 *       docIds (int[listSize]) = Pinot document IDs
 *       pqCodes (byte[listSize][pqM]) = PQ codes for each vector
 *       originalVectors (float[listSize][dimension]) = original vectors for exact rerank
 * </pre>
 */
public final class IvfPqIndexFormat {
  public static final int MAGIC = 0x49505100; // "IPQ\0"
  public static final int VERSION = 1;
  public static final int HEADER_SIZE = 32; // 8 int fields * 4 bytes

  public static final int DIST_COSINE = 0;
  public static final int DIST_EUCLIDEAN = 1;
  public static final int DIST_INNER_PRODUCT = 2;

  private IvfPqIndexFormat() {
  }
}
