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

import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;
import org.apache.pinot.segment.spi.index.creator.VectorQuantizerType;


/**
 * Interface for vector quantizers that encode, decode, and compute distances on vectors.
 *
 * <p>A quantizer transforms raw float32 vectors into a compact encoded representation (byte array)
 * and provides methods to decode back to float32 and to compute approximate distances directly
 * on the encoded form.</p>
 *
 * <p>Implementations must be thread-safe for concurrent {@link #encode}, {@link #decode}, and
 * {@link #computeDistance} calls. Training state (if any) should be immutable after construction.</p>
 */
public interface VectorQuantizer {

  /**
   * Returns the quantizer type.
   */
  VectorQuantizerType getType();

  /**
   * Returns the expected dimension of input vectors.
   */
  int getDimension();

  /**
   * Returns the number of bytes in the encoded representation of a single vector.
   */
  int getEncodedBytesPerVector();

  /**
   * Encodes a raw float32 vector into its quantized byte representation.
   *
   * @param vector the raw float32 vector (length must equal {@link #getDimension()})
   * @return the encoded byte array (length equals {@link #getEncodedBytesPerVector()})
   * @throws IllegalArgumentException if the vector length does not match the expected dimension
   */
  byte[] encode(float[] vector);

  /**
   * Decodes a quantized byte representation back to a float32 vector.
   *
   * <p>For lossy quantizers (SQ8, SQ4, PQ), the decoded vector is an approximation of the original.</p>
   *
   * @param encoded the encoded byte array (length must equal {@link #getEncodedBytesPerVector()})
   * @return the decoded float32 vector (length equals {@link #getDimension()})
   * @throws IllegalArgumentException if the encoded array length does not match expectations
   */
  float[] decode(byte[] encoded);

  /**
   * Computes the distance between a raw float32 query vector and an encoded document vector.
   *
   * <p>Implementations may compute the distance in the compressed domain for efficiency or
   * decode first and compute exactly, depending on the quantizer type.</p>
   *
   * @param query the raw float32 query vector
   * @param encodedDoc the encoded document vector
   * @param distanceFunction the distance function to use
   * @return the computed distance
   */
  float computeDistance(float[] query, byte[] encodedDoc,
      VectorIndexConfig.VectorDistanceFunction distanceFunction);
}
