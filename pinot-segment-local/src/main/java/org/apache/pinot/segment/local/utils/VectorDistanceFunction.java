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
package org.apache.pinot.segment.local.utils;

import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;


/**
 * Pure-Java implementations of vector distance functions used by the IVF_FLAT index.
 *
 * <p>All distance functions return a non-negative value where lower means more similar.
 * This convention allows a single min-heap to be used for top-K selection regardless
 * of the distance metric.</p>
 *
 * <p>Conventions per metric:</p>
 * <ul>
 *   <li><b>L2 / EUCLIDEAN</b> -- sum of squared differences (no sqrt, sufficient for ranking)</li>
 *   <li><b>COSINE</b> -- {@code 1 - cosine_similarity(a, b)}. Callers that normalize vectors
 *       at index time can use {@link #dotProductDistance} directly on the normalized vectors,
 *       since {@code cosine_distance(a,b) = 1 - dot(norm(a), norm(b))}.</li>
 *   <li><b>INNER_PRODUCT / DOT_PRODUCT</b> -- {@code -dot(a, b)} so that <em>higher</em>
 *       similarity (larger dot product) maps to <em>lower</em> distance.</li>
 * </ul>
 *
 * <p>This class is stateless and thread-safe.</p>
 */
public final class VectorDistanceFunction {

  private VectorDistanceFunction() {
  }

  // -----------------------------------------------------------------------
  // L2 (Euclidean) squared distance
  // -----------------------------------------------------------------------

  /**
   * Computes the squared Euclidean (L2) distance between two vectors.
   *
   * @param a first vector
   * @param b second vector (must have the same length as {@code a})
   * @return the sum of squared element-wise differences
   * @throws IllegalArgumentException if the vectors have different lengths
   */
  public static float l2SquaredDistance(float[] a, float[] b) {
    checkDimensions(a, b);
    float sum = 0.0f;
    for (int i = 0; i < a.length; i++) {
      float diff = a[i] - b[i];
      sum += diff * diff;
    }
    return sum;
  }

  // -----------------------------------------------------------------------
  // Inner product / dot-product distance
  // -----------------------------------------------------------------------

  /**
   * Computes the negative dot product between two vectors so that higher
   * similarity (larger positive dot product) becomes lower distance.
   *
   * @param a first vector
   * @param b second vector (must have the same length as {@code a})
   * @return {@code -dot(a, b)}
   * @throws IllegalArgumentException if the vectors have different lengths
   */
  public static float dotProductDistance(float[] a, float[] b) {
    checkDimensions(a, b);
    float dot = 0.0f;
    for (int i = 0; i < a.length; i++) {
      dot += a[i] * b[i];
    }
    return -dot;
  }

  // -----------------------------------------------------------------------
  // Cosine distance
  // -----------------------------------------------------------------------

  /**
   * Computes the cosine distance between two vectors: {@code 1 - cosine_similarity(a, b)}.
   *
   * <p>If either vector has zero magnitude, the distance is defined as {@code 1.0f}
   * (maximum distance).</p>
   *
   * @param a first vector
   * @param b second vector (must have the same length as {@code a})
   * @return cosine distance in [0, 2]
   * @throws IllegalArgumentException if the vectors have different lengths
   */
  public static float cosineDistance(float[] a, float[] b) {
    checkDimensions(a, b);
    float dot = 0.0f;
    float normA = 0.0f;
    float normB = 0.0f;
    for (int i = 0; i < a.length; i++) {
      dot += a[i] * b[i];
      normA += a[i] * a[i];
      normB += b[i] * b[i];
    }
    float denominator = (float) Math.sqrt(normA * normB);
    if (denominator == 0.0f) {
      return 1.0f;
    }
    return 1.0f - dot / denominator;
  }

  /**
   * Computes the cosine distance between a <em>pre-normalized</em> vector {@code a}
   * and an <em>un-normalized</em> vector {@code b}. This avoids recomputing the norm
   * of the stored (normalized) vectors during search.
   *
   * @param normalizedA a unit-length vector
   * @param b           an arbitrary vector
   * @return cosine distance in [0, 2]
   */
  public static float cosineDistanceWithNormalizedFirst(float[] normalizedA, float[] b) {
    checkDimensions(normalizedA, b);
    float dot = 0.0f;
    float normB = 0.0f;
    for (int i = 0; i < normalizedA.length; i++) {
      dot += normalizedA[i] * b[i];
      normB += b[i] * b[i];
    }
    float denominator = (float) Math.sqrt(normB);
    if (denominator == 0.0f) {
      return 1.0f;
    }
    return 1.0f - dot / denominator;
  }

  // -----------------------------------------------------------------------
  // Normalization
  // -----------------------------------------------------------------------

  /**
   * Returns a new unit-length copy of the given vector. If the vector has zero
   * magnitude, a zero vector of the same length is returned.
   *
   * @param vector the input vector
   * @return a new array containing the normalized vector
   */
  public static float[] normalize(float[] vector) {
    float norm = 0.0f;
    for (float v : vector) {
      norm += v * v;
    }
    norm = (float) Math.sqrt(norm);
    float[] result = new float[vector.length];
    if (norm > 0.0f) {
      for (int i = 0; i < vector.length; i++) {
        result[i] = vector[i] / norm;
      }
    }
    return result;
  }

  // -----------------------------------------------------------------------
  // Dispatch helper
  // -----------------------------------------------------------------------

  /**
   * Computes the distance between two vectors using the specified distance function.
   *
   * @param a                first vector
   * @param b                second vector
   * @param distanceFunction the distance function to use
   * @return the computed distance (lower means more similar)
   */
  public static float computeDistance(float[] a, float[] b,
      VectorIndexConfig.VectorDistanceFunction distanceFunction) {
    switch (distanceFunction) {
      case EUCLIDEAN:
      case L2:
        return l2SquaredDistance(a, b);
      case COSINE:
        return cosineDistance(a, b);
      case INNER_PRODUCT:
      case DOT_PRODUCT:
        return dotProductDistance(a, b);
      default:
        throw new IllegalArgumentException("Unsupported distance function: " + distanceFunction);
    }
  }

  // -----------------------------------------------------------------------
  // Validation
  // -----------------------------------------------------------------------

  private static void checkDimensions(float[] a, float[] b) {
    if (a.length != b.length) {
      throw new IllegalArgumentException(
          "Vector dimension mismatch: " + a.length + " vs " + b.length);
    }
  }
}
