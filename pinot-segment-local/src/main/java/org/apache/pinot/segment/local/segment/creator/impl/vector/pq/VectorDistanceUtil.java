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
 * Distance functions for vector similarity search.
 * All functions return a "distance" where lower = more similar,
 * so they can be uniformly used in min-heap ranking.
 */
public final class VectorDistanceUtil {

  private VectorDistanceUtil() {
  }

  /**
   * Compute distance between two vectors using the specified distance function code.
   *
   * @param a first vector
   * @param b second vector
   * @param dim dimensionality
   * @param distanceFunctionCode one of {@link IvfPqIndexFormat#DIST_COSINE},
   *     {@link IvfPqIndexFormat#DIST_EUCLIDEAN}, {@link IvfPqIndexFormat#DIST_INNER_PRODUCT}
   * @return distance (lower = more similar)
   */
  public static float computeDistance(float[] a, float[] b, int dim, int distanceFunctionCode) {
    switch (distanceFunctionCode) {
      case IvfPqIndexFormat.DIST_EUCLIDEAN:
        return l2DistanceSquared(a, b, dim);
      case IvfPqIndexFormat.DIST_COSINE:
        return cosineDistance(a, b, dim);
      case IvfPqIndexFormat.DIST_INNER_PRODUCT:
        return negativeInnerProduct(a, b, dim);
      default:
        return l2DistanceSquared(a, b, dim);
    }
  }

  /**
   * Squared L2 (Euclidean) distance.
   */
  public static float l2DistanceSquared(float[] a, float[] b, int dim) {
    float sum = 0;
    for (int i = 0; i < dim; i++) {
      float diff = a[i] - b[i];
      sum += diff * diff;
    }
    return sum;
  }

  /**
   * Cosine distance = 1 - cosine_similarity.
   * Returns a value in [0, 2] where 0 = identical direction.
   */
  public static float cosineDistance(float[] a, float[] b, int dim) {
    float dotProduct = 0;
    float normA = 0;
    float normB = 0;
    for (int i = 0; i < dim; i++) {
      dotProduct += a[i] * b[i];
      normA += a[i] * a[i];
      normB += b[i] * b[i];
    }
    float denom = (float) (Math.sqrt(normA) * Math.sqrt(normB));
    if (denom == 0) {
      return 1.0f;
    }
    return 1.0f - dotProduct / denom;
  }

  /**
   * Returns a unit-normalized copy of the vector.
   */
  public static float[] normalize(float[] vector) {
    float norm = 0;
    for (float v : vector) {
      norm += v * v;
    }
    norm = (float) Math.sqrt(norm);
    float[] result = new float[vector.length];
    if (norm > 0) {
      for (int i = 0; i < vector.length; i++) {
        result[i] = vector[i] / norm;
      }
    }
    return result;
  }

  /**
   * Negative inner product as distance (lower = higher similarity).
   */
  public static float negativeInnerProduct(float[] a, float[] b, int dim) {
    float dotProduct = 0;
    for (int i = 0; i < dim; i++) {
      dotProduct += a[i] * b[i];
    }
    return -dotProduct;
  }
}
