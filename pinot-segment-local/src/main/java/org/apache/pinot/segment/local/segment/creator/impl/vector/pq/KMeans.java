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

import java.util.Arrays;
import java.util.Random;


/**
 * Simple KMeans clustering for IVF coarse quantization and PQ sub-quantizer training.
 * Pure Java implementation optimized for correctness and readability.
 */
public class KMeans {
  private static final int DEFAULT_MAX_ITERATIONS = 25;
  private static final double CONVERGENCE_THRESHOLD = 1e-6;

  private KMeans() {
  }

  /**
   * Runs KMeans clustering on the given vectors.
   *
   * @param vectors training vectors, each of length {@code dim}
   * @param dim dimensionality of each vector
   * @param k number of clusters
   * @param random random source for centroid initialization
   * @return cluster centroids, shape [k][dim]
   */
  public static float[][] train(float[][] vectors, int dim, int k, Random random) {
    return train(vectors, dim, k, random, DEFAULT_MAX_ITERATIONS);
  }

  /**
   * Runs KMeans clustering with a configurable iteration limit.
   */
  public static float[][] train(float[][] vectors, int dim, int k, Random random, int maxIterations) {
    int n = vectors.length;
    if (n == 0 || k <= 0) {
      throw new IllegalArgumentException("Need at least 1 vector and k > 0");
    }
    if (k > n) {
      k = n;
    }

    // Initialize centroids using KMeans++ initialization
    float[][] centroids = initializeCentroidsPlusPlus(vectors, dim, k, random);
    int[] assignments = new int[n];

    for (int iter = 0; iter < maxIterations; iter++) {
      // Assign each vector to the nearest centroid
      boolean changed = false;
      for (int i = 0; i < n; i++) {
        int nearest = findNearest(vectors[i], centroids, dim);
        if (nearest != assignments[i]) {
          assignments[i] = nearest;
          changed = true;
        }
      }

      if (!changed) {
        break;
      }

      // Update centroids
      float[][] newCentroids = new float[k][dim];
      int[] counts = new int[k];
      for (int i = 0; i < n; i++) {
        int cluster = assignments[i];
        counts[cluster]++;
        for (int d = 0; d < dim; d++) {
          newCentroids[cluster][d] += vectors[i][d];
        }
      }

      double maxShift = 0;
      for (int c = 0; c < k; c++) {
        if (counts[c] > 0) {
          for (int d = 0; d < dim; d++) {
            newCentroids[c][d] /= counts[c];
          }
          double shift = l2DistanceSquared(centroids[c], newCentroids[c], dim);
          maxShift = Math.max(maxShift, shift);
        } else {
          // Empty cluster: reinitialize to a random vector
          newCentroids[c] = Arrays.copyOf(vectors[random.nextInt(n)], dim);
        }
      }

      centroids = newCentroids;
      if (maxShift < CONVERGENCE_THRESHOLD) {
        break;
      }
    }

    return centroids;
  }

  /**
   * KMeans++ initialization: pick first centroid randomly, subsequent centroids
   * with probability proportional to squared distance from nearest existing centroid.
   */
  private static float[][] initializeCentroidsPlusPlus(float[][] vectors, int dim, int k, Random random) {
    int n = vectors.length;
    float[][] centroids = new float[k][dim];

    // Pick first centroid uniformly at random
    centroids[0] = Arrays.copyOf(vectors[random.nextInt(n)], dim);

    float[] minDist = new float[n];
    Arrays.fill(minDist, Float.MAX_VALUE);

    for (int c = 1; c < k; c++) {
      // Update min distances
      double totalDist = 0;
      for (int i = 0; i < n; i++) {
        float dist = l2DistanceSquared(vectors[i], centroids[c - 1], dim);
        if (dist < minDist[i]) {
          minDist[i] = dist;
        }
        totalDist += minDist[i];
      }

      // Pick next centroid with probability proportional to distance
      double threshold = random.nextDouble() * totalDist;
      double cumulative = 0;
      int selected = n - 1;
      for (int i = 0; i < n; i++) {
        cumulative += minDist[i];
        if (cumulative >= threshold) {
          selected = i;
          break;
        }
      }
      centroids[c] = Arrays.copyOf(vectors[selected], dim);
    }

    return centroids;
  }

  /**
   * Find the index of the nearest centroid to the given vector using L2 distance.
   */
  public static int findNearest(float[] vector, float[][] centroids, int dim) {
    int nearest = 0;
    float minDist = Float.MAX_VALUE;
    for (int c = 0; c < centroids.length; c++) {
      float dist = l2DistanceSquared(vector, centroids[c], dim);
      if (dist < minDist) {
        minDist = dist;
        nearest = c;
      }
    }
    return nearest;
  }

  /**
   * Find the indices of the k nearest centroids.
   */
  public static int[] findKNearest(float[] vector, float[][] centroids, int dim, int k) {
    int numCentroids = centroids.length;
    k = Math.min(k, numCentroids);
    float[] distances = new float[numCentroids];
    int[] indices = new int[numCentroids];

    for (int c = 0; c < numCentroids; c++) {
      distances[c] = l2DistanceSquared(vector, centroids[c], dim);
      indices[c] = c;
    }

    // Partial sort to find k nearest
    for (int i = 0; i < k; i++) {
      int minIdx = i;
      for (int j = i + 1; j < numCentroids; j++) {
        if (distances[j] < distances[minIdx]) {
          minIdx = j;
        }
      }
      // swap
      float tmpDist = distances[i];
      distances[i] = distances[minIdx];
      distances[minIdx] = tmpDist;
      int tmpIdx = indices[i];
      indices[i] = indices[minIdx];
      indices[minIdx] = tmpIdx;
    }

    return Arrays.copyOf(indices, k);
  }

  /**
   * Computes squared L2 distance between two vectors.
   */
  public static float l2DistanceSquared(float[] a, float[] b, int dim) {
    float sum = 0;
    for (int i = 0; i < dim; i++) {
      float diff = a[i] - b[i];
      sum += diff * diff;
    }
    return sum;
  }
}
