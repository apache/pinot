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
package org.apache.pinot.segment.local.segment.index.vector;

import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.Random;
import org.apache.pinot.common.function.scalar.VectorFunctions;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;


/**
 * Utility for training k-means centroids used by IVF_PQ.
 *
 * <p>This class is stateless and thread-safe.</p>
 */
public final class KMeansTrainer {
  private static final int MAX_KMEANS_ITERATIONS = 50;
  private static final float CONVERGENCE_THRESHOLD = 1e-5f;

  private KMeansTrainer() {
  }

  public static float[][] train(float[][] samples, int numCentroids, long seed,
      VectorIndexConfig.VectorDistanceFunction distanceFunction) {
    Preconditions.checkArgument(numCentroids > 0, "numCentroids must be positive");
    Preconditions.checkArgument(samples != null, "samples must not be null");
    Preconditions.checkArgument(samples.length > 0, "samples must not be empty");

    int dimension = samples[0].length;
    float[][] centroids = initializeCentroids(samples, numCentroids, seed, distanceFunction);
    float[][] newCentroids = new float[numCentroids][dimension];
    int[] counts = new int[numCentroids];

    for (int iteration = 0; iteration < MAX_KMEANS_ITERATIONS; iteration++) {
      Arrays.fill(counts, 0);
      for (float[] row : newCentroids) {
        Arrays.fill(row, 0.0f);
      }

      for (float[] sample : samples) {
        int centroid = VectorQuantizationUtils.findNearestCentroid(sample, centroids, distanceFunction);
        counts[centroid]++;
        for (int d = 0; d < dimension; d++) {
          newCentroids[centroid][d] += sample[d];
        }
      }

      float maxMovement = 0.0f;
      for (int c = 0; c < numCentroids; c++) {
        if (counts[c] == 0) {
          newCentroids[c] = centroids[c].clone();
          continue;
        }
        for (int d = 0; d < dimension; d++) {
          newCentroids[c][d] /= counts[c];
        }
        maxMovement = Math.max(maxMovement, (float) VectorFunctions.euclideanDistance(centroids[c], newCentroids[c]));
      }

      centroids = VectorQuantizationUtils.clone2d(newCentroids);
      if (maxMovement < CONVERGENCE_THRESHOLD) {
        break;
      }
    }
    return centroids;
  }

  private static float[][] initializeCentroids(float[][] samples, int numCentroids, long seed,
      VectorIndexConfig.VectorDistanceFunction distanceFunction) {
    int sampleCount = samples.length;
    int dimension = samples[0].length;
    float[][] centroids = new float[numCentroids][dimension];
    Random random = new Random(seed);

    if (sampleCount >= numCentroids) {
      centroids[0] = samples[random.nextInt(sampleCount)].clone();
      float[] minDistances = new float[sampleCount];
      Arrays.fill(minDistances, Float.MAX_VALUE);

      for (int centroid = 1; centroid < numCentroids; centroid++) {
        float totalWeight = 0.0f;
        for (int i = 0; i < sampleCount; i++) {
          float distance =
              VectorQuantizationUtils.computeTrainingDistance(samples[i], centroids[centroid - 1], distanceFunction);
          if (distance < minDistances[i]) {
            minDistances[i] = distance;
          }
          totalWeight += minDistances[i];
        }

        if (totalWeight <= 0.0f) {
          centroids[centroid] = samples[random.nextInt(sampleCount)].clone();
          continue;
        }

        float target = random.nextFloat() * totalWeight;
        float cumulative = 0.0f;
        int selected = sampleCount - 1;
        for (int i = 0; i < sampleCount; i++) {
          cumulative += minDistances[i];
          if (cumulative >= target) {
            selected = i;
            break;
          }
        }
        centroids[centroid] = samples[selected].clone();
      }
      return centroids;
    }

    for (int centroid = 0; centroid < numCentroids; centroid++) {
      centroids[centroid] = samples[random.nextInt(sampleCount)].clone();
    }
    return centroids;
  }
}
