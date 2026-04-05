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
 * Utility methods for offline vector quantization.
 *
 * <p>Shared by the IVF_PQ creator, reader, and benchmark harness.</p>
 */
public final class VectorQuantizationUtils {
  private static final int MAX_KMEANS_ITERATIONS = 50;
  private static final float CONVERGENCE_THRESHOLD = 1e-5f;

  private VectorQuantizationUtils() {
  }

  public static int[] computeSubvectorLengths(int dimension, int pqM) {
    Preconditions.checkArgument(dimension > 0, "dimension must be positive");
    Preconditions.checkArgument(pqM > 0, "pqM must be positive");
    Preconditions.checkArgument(pqM <= dimension, "pqM must be <= dimension");

    int[] lengths = new int[pqM];
    int base = dimension / pqM;
    int remainder = dimension % pqM;
    for (int i = 0; i < pqM; i++) {
      lengths[i] = base + (i < remainder ? 1 : 0);
    }
    return lengths;
  }

  public static int[] computeSubvectorOffsets(int[] lengths) {
    int[] offsets = new int[lengths.length];
    int offset = 0;
    for (int i = 0; i < lengths.length; i++) {
      offsets[i] = offset;
      offset += lengths[i];
    }
    return offsets;
  }

  public static float[] normalizeCopy(float[] vector) {
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

  public static float[] transformForDistance(float[] vector,
      VectorIndexConfig.VectorDistanceFunction distanceFunction) {
    if (distanceFunction == VectorIndexConfig.VectorDistanceFunction.COSINE) {
      return normalizeCopy(vector);
    }
    return vector.clone();
  }

  public static float[][] transformAll(float[][] vectors, VectorIndexConfig.VectorDistanceFunction distanceFunction) {
    float[][] transformed = new float[vectors.length][];
    for (int i = 0; i < vectors.length; i++) {
      transformed[i] = transformForDistance(vectors[i], distanceFunction);
    }
    return transformed;
  }

  public static float computeDistance(float[] a, float[] b,
      VectorIndexConfig.VectorDistanceFunction distanceFunction) {
    switch (distanceFunction) {
      case EUCLIDEAN:
      case L2:
        return (float) VectorFunctions.euclideanDistance(a, b);
      case COSINE:
        return (float) VectorFunctions.cosineDistance(a, b, 1.0d);
      case INNER_PRODUCT:
      case DOT_PRODUCT:
        return (float) -VectorFunctions.dotProduct(a, b);
      default:
        throw new IllegalArgumentException("Unsupported distance function: " + distanceFunction);
    }
  }

  /**
   * Computes the non-negative distance used for centroid initialization and k-means training.
   *
   * <p>K-means minimizes Euclidean distance by construction. For cosine distance we normalize first so
   * Euclidean distance reflects angular separation. For inner/dot-product search we still use Euclidean
   * training distance because the raw similarity score is not suitable as a k-means++ sampling weight.</p>
   */
  public static float computeTrainingDistance(float[] a, float[] b,
      VectorIndexConfig.VectorDistanceFunction distanceFunction) {
    if (distanceFunction == VectorIndexConfig.VectorDistanceFunction.COSINE) {
      return (float) VectorFunctions.euclideanDistance(normalizeCopy(a), normalizeCopy(b));
    }
    return (float) VectorFunctions.euclideanDistance(a, b);
  }

  public static int findNearestCentroid(float[] vector, float[][] centroids,
      VectorIndexConfig.VectorDistanceFunction distanceFunction) {
    Preconditions.checkArgument(centroids.length > 0, "centroids must not be empty");
    int nearest = 0;
    float nearestDistance = Float.MAX_VALUE;
    for (int i = 0; i < centroids.length; i++) {
      float distance = computeDistance(vector, centroids[i], distanceFunction);
      if (distance < nearestDistance) {
        nearestDistance = distance;
        nearest = i;
      }
    }
    return nearest;
  }

  public static int[] assignVectors(float[][] vectors, float[][] centroids,
      VectorIndexConfig.VectorDistanceFunction distanceFunction) {
    int[] assignments = new int[vectors.length];
    for (int i = 0; i < vectors.length; i++) {
      assignments[i] = findNearestCentroid(vectors[i], centroids, distanceFunction);
    }
    return assignments;
  }

  public static float[][] trainKMeans(float[][] samples, int numCentroids, long seed,
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

      for (int i = 0; i < samples.length; i++) {
        int centroid = findNearestCentroid(samples[i], centroids, distanceFunction);
        counts[centroid]++;
        for (int d = 0; d < dimension; d++) {
          newCentroids[centroid][d] += samples[i][d];
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

      centroids = clone2d(newCentroids);
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
          float distance = computeTrainingDistance(samples[i], centroids[centroid - 1], distanceFunction);
          if (distance < minDistances[i]) {
            minDistances[i] = distance;
          }
          totalWeight += minDistances[i];
        }

        if (totalWeight <= 0.0f) {
          int fallback = random.nextInt(sampleCount);
          centroids[centroid] = samples[fallback].clone();
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

  public static float[][][] trainProductQuantizer(float[][] residuals, int dimension, int pqM, int pqNbits,
      long seed) {
    Preconditions.checkArgument(pqM > 0, "pqM must be positive");
    Preconditions.checkArgument(pqNbits > 0, "pqNbits must be positive");
    Preconditions.checkArgument(pqNbits <= 8, "pqNbits must be <= 8 for the byte-coded on-disk format");
    Preconditions.checkArgument(dimension > 0, "dimension must be positive");
    Preconditions.checkArgument(pqM <= dimension, "pqM must be <= dimension");
    Preconditions.checkArgument(residuals.length > 0, "residuals must not be empty");

    int[] lengths = computeSubvectorLengths(dimension, pqM);
    int codebookSize = 1 << pqNbits;
    float[][][] codebooks = new float[pqM][][];
    for (int m = 0; m < pqM; m++) {
      int subDim = lengths[m];
      float[][] subSamples = new float[residuals.length][subDim];
      int offset = 0;
      for (int i = 0; i < m; i++) {
        offset += lengths[i];
      }
      for (int i = 0; i < residuals.length; i++) {
        for (int d = 0; d < subDim; d++) {
          subSamples[i][d] = residuals[i][offset + d];
        }
      }
      codebooks[m] = trainKMeans(subSamples, codebookSize, seed + (m * 31L) + 17L,
          VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN);
    }
    return codebooks;
  }

  public static byte[] encodeResidual(float[] residual, float[][][] codebooks, int[] lengths) {
    byte[] codes = new byte[codebooks.length];
    int offset = 0;
    for (int m = 0; m < codebooks.length; m++) {
      int subDim = lengths[m];
      int best = 0;
      float bestDistance = Float.MAX_VALUE;
      for (int code = 0; code < codebooks[m].length; code++) {
        float distance = 0.0f;
        for (int d = 0; d < subDim; d++) {
          float diff = residual[offset + d] - codebooks[m][code][d];
          distance += diff * diff;
        }
        if (distance < bestDistance) {
          bestDistance = distance;
          best = code;
        }
      }
      codes[m] = (byte) best;
      offset += subDim;
    }
    return codes;
  }

  public static float[] decodeResidual(byte[] codes, float[][][] codebooks, int[] lengths) {
    int dimension = 0;
    for (int length : lengths) {
      dimension += length;
    }
    float[] residual = new float[dimension];
    int offset = 0;
    for (int m = 0; m < codebooks.length; m++) {
      float[] centroid = codebooks[m][codes[m] & 0xFF];
      int subDim = lengths[m];
      for (int d = 0; d < subDim; d++) {
        residual[offset + d] = centroid[d];
      }
      offset += subDim;
    }
    return residual;
  }

  public static float[][] buildL2DistanceTables(float[] queryResidual, float[][][] codebooks, int[] lengths) {
    float[][] tables = new float[codebooks.length][];
    int offset = 0;
    for (int m = 0; m < codebooks.length; m++) {
      int subDim = lengths[m];
      float[] table = new float[codebooks[m].length];
      for (int code = 0; code < codebooks[m].length; code++) {
        float distance = 0.0f;
        for (int d = 0; d < subDim; d++) {
          float diff = queryResidual[offset + d] - codebooks[m][code][d];
          distance += diff * diff;
        }
        table[code] = distance;
      }
      tables[m] = table;
      offset += subDim;
    }
    return tables;
  }

  public static float[] addVectors(float[] left, float[] right) {
    Preconditions.checkArgument(left.length == right.length, "Vector lengths do not match");
    float[] result = new float[left.length];
    for (int i = 0; i < left.length; i++) {
      result[i] = left[i] + right[i];
    }
    return result;
  }

  public static float[] subtractVectors(float[] left, float[] right) {
    Preconditions.checkArgument(left.length == right.length, "Vector lengths do not match");
    float[] result = new float[left.length];
    for (int i = 0; i < left.length; i++) {
      result[i] = left[i] - right[i];
    }
    return result;
  }

  public static float[] reconstructVector(float[] centroid, byte[] codes, float[][][] codebooks, int[] lengths) {
    float[] residual = decodeResidual(codes, codebooks, lengths);
    return addVectors(centroid, residual);
  }

  public static float[][] clone2d(float[][] values) {
    float[][] copy = new float[values.length][];
    for (int i = 0; i < values.length; i++) {
      copy[i] = values[i].clone();
    }
    return copy;
  }
}
