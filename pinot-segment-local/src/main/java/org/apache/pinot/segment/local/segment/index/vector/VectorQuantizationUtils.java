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
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.function.scalar.VectorFunctions;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;
import org.apache.pinot.segment.spi.index.creator.VectorQuantizerType;
import org.apache.pinot.segment.spi.index.reader.VectorQuantizer;


/**
 * Utility methods for offline vector quantization.
 *
 * <p>Shared by the IVF_PQ creator, reader, and benchmark harness.</p>
 */
public final class VectorQuantizationUtils {
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
    return KMeansTrainer.train(samples, numCentroids, seed, distanceFunction);
  }

  public static float[][][] trainProductQuantizer(float[][] residuals, int dimension, int pqM, int pqNbits,
      long seed) {
    return ProductQuantizer.train(residuals, dimension, pqM, pqNbits, seed);
  }

  public static byte[] encodeResidual(float[] residual, float[][][] codebooks, int[] lengths) {
    return ProductQuantizer.encode(residual, codebooks, lengths);
  }

  public static float[] decodeResidual(byte[] codes, float[][][] codebooks, int[] lengths) {
    return ProductQuantizer.decode(codes, codebooks, lengths);
  }

  public static float[][] buildL2DistanceTables(float[] queryResidual, float[][][] codebooks, int[] lengths) {
    return ProductQuantizer.buildL2DistanceTables(queryResidual, codebooks, lengths);
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
    float[] residual = ProductQuantizer.decode(codes, codebooks, lengths);
    return addVectors(centroid, residual);
  }

  public static float[][] clone2d(float[][] values) {
    float[][] copy = new float[values.length][];
    for (int i = 0; i < values.length; i++) {
      copy[i] = values[i].clone();
    }
    return copy;
  }

  /**
   * Resolves the configured quantizer type from index properties.
   * Defaults to {@link VectorQuantizerType#FLAT} for backward compatibility.
   */
  public static VectorQuantizerType resolveQuantizerType(@Nullable Map<String, String> properties) {
    if (properties == null) {
      return VectorQuantizerType.FLAT;
    }
    String value = properties.get("quantizer");
    if (value == null || value.isEmpty()) {
      return VectorQuantizerType.FLAT;
    }
    return VectorQuantizerType.fromString(value);
  }

  /**
   * Creates a trained quantizer for write-time usage.
   *
   * <p>For scalar quantizers, if training vectors are empty this method creates a valid
   * deterministic quantizer with zero min/max bounds so empty indexes can still be serialized.</p>
   */
  public static VectorQuantizer createWriteQuantizer(VectorQuantizerType quantizerType, int dimension,
      @Nullable float[][] trainingVectors) {
    switch (quantizerType) {
      case FLAT:
        return new FlatQuantizer(dimension);
      case SQ8:
        return createScalarWriteQuantizer(dimension, ScalarQuantizer.BitWidth.SQ8, trainingVectors);
      case SQ4:
        return createScalarWriteQuantizer(dimension, ScalarQuantizer.BitWidth.SQ4, trainingVectors);
      default:
        throw new IllegalArgumentException("Unsupported quantizer for IVF_FLAT family: " + quantizerType);
    }
  }

  /**
   * Creates a read-time quantizer instance from serialized parameters.
   */
  public static VectorQuantizer createReadQuantizer(VectorQuantizerType quantizerType, int dimension,
      @Nullable byte[] serializedParams) {
    switch (quantizerType) {
      case FLAT:
        return new FlatQuantizer(dimension);
      case SQ8:
      case SQ4:
        Preconditions.checkArgument(serializedParams != null && serializedParams.length > 0,
            "Serialized scalar quantizer params must be present for quantizer=%s", quantizerType);
        return ScalarQuantizer.deserialize(serializedParams);
      default:
        throw new IllegalArgumentException("Unsupported quantizer for IVF_FLAT family: " + quantizerType);
    }
  }

  /**
   * Serializes quantizer parameters for persistence in the index header.
   */
  public static byte[] serializeQuantizerParams(VectorQuantizer quantizer) {
    if (quantizer instanceof ScalarQuantizer) {
      return ((ScalarQuantizer) quantizer).serialize();
    }
    return new byte[0];
  }

  private static VectorQuantizer createScalarWriteQuantizer(int dimension, ScalarQuantizer.BitWidth bitWidth,
      @Nullable float[][] trainingVectors) {
    if (trainingVectors == null || trainingVectors.length == 0) {
      float[] minValues = new float[dimension];
      float[] maxValues = new float[dimension];
      for (int d = 0; d < dimension; d++) {
        maxValues[d] = 1e-7f;
      }
      return new ScalarQuantizer(dimension, bitWidth, minValues, maxValues);
    }
    return ScalarQuantizer.train(trainingVectors, dimension, bitWidth);
  }
}
