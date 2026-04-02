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

import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Unit tests for {@link VectorQuantizationUtils}.
 */
public class VectorQuantizationUtilsTest {

  @Test
  public void testComputeSubvectorLengthsEvenDivision() {
    int[] lengths = VectorQuantizationUtils.computeSubvectorLengths(128, 8);
    Assert.assertEquals(lengths.length, 8);
    int sum = 0;
    for (int length : lengths) {
      Assert.assertEquals(length, 16);
      sum += length;
    }
    Assert.assertEquals(sum, 128);
  }

  @Test
  public void testComputeSubvectorLengthsUnevenDivision() {
    // 10 / 3 = 3 remainder 1: first subvector gets 4, rest get 3
    int[] lengths = VectorQuantizationUtils.computeSubvectorLengths(10, 3);
    Assert.assertEquals(lengths.length, 3);
    Assert.assertEquals(lengths[0], 4);
    Assert.assertEquals(lengths[1], 3);
    Assert.assertEquals(lengths[2], 3);
    Assert.assertEquals(lengths[0] + lengths[1] + lengths[2], 10);
  }

  @Test
  public void testComputeSubvectorLengthsSingleSubvector() {
    int[] lengths = VectorQuantizationUtils.computeSubvectorLengths(7, 1);
    Assert.assertEquals(lengths.length, 1);
    Assert.assertEquals(lengths[0], 7);
  }

  @Test
  public void testComputeSubvectorLengthsEqualToDimension() {
    int[] lengths = VectorQuantizationUtils.computeSubvectorLengths(4, 4);
    Assert.assertEquals(lengths.length, 4);
    for (int length : lengths) {
      Assert.assertEquals(length, 1);
    }
  }

  @Test
  public void testComputeSubvectorOffsets() {
    int[] lengths = {4, 3, 3};
    int[] offsets = VectorQuantizationUtils.computeSubvectorOffsets(lengths);
    Assert.assertEquals(offsets.length, 3);
    Assert.assertEquals(offsets[0], 0);
    Assert.assertEquals(offsets[1], 4);
    Assert.assertEquals(offsets[2], 7);
  }

  @Test
  public void testNormalizeCopy() {
    float[] vector = {3.0f, 4.0f};
    float[] normalized = VectorQuantizationUtils.normalizeCopy(vector);
    Assert.assertEquals(normalized[0], 0.6f, 1e-6f);
    Assert.assertEquals(normalized[1], 0.8f, 1e-6f);
    // Original should be unchanged
    Assert.assertEquals(vector[0], 3.0f, 1e-6f);
  }

  @Test
  public void testNormalizeCopyZeroVector() {
    float[] zero = {0.0f, 0.0f, 0.0f};
    float[] normalized = VectorQuantizationUtils.normalizeCopy(zero);
    for (float v : normalized) {
      Assert.assertEquals(v, 0.0f, 1e-6f);
    }
  }

  @Test
  public void testTransformForDistanceCosineNormalizes() {
    float[] vector = {3.0f, 4.0f};
    float[] transformed = VectorQuantizationUtils.transformForDistance(vector,
        VectorIndexConfig.VectorDistanceFunction.COSINE);
    float norm = 0.0f;
    for (float v : transformed) {
      norm += v * v;
    }
    Assert.assertEquals(norm, 1.0f, 1e-5f);
  }

  @Test
  public void testTransformForDistanceEuclideanClones() {
    float[] vector = {1.0f, 2.0f};
    float[] transformed = VectorQuantizationUtils.transformForDistance(vector,
        VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN);
    Assert.assertEquals(transformed[0], 1.0f, 1e-6f);
    Assert.assertEquals(transformed[1], 2.0f, 1e-6f);
    // Should be a clone, not the same array
    Assert.assertNotSame(transformed, vector);
  }

  @Test
  public void testComputeDistanceEuclidean() {
    float[] a = {1.0f, 0.0f};
    float[] b = {0.0f, 1.0f};
    float distance = VectorQuantizationUtils.computeDistance(a, b,
        VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN);
    Assert.assertTrue(distance > 0.0f);
  }

  @Test
  public void testComputeDistanceL2() {
    float[] a = {1.0f, 0.0f};
    float[] b = {0.0f, 1.0f};
    float distEuclidean = VectorQuantizationUtils.computeDistance(a, b,
        VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN);
    float distL2 = VectorQuantizationUtils.computeDistance(a, b,
        VectorIndexConfig.VectorDistanceFunction.L2);
    Assert.assertEquals(distL2, distEuclidean, 1e-6f);
  }

  @Test
  public void testComputeDistanceCosineIdentical() {
    float[] a = {1.0f, 0.0f};
    float distance = VectorQuantizationUtils.computeDistance(a, a,
        VectorIndexConfig.VectorDistanceFunction.COSINE);
    Assert.assertEquals(distance, 0.0f, 1e-5f);
  }

  @Test
  public void testComputeDistanceCosineOrthogonal() {
    float[] a = {1.0f, 0.0f};
    float[] b = {0.0f, 1.0f};
    float distance = VectorQuantizationUtils.computeDistance(a, b,
        VectorIndexConfig.VectorDistanceFunction.COSINE);
    Assert.assertEquals(distance, 1.0f, 1e-5f);
  }

  @Test
  public void testComputeDistanceDotProduct() {
    float[] a = {2.0f, 3.0f};
    float[] b = {4.0f, 5.0f};
    // dot product = 2*4 + 3*5 = 23, returned as -23 (negated for min-distance ranking)
    float distance = VectorQuantizationUtils.computeDistance(a, b,
        VectorIndexConfig.VectorDistanceFunction.DOT_PRODUCT);
    Assert.assertEquals(distance, -23.0f, 1e-5f);
  }

  @Test
  public void testComputeDistanceInnerProduct() {
    float[] a = {2.0f, 3.0f};
    float[] b = {4.0f, 5.0f};
    float distDot = VectorQuantizationUtils.computeDistance(a, b,
        VectorIndexConfig.VectorDistanceFunction.DOT_PRODUCT);
    float distIp = VectorQuantizationUtils.computeDistance(a, b,
        VectorIndexConfig.VectorDistanceFunction.INNER_PRODUCT);
    Assert.assertEquals(distIp, distDot, 1e-6f);
  }

  @Test
  public void testComputeTrainingDistanceDotProductIsNonNegative() {
    float[] a = {2.0f, 3.0f};
    float[] b = {4.0f, 5.0f};
    float trainingDistance = VectorQuantizationUtils.computeTrainingDistance(a, b,
        VectorIndexConfig.VectorDistanceFunction.DOT_PRODUCT);
    Assert.assertTrue(trainingDistance >= 0.0f);
  }

  @Test
  public void testComputeTrainingDistanceCosineUsesNormalizedVectors() {
    float[] a = {3.0f, 4.0f};
    float[] b = {6.0f, 8.0f};
    float trainingDistance = VectorQuantizationUtils.computeTrainingDistance(a, b,
        VectorIndexConfig.VectorDistanceFunction.COSINE);
    Assert.assertEquals(trainingDistance, 0.0f, 1e-5f);
  }

  @Test
  public void testEncodeDecodeResidualRoundTrip() {
    // Create a simple codebook for 2 subquantizers, each with 4 codewords of length 2
    float[][][] codebooks = new float[2][4][2];
    codebooks[0][0] = new float[]{1.0f, 0.0f};
    codebooks[0][1] = new float[]{0.0f, 1.0f};
    codebooks[0][2] = new float[]{-1.0f, 0.0f};
    codebooks[0][3] = new float[]{0.0f, -1.0f};
    codebooks[1][0] = new float[]{2.0f, 0.0f};
    codebooks[1][1] = new float[]{0.0f, 2.0f};
    codebooks[1][2] = new float[]{-2.0f, 0.0f};
    codebooks[1][3] = new float[]{0.0f, -2.0f};
    int[] lengths = {2, 2};

    // Encode a residual that is closest to codebook[0][1] and codebook[1][0]
    float[] residual = {0.1f, 0.9f, 1.8f, 0.1f};
    byte[] codes = VectorQuantizationUtils.encodeResidual(residual, codebooks, lengths);

    Assert.assertEquals(codes[0] & 0xFF, 1); // closest to {0.0, 1.0}
    Assert.assertEquals(codes[1] & 0xFF, 0); // closest to {2.0, 0.0}

    // Decode should return the codewords
    float[] decoded = VectorQuantizationUtils.decodeResidual(codes, codebooks, lengths);
    Assert.assertEquals(decoded[0], 0.0f, 1e-6f);
    Assert.assertEquals(decoded[1], 1.0f, 1e-6f);
    Assert.assertEquals(decoded[2], 2.0f, 1e-6f);
    Assert.assertEquals(decoded[3], 0.0f, 1e-6f);
  }

  @Test
  public void testBuildL2DistanceTables() {
    float[][][] codebooks = new float[1][2][2];
    codebooks[0][0] = new float[]{1.0f, 0.0f};
    codebooks[0][1] = new float[]{0.0f, 1.0f};
    int[] lengths = {2};

    float[] queryResidual = {1.0f, 0.0f};
    float[][] tables = VectorQuantizationUtils.buildL2DistanceTables(queryResidual, codebooks, lengths);

    Assert.assertEquals(tables.length, 1);
    Assert.assertEquals(tables[0].length, 2);
    // Distance from {1,0} to {1,0} = 0
    Assert.assertEquals(tables[0][0], 0.0f, 1e-6f);
    // Distance from {1,0} to {0,1} = (1-0)^2 + (0-1)^2 = 2
    Assert.assertEquals(tables[0][1], 2.0f, 1e-6f);
  }

  @Test
  public void testSubtractVectors() {
    float[] a = {5.0f, 3.0f};
    float[] b = {2.0f, 1.0f};
    float[] result = VectorQuantizationUtils.subtractVectors(a, b);
    Assert.assertEquals(result[0], 3.0f, 1e-6f);
    Assert.assertEquals(result[1], 2.0f, 1e-6f);
  }

  @Test
  public void testAddVectors() {
    float[] a = {1.0f, 2.0f};
    float[] b = {3.0f, 4.0f};
    float[] result = VectorQuantizationUtils.addVectors(a, b);
    Assert.assertEquals(result[0], 4.0f, 1e-6f);
    Assert.assertEquals(result[1], 6.0f, 1e-6f);
  }

  @Test
  public void testReconstructVector() {
    float[] centroid = {10.0f, 20.0f, 30.0f, 40.0f};
    float[][][] codebooks = new float[2][2][2];
    codebooks[0][0] = new float[]{1.0f, 0.0f};
    codebooks[0][1] = new float[]{0.0f, 1.0f};
    codebooks[1][0] = new float[]{2.0f, 0.0f};
    codebooks[1][1] = new float[]{0.0f, 2.0f};
    int[] lengths = {2, 2};
    byte[] codes = {0, 1}; // codebook[0][0] = {1,0}, codebook[1][1] = {0,2}

    float[] reconstructed = VectorQuantizationUtils.reconstructVector(centroid, codes, codebooks, lengths);
    Assert.assertEquals(reconstructed[0], 11.0f, 1e-6f); // 10 + 1
    Assert.assertEquals(reconstructed[1], 20.0f, 1e-6f); // 20 + 0
    Assert.assertEquals(reconstructed[2], 30.0f, 1e-6f); // 30 + 0
    Assert.assertEquals(reconstructed[3], 42.0f, 1e-6f); // 40 + 2
  }

  @Test
  public void testTrainKMeansConverges() {
    float[][] samples = {
        {0.0f, 0.0f}, {0.1f, 0.0f}, {0.0f, 0.1f},
        {10.0f, 10.0f}, {10.1f, 10.0f}, {10.0f, 10.1f}
    };
    float[][] centroids = VectorQuantizationUtils.trainKMeans(samples, 2, 42L,
        VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN);
    Assert.assertEquals(centroids.length, 2);

    // Verify the two centroids are far apart (near {0,0} and {10,10})
    float dist = VectorQuantizationUtils.computeDistance(centroids[0], centroids[1],
        VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN);
    Assert.assertTrue(dist > 5.0f, "Centroids should be well separated, got distance: " + dist);
  }

  @Test
  public void testAssignVectors() {
    float[][] centroids = {{0.0f, 0.0f}, {10.0f, 10.0f}};
    float[][] vectors = {{0.1f, 0.0f}, {9.9f, 10.0f}, {0.0f, 0.1f}};
    int[] assignments = VectorQuantizationUtils.assignVectors(vectors, centroids,
        VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN);
    Assert.assertEquals(assignments[0], 0);
    Assert.assertEquals(assignments[1], 1);
    Assert.assertEquals(assignments[2], 0);
  }

  @Test
  public void testFindNearestCentroid() {
    float[][] centroids = {{0.0f, 0.0f}, {10.0f, 10.0f}, {5.0f, 5.0f}};
    float[] query = {4.9f, 5.1f};
    int nearest = VectorQuantizationUtils.findNearestCentroid(query, centroids,
        VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN);
    Assert.assertEquals(nearest, 2);
  }

  @Test
  public void testClone2d() {
    float[][] original = {{1.0f, 2.0f}, {3.0f, 4.0f}};
    float[][] cloned = VectorQuantizationUtils.clone2d(original);
    Assert.assertEquals(cloned.length, 2);
    Assert.assertEquals(cloned[0][0], 1.0f, 1e-6f);
    // Modifying clone should not affect original
    cloned[0][0] = 99.0f;
    Assert.assertEquals(original[0][0], 1.0f, 1e-6f);
  }
}
