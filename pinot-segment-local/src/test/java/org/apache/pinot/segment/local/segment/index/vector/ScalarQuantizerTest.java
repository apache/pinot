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

import java.util.Random;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Tests for {@link ScalarQuantizer} SQ8 and SQ4 implementations.
 */
public class ScalarQuantizerTest {

  private static final int DIMENSION = 128;
  private static final float TOLERANCE_SQ8 = 0.01f;
  private static final float TOLERANCE_SQ4 = 0.1f;
  private static final float DISTANCE_TOLERANCE = 1e-4f;

  @Test
  public void testSq8TrainAndEncode() {
    float[][] vectors = generateRandomVectors(1000, DIMENSION, 42);
    ScalarQuantizer sq = ScalarQuantizer.train(vectors, DIMENSION, ScalarQuantizer.BitWidth.SQ8);

    assertTrue(sq.isTrained());
    assertEquals(sq.getDimension(), DIMENSION);
    assertEquals(sq.getBitWidth(), ScalarQuantizer.BitWidth.SQ8);
    assertEquals(sq.getEncodedBytesPerVector(), DIMENSION);
  }

  @Test
  public void testSq8EncodeDecode() {
    float[][] vectors = generateRandomVectors(1000, DIMENSION, 42);
    ScalarQuantizer sq = ScalarQuantizer.train(vectors, DIMENSION, ScalarQuantizer.BitWidth.SQ8);

    // Encode and decode a vector, check reconstruction error
    float[] original = vectors[0];
    byte[] encoded = sq.encode(original);
    assertEquals(encoded.length, DIMENSION);

    float[] decoded = sq.decode(encoded);
    assertEquals(decoded.length, DIMENSION);

    for (int d = 0; d < DIMENSION; d++) {
      assertEquals(decoded[d], original[d], TOLERANCE_SQ8,
          "SQ8 reconstruction error at dim " + d + " exceeds tolerance");
    }
  }

  @Test
  public void testSq4TrainAndEncode() {
    float[][] vectors = generateRandomVectors(1000, DIMENSION, 42);
    ScalarQuantizer sq = ScalarQuantizer.train(vectors, DIMENSION, ScalarQuantizer.BitWidth.SQ4);

    assertTrue(sq.isTrained());
    assertEquals(sq.getEncodedBytesPerVector(), (DIMENSION + 1) / 2);
  }

  @Test
  public void testSq4EncodeDecode() {
    float[][] vectors = generateRandomVectors(1000, DIMENSION, 42);
    ScalarQuantizer sq = ScalarQuantizer.train(vectors, DIMENSION, ScalarQuantizer.BitWidth.SQ4);

    float[] original = vectors[0];
    byte[] encoded = sq.encode(original);
    assertEquals(encoded.length, (DIMENSION + 1) / 2);

    float[] decoded = sq.decode(encoded);
    assertEquals(decoded.length, DIMENSION);

    for (int d = 0; d < DIMENSION; d++) {
      assertEquals(decoded[d], original[d], TOLERANCE_SQ4,
          "SQ4 reconstruction error at dim " + d + " exceeds tolerance");
    }
  }

  @Test
  public void testSq4OddDimension() {
    int oddDim = 127;
    float[][] vectors = generateRandomVectors(100, oddDim, 42);
    ScalarQuantizer sq = ScalarQuantizer.train(vectors, oddDim, ScalarQuantizer.BitWidth.SQ4);

    assertEquals(sq.getEncodedBytesPerVector(), (oddDim + 1) / 2);

    float[] original = vectors[0];
    byte[] encoded = sq.encode(original);
    float[] decoded = sq.decode(encoded);
    assertEquals(decoded.length, oddDim);
  }

  @Test
  public void testDistanceComputation() {
    float[][] vectors = generateRandomVectors(1000, DIMENSION, 42);
    ScalarQuantizer sq = ScalarQuantizer.train(vectors, DIMENSION, ScalarQuantizer.BitWidth.SQ8);

    float[] query = vectors[0];
    float[] doc = vectors[1];
    byte[] encodedDoc = sq.encode(doc);

    float approxDistance = sq.computeDistance(query, encodedDoc,
        VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN);
    assertTrue(approxDistance >= 0, "Distance should be non-negative");

    // Cosine distance
    float cosineDistance = sq.computeDistance(query, encodedDoc,
        VectorIndexConfig.VectorDistanceFunction.COSINE);
    assertNotNull(cosineDistance);

    // Inner product (can be negative)
    float ipDistance = sq.computeDistance(query, encodedDoc,
        VectorIndexConfig.VectorDistanceFunction.INNER_PRODUCT);
    assertNotNull(ipDistance);
  }

  @Test
  public void testDirectDistanceMatchesDecodePathForSq8() {
    assertDirectDistanceMatchesDecodePath(ScalarQuantizer.BitWidth.SQ8);
  }

  @Test
  public void testDirectDistanceMatchesDecodePathForSq4() {
    assertDirectDistanceMatchesDecodePath(ScalarQuantizer.BitWidth.SQ4);
  }

  @Test
  public void testCosineDistanceWithZeroNormQueryReturnsFiniteFallback() {
    float[][] vectors = generateRandomVectors(1000, DIMENSION, 42);
    ScalarQuantizer sq = ScalarQuantizer.train(vectors, DIMENSION, ScalarQuantizer.BitWidth.SQ8);

    float[] zeroQuery = new float[DIMENSION];
    byte[] encodedDoc = sq.encode(vectors[0]);

    assertEquals(sq.computeDistance(zeroQuery, encodedDoc, VectorIndexConfig.VectorDistanceFunction.COSINE), 1.0f);
  }

  @Test
  public void testSerializeDeserialize() {
    float[][] vectors = generateRandomVectors(1000, DIMENSION, 42);
    ScalarQuantizer original = ScalarQuantizer.train(vectors, DIMENSION, ScalarQuantizer.BitWidth.SQ8);

    byte[] serialized = original.serialize();
    ScalarQuantizer restored = ScalarQuantizer.deserialize(serialized);

    assertEquals(restored.getDimension(), DIMENSION);
    assertEquals(restored.getBitWidth(), ScalarQuantizer.BitWidth.SQ8);
    assertTrue(restored.isTrained());

    // Verify min/max values match
    float[] origMin = original.getMinValues();
    float[] restoredMin = restored.getMinValues();
    for (int d = 0; d < DIMENSION; d++) {
      assertEquals(restoredMin[d], origMin[d], 1e-6f, "Min mismatch at dim " + d);
    }

    // Verify encode/decode consistency
    float[] testVec = vectors[0];
    byte[] origEncoded = original.encode(testVec);
    byte[] restoredEncoded = restored.encode(testVec);
    assertEquals(restoredEncoded, origEncoded, "Encoded bytes should match after deserialization");
  }

  @Test
  public void testSq4SerializeDeserialize() {
    float[][] vectors = generateRandomVectors(1000, DIMENSION, 42);
    ScalarQuantizer original = ScalarQuantizer.train(vectors, DIMENSION, ScalarQuantizer.BitWidth.SQ4);

    byte[] serialized = original.serialize();
    ScalarQuantizer restored = ScalarQuantizer.deserialize(serialized);

    assertEquals(restored.getBitWidth(), ScalarQuantizer.BitWidth.SQ4);

    float[] testVec = vectors[0];
    byte[] origEncoded = original.encode(testVec);
    byte[] restoredEncoded = restored.encode(testVec);
    assertEquals(restoredEncoded, origEncoded);
  }

  @Test
  public void testSq8RecallQuality() {
    // Generate vectors and verify that SQ8 approximate distances preserve
    // approximate ordering (recall quality check)
    int numVectors = 500;
    float[][] vectors = generateRandomVectors(numVectors, DIMENSION, 42);
    ScalarQuantizer sq = ScalarQuantizer.train(vectors, DIMENSION, ScalarQuantizer.BitWidth.SQ8);

    float[] query = vectors[0];
    byte[][] encoded = new byte[numVectors][];
    for (int i = 0; i < numVectors; i++) {
      encoded[i] = sq.encode(vectors[i]);
    }

    // Find top-10 by exact distance and by approximate distance, check overlap
    int topK = 10;
    int[] exactTop = findTopK(query, vectors, topK, VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN);
    int[] approxTop = findTopKApprox(query, encoded, sq, topK, VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN);

    int overlap = countOverlap(exactTop, approxTop);
    // SQ8 should have >= 80% recall for top-10 on random data
    assertTrue(overlap >= 8, "SQ8 recall too low: " + overlap + "/10");
  }

  @Test
  public void testSq4RecallQuality() {
    int numVectors = 500;
    float[][] vectors = generateRandomVectors(numVectors, DIMENSION, 42);
    ScalarQuantizer sq = ScalarQuantizer.train(vectors, DIMENSION, ScalarQuantizer.BitWidth.SQ4);

    float[] query = vectors[0];
    byte[][] encoded = new byte[numVectors][];
    for (int i = 0; i < numVectors; i++) {
      encoded[i] = sq.encode(vectors[i]);
    }

    int topK = 10;
    int[] exactTop = findTopK(query, vectors, topK, VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN);
    int[] approxTop = findTopKApprox(query, encoded, sq, topK, VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN);

    int overlap = countOverlap(exactTop, approxTop);
    // SQ4 recall is lower; expect >= 50% for top-10 on random data
    assertTrue(overlap >= 5, "SQ4 recall too low: " + overlap + "/10");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testDimensionMismatch() {
    float[][] vectors = generateRandomVectors(100, DIMENSION, 42);
    ScalarQuantizer sq = ScalarQuantizer.train(vectors, DIMENSION, ScalarQuantizer.BitWidth.SQ8);
    sq.encode(new float[DIMENSION + 1]); // should throw
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testEmptyTrainingVectors() {
    ScalarQuantizer.train(new float[0][], DIMENSION, ScalarQuantizer.BitWidth.SQ8);
  }

  // -----------------------------------------------------------------------
  // Helpers
  // -----------------------------------------------------------------------

  private static float[][] generateRandomVectors(int count, int dim, long seed) {
    Random rng = new Random(seed);
    float[][] vectors = new float[count][dim];
    for (int i = 0; i < count; i++) {
      for (int d = 0; d < dim; d++) {
        vectors[i][d] = rng.nextFloat() * 2 - 1; // [-1, 1]
      }
    }
    return vectors;
  }

  private static int[] findTopK(float[] query, float[][] vectors, int k,
      VectorIndexConfig.VectorDistanceFunction df) {
    int n = vectors.length;
    float[] distances = new float[n];
    int[] indices = new int[n];
    for (int i = 0; i < n; i++) {
      distances[i] = exactDistance(query, vectors[i], df);
      indices[i] = i;
    }
    // Simple selection sort for top-K
    for (int i = 0; i < k; i++) {
      for (int j = i + 1; j < n; j++) {
        if (distances[j] < distances[i]) {
          float tmpD = distances[i];
          distances[i] = distances[j];
          distances[j] = tmpD;
          int tmpI = indices[i];
          indices[i] = indices[j];
          indices[j] = tmpI;
        }
      }
    }
    int[] result = new int[k];
    System.arraycopy(indices, 0, result, 0, k);
    return result;
  }

  private void assertDirectDistanceMatchesDecodePath(ScalarQuantizer.BitWidth bitWidth) {
    float[][] vectors = generateRandomVectors(1000, DIMENSION, 42);
    ScalarQuantizer quantizer = ScalarQuantizer.train(vectors, DIMENSION, bitWidth);

    float[] query = vectors[0];
    float[] document = vectors[1];
    byte[] encodedDoc = quantizer.encode(document);
    float[] decodedDoc = quantizer.decode(encodedDoc);

    assertEquals(quantizer.computeDistance(query, encodedDoc, VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN),
        exactDistance(query, decodedDoc, VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN), DISTANCE_TOLERANCE);
    assertEquals(quantizer.computeDistance(query, encodedDoc, VectorIndexConfig.VectorDistanceFunction.INNER_PRODUCT),
        exactDistance(query, decodedDoc, VectorIndexConfig.VectorDistanceFunction.INNER_PRODUCT), DISTANCE_TOLERANCE);
    assertEquals(quantizer.computeDistance(query, encodedDoc, VectorIndexConfig.VectorDistanceFunction.COSINE),
        exactDistance(query, decodedDoc, VectorIndexConfig.VectorDistanceFunction.COSINE), DISTANCE_TOLERANCE);
  }

  private static int[] findTopKApprox(float[] query, byte[][] encoded, ScalarQuantizer sq, int k,
      VectorIndexConfig.VectorDistanceFunction df) {
    int n = encoded.length;
    float[] distances = new float[n];
    int[] indices = new int[n];
    for (int i = 0; i < n; i++) {
      distances[i] = sq.computeDistance(query, encoded[i], df);
      indices[i] = i;
    }
    for (int i = 0; i < k; i++) {
      for (int j = i + 1; j < n; j++) {
        if (distances[j] < distances[i]) {
          float tmpD = distances[i];
          distances[i] = distances[j];
          distances[j] = tmpD;
          int tmpI = indices[i];
          indices[i] = indices[j];
          indices[j] = tmpI;
        }
      }
    }
    int[] result = new int[k];
    System.arraycopy(indices, 0, result, 0, k);
    return result;
  }

  private static float exactDistance(float[] a, float[] b, VectorIndexConfig.VectorDistanceFunction df) {
    float sum = 0;
    switch (df) {
      case EUCLIDEAN:
      case L2:
        for (int d = 0; d < a.length; d++) {
          float diff = a[d] - b[d];
          sum += diff * diff;
        }
        return sum;
      case COSINE:
        float dot = 0;
        float normA = 0;
        float normB = 0;
        for (int d = 0; d < a.length; d++) {
          dot += a[d] * b[d];
          normA += a[d] * a[d];
          normB += b[d] * b[d];
        }
        float denom = (float) (Math.sqrt(normA) * Math.sqrt(normB));
        return denom > 0 ? 1.0f - dot / denom : 1.0f;
      case INNER_PRODUCT:
      case DOT_PRODUCT:
        for (int d = 0; d < a.length; d++) {
          sum += a[d] * b[d];
        }
        return -sum;
      default:
        throw new IllegalArgumentException("Unsupported: " + df);
    }
  }

  private static int countOverlap(int[] a, int[] b) {
    int count = 0;
    for (int ai : a) {
      for (int bi : b) {
        if (ai == bi) {
          count++;
          break;
        }
      }
    }
    return count;
  }
}
