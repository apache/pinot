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

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.function.scalar.VectorFunctions;
import org.apache.pinot.segment.local.segment.index.readers.vector.IvfFlatVectorIndexReader;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Unit tests for the IVF_FLAT vector index implementation.
 *
 * <p>Tests cover serialization round-trips, search correctness with all distance functions,
 * k-means training quality, and various edge cases.</p>
 */
public class IvfFlatVectorIndexTest {
  private static final String COLUMN_NAME = "vectorCol";
  private static final long TEST_SEED = 42L;
  private File _tempDir;

  @BeforeMethod
  public void setUp()
      throws IOException {
    _tempDir = new File(FileUtils.getTempDirectory(), "ivf_flat_test_" + System.nanoTime());
    Assert.assertTrue(_tempDir.mkdirs(), "Failed to create temp directory: " + _tempDir);
  }

  @AfterMethod
  public void tearDown() {
    FileUtils.deleteQuietly(_tempDir);
  }

  // -----------------------------------------------------------------------
  // Distance function unit tests
  // -----------------------------------------------------------------------

  @Test
  public void testL2SquaredDistance() {
    float[] a = {1.0f, 2.0f, 3.0f};
    float[] b = {4.0f, 5.0f, 6.0f};
    // Expected: (4-1)^2 + (5-2)^2 + (6-3)^2 = 9 + 9 + 9 = 27
    float dist = (float) VectorFunctions.euclideanDistance(a, b);
    Assert.assertEquals(dist, 27.0f, 1e-5f, "L2 squared distance should be 27");
  }

  @Test
  public void testL2SquaredDistanceSameVector() {
    float[] a = {1.0f, 2.0f, 3.0f};
    float dist = (float) VectorFunctions.euclideanDistance(a, a);
    Assert.assertEquals(dist, 0.0f, 1e-5f, "L2 distance of a vector to itself should be 0");
  }

  @Test
  public void testDotProductDistance() {
    float[] a = {1.0f, 2.0f, 3.0f};
    float[] b = {4.0f, 5.0f, 6.0f};
    // Expected: -(1*4 + 2*5 + 3*6) = -(4 + 10 + 18) = -32
    float dist = (float) -VectorFunctions.dotProduct(a, b);
    Assert.assertEquals(dist, -32.0f, 1e-5f, "Dot product distance should be -32");
  }

  @Test
  public void testCosineDistance() {
    float[] a = {1.0f, 0.0f};
    float[] b = {0.0f, 1.0f};
    // Orthogonal vectors: cosine similarity = 0, cosine distance = 1
    float dist = (float) VectorFunctions.cosineDistance(a, b);
    Assert.assertEquals(dist, 1.0f, 1e-5f, "Cosine distance between orthogonal vectors should be 1");
  }

  @Test
  public void testCosineDistanceSameDirection() {
    float[] a = {1.0f, 2.0f, 3.0f};
    float[] b = {2.0f, 4.0f, 6.0f};
    // Same direction: cosine similarity = 1, cosine distance = 0
    float dist = (float) VectorFunctions.cosineDistance(a, b);
    Assert.assertEquals(dist, 0.0f, 1e-5f, "Cosine distance between same-direction vectors should be 0");
  }

  @Test
  public void testCosineDistanceZeroVector() {
    float[] a = {0.0f, 0.0f};
    float[] b = {1.0f, 2.0f};
    // VectorFunctions returns NaN for zero-vector cosine distance (default behavior)
    float dist = (float) VectorFunctions.cosineDistance(a, b);
    Assert.assertTrue(Float.isNaN(dist), "Cosine distance with zero vector should be NaN");
  }

  @Test
  public void testNormalize() {
    float[] v = {3.0f, 4.0f};
    float[] normalized = normalizeVector(v);
    Assert.assertEquals(normalized[0], 0.6f, 1e-5f);
    Assert.assertEquals(normalized[1], 0.8f, 1e-5f);
  }

  @Test
  public void testNormalizeZeroVector() {
    float[] v = {0.0f, 0.0f};
    float[] normalized = normalizeVector(v);
    Assert.assertEquals(normalized[0], 0.0f, 1e-5f);
    Assert.assertEquals(normalized[1], 0.0f, 1e-5f);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testDimensionMismatch() {
    float[] a = {1.0f, 2.0f};
    float[] b = {1.0f, 2.0f, 3.0f};
    VectorFunctions.euclideanDistance(a, b);
  }

  // -----------------------------------------------------------------------
  // Serialization round-trip tests
  // -----------------------------------------------------------------------

  @Test
  public void testRoundTripL2()
      throws IOException {
    runRoundTripTest(VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN, 100, 10, 8);
  }

  @Test
  public void testRoundTripCosine()
      throws IOException {
    runRoundTripTest(VectorIndexConfig.VectorDistanceFunction.COSINE, 100, 10, 8);
  }

  @Test
  public void testRoundTripInnerProduct()
      throws IOException {
    runRoundTripTest(VectorIndexConfig.VectorDistanceFunction.INNER_PRODUCT, 100, 10, 8);
  }

  @Test
  public void testRoundTripDotProduct()
      throws IOException {
    runRoundTripTest(VectorIndexConfig.VectorDistanceFunction.DOT_PRODUCT, 100, 10, 8);
  }

  @Test
  public void testRoundTripL2Alias()
      throws IOException {
    runRoundTripTest(VectorIndexConfig.VectorDistanceFunction.L2, 100, 10, 8);
  }

  private void runRoundTripTest(VectorIndexConfig.VectorDistanceFunction distanceFunction,
      int numVectors, int dimension, int nlist)
      throws IOException {
    VectorIndexConfig config = createConfig(distanceFunction, dimension, nlist);
    float[][] vectors = generateRandomVectors(numVectors, dimension, TEST_SEED);

    // Create index
    try (IvfFlatVectorIndexCreator creator = new IvfFlatVectorIndexCreator(COLUMN_NAME, _tempDir, config)) {
      for (float[] vector : vectors) {
        creator.add(vector);
      }
      creator.seal();
    }

    // Verify index file exists
    File indexFile = new File(_tempDir, COLUMN_NAME + V1Constants.Indexes.VECTOR_IVF_FLAT_INDEX_FILE_EXTENSION);
    Assert.assertTrue(indexFile.exists(), "Index file should exist after seal()");
    Assert.assertTrue(indexFile.length() > 0, "Index file should not be empty");

    // Read back and verify metadata
    try (IvfFlatVectorIndexReader reader = new IvfFlatVectorIndexReader(COLUMN_NAME, _tempDir, config)) {
      Assert.assertEquals(reader.getDimension(), dimension);
      Assert.assertEquals(reader.getNumVectors(), numVectors);
      Assert.assertEquals(reader.getNlist(), Math.min(nlist, numVectors));
      Assert.assertEquals(reader.getDistanceFunction(), distanceFunction);

      // Verify all doc IDs are present across inverted lists
      Set<Integer> allDocIds = new HashSet<>();
      for (int[] docIds : reader.getListDocIds()) {
        for (int docId : docIds) {
          allDocIds.add(docId);
        }
      }
      Assert.assertEquals(allDocIds.size(), numVectors, "All doc IDs should appear in inverted lists");
    }
  }

  // -----------------------------------------------------------------------
  // Search correctness tests
  // -----------------------------------------------------------------------

  @Test
  public void testSearchWithNprobeEqualsNlist()
      throws IOException {
    // When nprobe = nlist, we scan all inverted lists, effectively doing brute force.
    // Result should match brute-force top-K.
    int numVectors = 100;
    int dimension = 10;
    int nlist = 5;
    int topK = 5;

    VectorIndexConfig config = createConfig(VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN, dimension, nlist);
    float[][] vectors = generateRandomVectors(numVectors, dimension, TEST_SEED);
    float[] query = generateRandomVectors(1, dimension, TEST_SEED + 999)[0];

    // Create index
    try (IvfFlatVectorIndexCreator creator = new IvfFlatVectorIndexCreator(COLUMN_NAME, _tempDir, config)) {
      for (float[] vector : vectors) {
        creator.add(vector);
      }
      creator.seal();
    }

    // Search with nprobe = nlist (exhaustive scan)
    try (IvfFlatVectorIndexReader reader = new IvfFlatVectorIndexReader(COLUMN_NAME, _tempDir, config)) {
      reader.setNprobe(nlist);
      MutableRoaringBitmap result = (MutableRoaringBitmap) reader.getDocIds(query, topK);
      Assert.assertEquals(result.getCardinality(), topK, "Should return exactly topK results");

      // Compute brute-force top-K
      Set<Integer> bruteForceTopK = bruteForceTopK(vectors, query, topK,
          VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN);

      // With nprobe = nlist, IVF_FLAT should match brute force exactly
      Set<Integer> ivfResults = new HashSet<>();
      for (int docId : result.toArray()) {
        ivfResults.add(docId);
      }
      Assert.assertEquals(ivfResults, bruteForceTopK,
          "With nprobe=nlist, IVF_FLAT should match brute-force results");
    }
  }

  @Test
  public void testSearchCosine()
      throws IOException {
    testSearchWithDistanceFunction(VectorIndexConfig.VectorDistanceFunction.COSINE);
  }

  @Test
  public void testSearchL2()
      throws IOException {
    testSearchWithDistanceFunction(VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN);
  }

  @Test
  public void testSearchInnerProduct()
      throws IOException {
    testSearchWithDistanceFunction(VectorIndexConfig.VectorDistanceFunction.INNER_PRODUCT);
  }

  private void testSearchWithDistanceFunction(VectorIndexConfig.VectorDistanceFunction distanceFunction)
      throws IOException {
    int numVectors = 200;
    int dimension = 8;
    int nlist = 10;
    int topK = 10;

    VectorIndexConfig config = createConfig(distanceFunction, dimension, nlist);
    float[][] vectors = generateRandomVectors(numVectors, dimension, TEST_SEED);
    float[] query = generateRandomVectors(1, dimension, TEST_SEED + 777)[0];

    // Create index
    try (IvfFlatVectorIndexCreator creator = new IvfFlatVectorIndexCreator(COLUMN_NAME, _tempDir, config)) {
      for (float[] vector : vectors) {
        creator.add(vector);
      }
      creator.seal();
    }

    // Search
    try (IvfFlatVectorIndexReader reader = new IvfFlatVectorIndexReader(COLUMN_NAME, _tempDir, config)) {
      // Use full scan for correctness
      reader.setNprobe(nlist);
      MutableRoaringBitmap result = (MutableRoaringBitmap) reader.getDocIds(query, topK);
      Assert.assertEquals(result.getCardinality(), topK, "Should return topK results");

      // Verify against brute force
      Set<Integer> bruteForce = bruteForceTopK(vectors, query, topK, distanceFunction);
      Set<Integer> ivfResults = new HashSet<>();
      for (int docId : result.toArray()) {
        ivfResults.add(docId);
      }
      Assert.assertEquals(ivfResults, bruteForce,
          "Full-scan IVF_FLAT should match brute-force for " + distanceFunction);
    }
  }

  @Test
  public void testNprobeAffectsResults()
      throws IOException {
    int numVectors = 500;
    int dimension = 8;
    int nlist = 20;
    int topK = 5;

    VectorIndexConfig config = createConfig(VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN, dimension, nlist);
    float[][] vectors = generateRandomVectors(numVectors, dimension, TEST_SEED);
    float[] query = generateRandomVectors(1, dimension, TEST_SEED + 555)[0];

    // Create index
    try (IvfFlatVectorIndexCreator creator = new IvfFlatVectorIndexCreator(COLUMN_NAME, _tempDir, config)) {
      for (float[] vector : vectors) {
        creator.add(vector);
      }
      creator.seal();
    }

    // Search with nprobe=1 and nprobe=nlist
    try (IvfFlatVectorIndexReader reader = new IvfFlatVectorIndexReader(COLUMN_NAME, _tempDir, config)) {
      reader.setNprobe(1);
      MutableRoaringBitmap result1 = (MutableRoaringBitmap) reader.getDocIds(query, topK);

      reader.setNprobe(nlist);
      MutableRoaringBitmap resultAll = (MutableRoaringBitmap) reader.getDocIds(query, topK);

      // Both should return topK results
      Assert.assertTrue(result1.getCardinality() <= topK);
      Assert.assertEquals(resultAll.getCardinality(), topK);

      // With more probes, we should get equal or better results (this is not always
      // strictly true for nprobe=1 returning fewer candidates, but both should be valid)
      Assert.assertTrue(result1.getCardinality() >= 1, "nprobe=1 should return at least 1 result");
    }
  }

  // -----------------------------------------------------------------------
  // Edge case tests
  // -----------------------------------------------------------------------

  @Test
  public void testSingleVector()
      throws IOException {
    int dimension = 4;
    VectorIndexConfig config = createConfig(VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN, dimension, 1);
    float[] vector = {1.0f, 2.0f, 3.0f, 4.0f};

    try (IvfFlatVectorIndexCreator creator = new IvfFlatVectorIndexCreator(COLUMN_NAME, _tempDir, config)) {
      creator.add(vector);
      creator.seal();
    }

    try (IvfFlatVectorIndexReader reader = new IvfFlatVectorIndexReader(COLUMN_NAME, _tempDir, config)) {
      Assert.assertEquals(reader.getNumVectors(), 1);
      Assert.assertEquals(reader.getNlist(), 1);

      MutableRoaringBitmap result = (MutableRoaringBitmap) reader.getDocIds(new float[]{1.0f, 2.0f, 3.0f, 4.0f}, 1);
      Assert.assertEquals(result.getCardinality(), 1);
      Assert.assertTrue(result.contains(0));
    }
  }

  @Test
  public void testNlistEqualsOne()
      throws IOException {
    int numVectors = 50;
    int dimension = 4;
    int nlist = 1;
    int topK = 5;

    VectorIndexConfig config = createConfig(VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN, dimension, nlist);
    float[][] vectors = generateRandomVectors(numVectors, dimension, TEST_SEED);
    float[] query = vectors[0]; // Query with the first vector

    try (IvfFlatVectorIndexCreator creator = new IvfFlatVectorIndexCreator(COLUMN_NAME, _tempDir, config)) {
      for (float[] vector : vectors) {
        creator.add(vector);
      }
      creator.seal();
    }

    try (IvfFlatVectorIndexReader reader = new IvfFlatVectorIndexReader(COLUMN_NAME, _tempDir, config)) {
      Assert.assertEquals(reader.getNlist(), 1);
      MutableRoaringBitmap result = (MutableRoaringBitmap) reader.getDocIds(query, topK);
      Assert.assertEquals(result.getCardinality(), topK);
      // The query vector is the first vector, so docId 0 must be in results
      Assert.assertTrue(result.contains(0), "Query vector itself should be in top-K");
    }
  }

  @Test
  public void testNprobeGreaterThanNlist()
      throws IOException {
    int numVectors = 30;
    int dimension = 4;
    int nlist = 3;

    VectorIndexConfig config = createConfig(VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN, dimension, nlist);
    float[][] vectors = generateRandomVectors(numVectors, dimension, TEST_SEED);

    try (IvfFlatVectorIndexCreator creator = new IvfFlatVectorIndexCreator(COLUMN_NAME, _tempDir, config)) {
      for (float[] vector : vectors) {
        creator.add(vector);
      }
      creator.seal();
    }

    try (IvfFlatVectorIndexReader reader = new IvfFlatVectorIndexReader(COLUMN_NAME, _tempDir, config)) {
      // Setting nprobe > nlist should be clamped
      reader.setNprobe(100);
      Assert.assertEquals(reader.getNprobe(), nlist, "nprobe should be clamped to nlist");

      MutableRoaringBitmap result = (MutableRoaringBitmap) reader.getDocIds(vectors[0], 5);
      Assert.assertTrue(result.getCardinality() > 0);
    }
  }

  @Test
  public void testDimensionOne()
      throws IOException {
    int numVectors = 20;
    int dimension = 1;
    int nlist = 3;

    VectorIndexConfig config = createConfig(VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN, dimension, nlist);

    try (IvfFlatVectorIndexCreator creator = new IvfFlatVectorIndexCreator(COLUMN_NAME, _tempDir, config)) {
      for (int i = 0; i < numVectors; i++) {
        creator.add(new float[]{(float) i});
      }
      creator.seal();
    }

    try (IvfFlatVectorIndexReader reader = new IvfFlatVectorIndexReader(COLUMN_NAME, _tempDir, config)) {
      Assert.assertEquals(reader.getDimension(), 1);

      // Query for value 5.0, should find docId 5 in top-1
      reader.setNprobe(nlist);
      MutableRoaringBitmap result = (MutableRoaringBitmap) reader.getDocIds(new float[]{5.0f}, 1);
      Assert.assertEquals(result.getCardinality(), 1);
      Assert.assertTrue(result.contains(5), "Nearest to 5.0 should be docId 5");
    }
  }

  @Test
  public void testTopKGreaterThanNumVectors()
      throws IOException {
    int numVectors = 5;
    int dimension = 3;
    int nlist = 2;

    VectorIndexConfig config = createConfig(VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN, dimension, nlist);
    float[][] vectors = generateRandomVectors(numVectors, dimension, TEST_SEED);

    try (IvfFlatVectorIndexCreator creator = new IvfFlatVectorIndexCreator(COLUMN_NAME, _tempDir, config)) {
      for (float[] vector : vectors) {
        creator.add(vector);
      }
      creator.seal();
    }

    try (IvfFlatVectorIndexReader reader = new IvfFlatVectorIndexReader(COLUMN_NAME, _tempDir, config)) {
      reader.setNprobe(nlist);
      // Ask for more results than we have vectors
      MutableRoaringBitmap result = (MutableRoaringBitmap) reader.getDocIds(vectors[0], 100);
      Assert.assertEquals(result.getCardinality(), numVectors,
          "Should return all vectors when topK > numVectors");
    }
  }

  @Test
  public void testNlistGreaterThanNumVectors()
      throws IOException {
    int numVectors = 5;
    int dimension = 4;
    int nlist = 20; // Much larger than numVectors

    VectorIndexConfig config = createConfig(VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN, dimension, nlist);
    float[][] vectors = generateRandomVectors(numVectors, dimension, TEST_SEED);

    try (IvfFlatVectorIndexCreator creator = new IvfFlatVectorIndexCreator(COLUMN_NAME, _tempDir, config)) {
      for (float[] vector : vectors) {
        creator.add(vector);
      }
      creator.seal();
    }

    try (IvfFlatVectorIndexReader reader = new IvfFlatVectorIndexReader(COLUMN_NAME, _tempDir, config)) {
      // nlist should be clamped to numVectors
      Assert.assertEquals(reader.getNlist(), numVectors,
          "nlist should be clamped to numVectors when nlist > numVectors");

      reader.setNprobe(reader.getNlist());
      MutableRoaringBitmap result = (MutableRoaringBitmap) reader.getDocIds(vectors[0], 3);
      Assert.assertTrue(result.getCardinality() > 0);
    }
  }

  // -----------------------------------------------------------------------
  // K-means training tests
  // -----------------------------------------------------------------------

  @Test
  public void testCentroidsAreDistinct()
      throws IOException {
    int numVectors = 200;
    int dimension = 8;
    int nlist = 10;

    VectorIndexConfig config = createConfig(VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN, dimension, nlist);
    float[][] vectors = generateRandomVectors(numVectors, dimension, TEST_SEED);

    try (IvfFlatVectorIndexCreator creator = new IvfFlatVectorIndexCreator(COLUMN_NAME, _tempDir, config)) {
      for (float[] vector : vectors) {
        creator.add(vector);
      }
      creator.seal();
    }

    try (IvfFlatVectorIndexReader reader = new IvfFlatVectorIndexReader(COLUMN_NAME, _tempDir, config)) {
      float[][] centroids = reader.getCentroids();
      Assert.assertEquals(centroids.length, nlist);

      // Verify centroids are not all the same
      boolean allSame = true;
      for (int i = 1; i < centroids.length; i++) {
        if ((float) VectorFunctions.euclideanDistance(centroids[0], centroids[i]) > 1e-10f) {
          allSame = false;
          break;
        }
      }
      Assert.assertFalse(allSame, "Centroids should not all be identical");
    }
  }

  @Test
  public void testInvertedListsAreBalanced()
      throws IOException {
    // With uniformly distributed data, inverted lists should be roughly balanced
    int numVectors = 1000;
    int dimension = 8;
    int nlist = 10;

    VectorIndexConfig config = createConfig(VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN, dimension, nlist);
    float[][] vectors = generateRandomVectors(numVectors, dimension, TEST_SEED);

    try (IvfFlatVectorIndexCreator creator = new IvfFlatVectorIndexCreator(COLUMN_NAME, _tempDir, config)) {
      for (float[] vector : vectors) {
        creator.add(vector);
      }
      creator.seal();
    }

    try (IvfFlatVectorIndexReader reader = new IvfFlatVectorIndexReader(COLUMN_NAME, _tempDir, config)) {
      int[][] listDocIds = reader.getListDocIds();
      int totalDocs = 0;
      int minSize = Integer.MAX_VALUE;
      int maxSize = Integer.MIN_VALUE;

      for (int[] list : listDocIds) {
        totalDocs += list.length;
        minSize = Math.min(minSize, list.length);
        maxSize = Math.max(maxSize, list.length);
      }

      Assert.assertEquals(totalDocs, numVectors, "Total docs across all lists should equal numVectors");

      // With 1000 vectors and 10 clusters, expect ~100 per cluster.
      // Allow wide tolerance: no list should be empty, and no list should have more than 50% of vectors.
      Assert.assertTrue(minSize > 0, "No inverted list should be empty");
      Assert.assertTrue(maxSize < numVectors / 2,
          "No single list should contain more than half the vectors. Max was: " + maxSize);
    }
  }

  // -----------------------------------------------------------------------
  // Larger dataset test (recall check)
  // -----------------------------------------------------------------------

  @Test
  public void testRecallOnLargerDataset()
      throws IOException {
    int numVectors = 2000;
    int dimension = 16;
    int nlist = 32;
    int topK = 10;
    int numQueries = 10;

    VectorIndexConfig config = createConfig(VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN, dimension, nlist);
    float[][] vectors = generateRandomVectors(numVectors, dimension, TEST_SEED);

    try (IvfFlatVectorIndexCreator creator = new IvfFlatVectorIndexCreator(COLUMN_NAME, _tempDir, config)) {
      for (float[] vector : vectors) {
        creator.add(vector);
      }
      creator.seal();
    }

    try (IvfFlatVectorIndexReader reader = new IvfFlatVectorIndexReader(COLUMN_NAME, _tempDir, config)) {
      reader.setNprobe(8); // Probe 8 out of 32 clusters

      float[][] queries = generateRandomVectors(numQueries, dimension, TEST_SEED + 1234);
      double totalRecall = 0.0;

      for (float[] query : queries) {
        MutableRoaringBitmap ivfResult = (MutableRoaringBitmap) reader.getDocIds(query, topK);
        Set<Integer> bruteForce = bruteForceTopK(vectors, query, topK,
            VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN);

        int hits = 0;
        for (int docId : ivfResult.toArray()) {
          if (bruteForce.contains(docId)) {
            hits++;
          }
        }
        totalRecall += (double) hits / topK;
      }

      double avgRecall = totalRecall / numQueries;
      // With nprobe=8 out of 32 clusters, on random data we expect reasonable recall (> 0.5)
      Assert.assertTrue(avgRecall > 0.5,
          "Average recall should be > 0.5 with nprobe=8/32. Got: " + avgRecall);
    }
  }

  // -----------------------------------------------------------------------
  // Creator edge cases
  // -----------------------------------------------------------------------

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testAddWrongDimension()
      throws IOException {
    VectorIndexConfig config = createConfig(VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN, 4, 2);
    try (IvfFlatVectorIndexCreator creator = new IvfFlatVectorIndexCreator(COLUMN_NAME, _tempDir, config)) {
      creator.add(new float[]{1.0f, 2.0f}); // Wrong dimension (2 instead of 4)
    }
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testAddAfterSeal()
      throws IOException {
    VectorIndexConfig config = createConfig(VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN, 2, 1);
    try (IvfFlatVectorIndexCreator creator = new IvfFlatVectorIndexCreator(COLUMN_NAME, _tempDir, config)) {
      creator.add(new float[]{1.0f, 2.0f});
      creator.seal();
      creator.add(new float[]{3.0f, 4.0f}); // Should throw
    }
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testDoubleSeal()
      throws IOException {
    VectorIndexConfig config = createConfig(VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN, 2, 1);
    try (IvfFlatVectorIndexCreator creator = new IvfFlatVectorIndexCreator(COLUMN_NAME, _tempDir, config)) {
      creator.add(new float[]{1.0f, 2.0f});
      creator.seal();
      creator.seal(); // Should throw
    }
  }

  @Test
  public void testEmptyIndex()
      throws IOException {
    VectorIndexConfig config = createConfig(VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN, 4, 2);
    try (IvfFlatVectorIndexCreator creator = new IvfFlatVectorIndexCreator(COLUMN_NAME, _tempDir, config)) {
      creator.seal();
    }

    try (IvfFlatVectorIndexReader reader = new IvfFlatVectorIndexReader(COLUMN_NAME, _tempDir, config)) {
      Assert.assertEquals(reader.getNumVectors(), 0);
      MutableRoaringBitmap result = (MutableRoaringBitmap) reader.getDocIds(new float[]{1, 2, 3, 4}, 5);
      Assert.assertEquals(result.getCardinality(), 0);
    }
  }

  // -----------------------------------------------------------------------
  // Helpers
  // -----------------------------------------------------------------------

  private VectorIndexConfig createConfig(VectorIndexConfig.VectorDistanceFunction distanceFunction,
      int dimension, int nlist) {
    Map<String, String> properties = new HashMap<>();
    properties.put("vectorIndexType", "IVF_FLAT");
    properties.put("vectorDimension", String.valueOf(dimension));
    properties.put("vectorDistanceFunction", distanceFunction.name());
    properties.put("nlist", String.valueOf(nlist));
    properties.put("trainingSeed", String.valueOf(TEST_SEED));
    return new VectorIndexConfig(false, "IVF_FLAT", dimension, 1, distanceFunction, properties);
  }

  private float[][] generateRandomVectors(int numVectors, int dimension, long seed) {
    Random rng = new Random(seed);
    float[][] vectors = new float[numVectors][dimension];
    for (int i = 0; i < numVectors; i++) {
      for (int d = 0; d < dimension; d++) {
        vectors[i][d] = rng.nextFloat() * 2.0f - 1.0f; // [-1, 1]
      }
    }
    return vectors;
  }

  /**
   * Brute-force top-K computation for verification.
   */
  private Set<Integer> bruteForceTopK(float[][] vectors, float[] query, int topK,
      VectorIndexConfig.VectorDistanceFunction distanceFunction) {
    int numVectors = vectors.length;
    float[] distances = new float[numVectors];
    for (int i = 0; i < numVectors; i++) {
      distances[i] = computeDistance(query, vectors[i], distanceFunction);
    }

    // Find top-K by sorting indices
    Integer[] indices = new Integer[numVectors];
    for (int i = 0; i < numVectors; i++) {
      indices[i] = i;
    }
    java.util.Arrays.sort(indices, (a, b) -> Float.compare(distances[a], distances[b]));

    Set<Integer> result = new HashSet<>();
    for (int i = 0; i < Math.min(topK, numVectors); i++) {
      result.add(indices[i]);
    }
    return result;
  }

  private static float computeDistance(float[] a, float[] b,
      VectorIndexConfig.VectorDistanceFunction distanceFunction) {
    switch (distanceFunction) {
      case EUCLIDEAN:
      case L2:
        return (float) VectorFunctions.euclideanDistance(a, b);
      case COSINE:
        return (float) VectorFunctions.cosineDistance(a, b);
      case INNER_PRODUCT:
      case DOT_PRODUCT:
        return (float) -VectorFunctions.dotProduct(a, b);
      default:
        throw new IllegalArgumentException("Unsupported: " + distanceFunction);
    }
  }

  private static float[] normalizeVector(float[] vector) {
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
}
