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

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.index.readers.vector.IvfPqVectorIndexReader;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class IvfPqVectorIndexTest {
  private static final int DIMENSION = 16;
  private static final int NUM_VECTORS = 500;
  private static final int TOP_K = 10;

  private File _tempDir;

  @BeforeMethod
  public void setUp() {
    _tempDir = new File(FileUtils.getTempDirectory(), "ivfpq_test_" + System.currentTimeMillis());
    _tempDir.mkdirs();
  }

  @AfterMethod
  public void tearDown()
      throws IOException {
    FileUtils.deleteDirectory(_tempDir);
  }

  @Test
  public void testCreateAndSearch()
      throws IOException {
    // Generate random vectors
    Random random = new Random(42);
    float[][] vectors = new float[NUM_VECTORS][DIMENSION];
    for (int i = 0; i < NUM_VECTORS; i++) {
      for (int d = 0; d < DIMENSION; d++) {
        vectors[i][d] = random.nextFloat();
      }
    }

    // Create IVF_PQ index
    VectorIndexConfig config = createIvfPqConfig();
    try (IvfPqVectorIndexCreator creator = new IvfPqVectorIndexCreator("embedding", _tempDir, config)) {
      for (float[] vector : vectors) {
        creator.add(vector);
      }
      creator.seal();
    }

    // Verify index file exists
    File indexFile = new File(_tempDir, "embedding" + IvfPqVectorIndexCreator.FILE_EXTENSION);
    assertTrue(indexFile.exists(), "Index file should exist");
    assertTrue(indexFile.length() > IvfPqIndexFormat.HEADER_SIZE, "Index file should be larger than header");

    // Read and search
    try (IvfPqVectorIndexReader reader = new IvfPqVectorIndexReader("embedding", _tempDir, NUM_VECTORS, 16)) {
      // Search for the first vector - it should find itself
      MutableRoaringBitmap results = reader.getDocIds(vectors[0], TOP_K);
      assertEquals(results.getCardinality(), TOP_K, "Should return exactly topK results");
      assertTrue(results.contains(0), "Should find the query vector itself (docId 0)");
    }
  }

  @Test
  public void testSearchRecallQuality()
      throws IOException {
    // Generate vectors with a clear nearest neighbor structure
    Random random = new Random(42);
    float[][] vectors = new float[NUM_VECTORS][DIMENSION];
    for (int i = 0; i < NUM_VECTORS; i++) {
      for (int d = 0; d < DIMENSION; d++) {
        vectors[i][d] = random.nextFloat();
      }
    }

    // Create index
    VectorIndexConfig config = createIvfPqConfig();
    try (IvfPqVectorIndexCreator creator = new IvfPqVectorIndexCreator("embedding", _tempDir, config)) {
      for (float[] vector : vectors) {
        creator.add(vector);
      }
      creator.seal();
    }

    // Find exact top-K by brute force
    float[] query = vectors[0];
    int[] exactTopK = findExactTopK(query, vectors, TOP_K);

    // Search with IVF_PQ
    try (IvfPqVectorIndexReader reader = new IvfPqVectorIndexReader("embedding", _tempDir, NUM_VECTORS, 16)) {
      MutableRoaringBitmap results = reader.getDocIds(query, TOP_K);

      // Count how many of the exact top-K are in the approximate results
      int hits = 0;
      for (int docId : exactTopK) {
        if (results.contains(docId)) {
          hits++;
        }
      }

      // With nprobe=16 on 500 vectors with nlist=32, recall should be decent
      // We require at least 50% recall for this test to pass
      double recall = (double) hits / TOP_K;
      assertTrue(recall >= 0.5,
          "Recall@" + TOP_K + " should be >= 50%, got " + (recall * 100) + "% (" + hits + "/" + TOP_K + ")");
    }
  }

  @Test
  public void testEmptyIndex()
      throws IOException {
    VectorIndexConfig config = createIvfPqConfig();
    try (IvfPqVectorIndexCreator creator = new IvfPqVectorIndexCreator("embedding", _tempDir, config)) {
      creator.seal();
    }

    File indexFile = new File(_tempDir, "embedding" + IvfPqVectorIndexCreator.FILE_EXTENSION);
    assertTrue(indexFile.exists(), "Index file should exist even for empty index");

    try (IvfPqVectorIndexReader reader = new IvfPqVectorIndexReader("embedding", _tempDir, 0, 8)) {
      MutableRoaringBitmap results = reader.getDocIds(new float[DIMENSION], TOP_K);
      assertEquals(results.getCardinality(), 0, "Empty index should return no results");
    }
  }

  @Test
  public void testSmallDataset()
      throws IOException {
    // Test with very few vectors (fewer than nlist)
    int numVectors = 5;
    Random random = new Random(42);
    float[][] vectors = new float[numVectors][DIMENSION];
    for (int i = 0; i < numVectors; i++) {
      for (int d = 0; d < DIMENSION; d++) {
        vectors[i][d] = random.nextFloat();
      }
    }

    VectorIndexConfig config = createIvfPqConfig();
    try (IvfPqVectorIndexCreator creator = new IvfPqVectorIndexCreator("embedding", _tempDir, config)) {
      for (float[] vector : vectors) {
        creator.add(vector);
      }
      creator.seal();
    }

    try (IvfPqVectorIndexReader reader = new IvfPqVectorIndexReader("embedding", _tempDir, numVectors, 8)) {
      MutableRoaringBitmap results = reader.getDocIds(vectors[0], 3);
      // Should return min(topK, numVectors) results
      assertTrue(results.getCardinality() <= numVectors);
      assertTrue(results.getCardinality() > 0);
    }
  }

  private VectorIndexConfig createIvfPqConfig() {
    Map<String, String> properties = new HashMap<>();
    properties.put("vectorIndexType", "IVF_PQ");
    properties.put("vectorDimension", String.valueOf(DIMENSION));
    properties.put("vectorDistanceFunction", "EUCLIDEAN");
    properties.put("nlist", "32");
    properties.put("pqM", "4");
    properties.put("pqNbits", "8");
    properties.put("trainSampleSize", "500");
    properties.put("trainingSeed", "42");
    return new VectorIndexConfig(false, "IVF_PQ", DIMENSION, 2,
        VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN, properties);
  }

  private int[] findExactTopK(float[] query, float[][] vectors, int k) {
    float[] distances = new float[vectors.length];
    int[] indices = new int[vectors.length];
    for (int i = 0; i < vectors.length; i++) {
      distances[i] = KMeans.l2DistanceSquared(query, vectors[i], query.length);
      indices[i] = i;
    }
    // Selection sort for top-k
    for (int i = 0; i < k; i++) {
      int minIdx = i;
      for (int j = i + 1; j < vectors.length; j++) {
        if (distances[j] < distances[minIdx]) {
          minIdx = j;
        }
      }
      float tmpD = distances[i];
      distances[i] = distances[minIdx];
      distances[minIdx] = tmpD;
      int tmpI = indices[i];
      indices[i] = indices[minIdx];
      indices[minIdx] = tmpI;
    }
    int[] result = new int[k];
    System.arraycopy(indices, 0, result, 0, k);
    return result;
  }
}
