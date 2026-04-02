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
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Tests for IVF_PQ runtime integration: nprobe override, exact rerank, and debug info.
 */
public class IvfPqRuntimeTest {
  private static final int DIMENSION = 16;
  private static final int NUM_VECTORS = 1000;
  private static final int TOP_K = 10;

  private File _tempDir;
  private float[][] _vectors;

  @BeforeMethod
  public void setUp()
      throws IOException {
    _tempDir = new File(FileUtils.getTempDirectory(), "ivfpq_runtime_test_" + System.currentTimeMillis());
    _tempDir.mkdirs();

    // Generate random vectors
    Random random = new Random(42);
    _vectors = new float[NUM_VECTORS][DIMENSION];
    for (int i = 0; i < NUM_VECTORS; i++) {
      for (int d = 0; d < DIMENSION; d++) {
        _vectors[i][d] = random.nextFloat();
      }
    }

    // Build index
    VectorIndexConfig config = createIvfPqConfig();
    try (IvfPqVectorIndexCreator creator = new IvfPqVectorIndexCreator("embedding", _tempDir, config)) {
      for (float[] vector : _vectors) {
        creator.add(vector);
      }
      creator.seal();
    }
  }

  @AfterMethod
  public void tearDown()
      throws IOException {
    FileUtils.deleteDirectory(_tempDir);
  }

  @Test
  public void testNprobeOverride()
      throws IOException {
    float[] query = _vectors[0];

    try (IvfPqVectorIndexReader reader = new IvfPqVectorIndexReader("embedding", _tempDir, NUM_VECTORS, 4)) {
      // Search with default nprobe=4
      ImmutableRoaringBitmap lowNprobe = reader.getDocIds(query, TOP_K, null);
      assertEquals(lowNprobe.getCardinality(), TOP_K);

      // Search with higher nprobe=32 via query option
      Map<String, String> highNprobeOptions = new HashMap<>();
      highNprobeOptions.put(QueryOptionKey.VECTOR_NPROBE, "32");
      ImmutableRoaringBitmap highNprobe = reader.getDocIds(query, TOP_K, highNprobeOptions);
      assertEquals(highNprobe.getCardinality(), TOP_K);

      // Both should find the query vector itself
      assertTrue(lowNprobe.contains(0), "Low nprobe should find docId 0 (query vector)");
      assertTrue(highNprobe.contains(0), "High nprobe should find docId 0 (query vector)");
    }
  }

  @Test
  public void testHigherNprobeImprovesRecall()
      throws IOException {
    float[] query = _vectors[50]; // Pick a non-first vector
    int[] exactTopK = findExactTopK(query, _vectors, TOP_K);

    try (IvfPqVectorIndexReader reader = new IvfPqVectorIndexReader("embedding", _tempDir, NUM_VECTORS, 1)) {
      // Very low nprobe = 1
      Map<String, String> lowOptions = new HashMap<>();
      lowOptions.put(QueryOptionKey.VECTOR_NPROBE, "1");
      lowOptions.put(QueryOptionKey.VECTOR_EXACT_RERANK, "false");
      ImmutableRoaringBitmap lowResult = reader.getDocIds(query, TOP_K, lowOptions);

      // High nprobe = 32
      Map<String, String> highOptions = new HashMap<>();
      highOptions.put(QueryOptionKey.VECTOR_NPROBE, "32");
      highOptions.put(QueryOptionKey.VECTOR_EXACT_RERANK, "false");
      ImmutableRoaringBitmap highResult = reader.getDocIds(query, TOP_K, highOptions);

      int lowHits = countHits(lowResult, exactTopK);
      int highHits = countHits(highResult, exactTopK);

      // Higher nprobe should generally give equal or better recall
      assertTrue(highHits >= lowHits,
          "Higher nprobe should have equal or better recall: low=" + lowHits + ", high=" + highHits);
    }
  }

  @Test
  public void testExactRerankImprovesQuality()
      throws IOException {
    float[] query = _vectors[50];
    int[] exactTopK = findExactTopK(query, _vectors, TOP_K);

    try (IvfPqVectorIndexReader reader = new IvfPqVectorIndexReader("embedding", _tempDir, NUM_VECTORS, 16)) {
      // Without rerank
      Map<String, String> noRerank = new HashMap<>();
      noRerank.put(QueryOptionKey.VECTOR_EXACT_RERANK, "false");
      ImmutableRoaringBitmap noRerankResult = reader.getDocIds(query, TOP_K, noRerank);

      // With rerank (default)
      Map<String, String> withRerank = new HashMap<>();
      withRerank.put(QueryOptionKey.VECTOR_EXACT_RERANK, "true");
      ImmutableRoaringBitmap rerankResult = reader.getDocIds(query, TOP_K, withRerank);

      int noRerankHits = countHits(noRerankResult, exactTopK);
      int rerankHits = countHits(rerankResult, exactTopK);

      // Rerank should generally give equal or better recall
      assertTrue(rerankHits >= noRerankHits,
          "Rerank should have equal or better recall: without=" + noRerankHits + ", with=" + rerankHits);
    }
  }

  @Test
  public void testDefaultRerankIsOn()
      throws IOException {
    float[] query = _vectors[0];
    int[] exactTopK = findExactTopK(query, _vectors, TOP_K);

    try (IvfPqVectorIndexReader reader = new IvfPqVectorIndexReader("embedding", _tempDir, NUM_VECTORS, 16)) {
      // Default (no options) should use rerank
      ImmutableRoaringBitmap defaultResult = reader.getDocIds(query, TOP_K, null);

      // Explicit rerank
      Map<String, String> rerankOptions = new HashMap<>();
      rerankOptions.put(QueryOptionKey.VECTOR_EXACT_RERANK, "true");
      ImmutableRoaringBitmap rerankResult = reader.getDocIds(query, TOP_K, rerankOptions);

      // Default should match explicit rerank (same results)
      assertEquals(defaultResult.getCardinality(), rerankResult.getCardinality(),
          "Default should produce same cardinality as explicit rerank");
    }
  }

  @Test
  public void testRerankDisabled()
      throws IOException {
    float[] query = _vectors[0];

    try (IvfPqVectorIndexReader reader = new IvfPqVectorIndexReader("embedding", _tempDir, NUM_VECTORS, 16)) {
      Map<String, String> noRerank = new HashMap<>();
      noRerank.put(QueryOptionKey.VECTOR_EXACT_RERANK, "false");
      ImmutableRoaringBitmap result = reader.getDocIds(query, TOP_K, noRerank);

      assertEquals(result.getCardinality(), TOP_K, "Should still return topK results");
      assertTrue(result.contains(0), "Should find the query vector itself");
    }
  }

  @Test
  public void testDebugInfo()
      throws IOException {
    try (IvfPqVectorIndexReader reader = new IvfPqVectorIndexReader("embedding", _tempDir, NUM_VECTORS, 8)) {
      Map<String, String> debugInfo = reader.getIndexDebugInfo();
      assertNotNull(debugInfo);
      assertFalse(debugInfo.isEmpty());
      assertEquals(debugInfo.get("backend"), "IVF_PQ");
      assertEquals(debugInfo.get("pqM"), "4");
      assertEquals(debugInfo.get("pqNbits"), "8");
      assertEquals(debugInfo.get("nprobe"), "8");
      assertEquals(debugInfo.get("numVectors"), String.valueOf(NUM_VECTORS));
      assertEquals(debugInfo.get("dimension"), String.valueOf(DIMENSION));
      assertEquals(debugInfo.get("exactRerank"), "true");
    }
  }

  @Test
  public void testCosineDistanceRerank()
      throws IOException {
    // Build a separate index with COSINE distance
    File cosineDir = new File(_tempDir, "cosine");
    cosineDir.mkdirs();

    // Generate normalized vectors for cosine
    Random random = new Random(123);
    int n = 500;
    float[][] cosineVectors = new float[n][DIMENSION];
    for (int i = 0; i < n; i++) {
      float norm = 0;
      for (int d = 0; d < DIMENSION; d++) {
        cosineVectors[i][d] = random.nextFloat() - 0.5f;
        norm += cosineVectors[i][d] * cosineVectors[i][d];
      }
      norm = (float) Math.sqrt(norm);
      for (int d = 0; d < DIMENSION; d++) {
        cosineVectors[i][d] /= norm;
      }
    }

    VectorIndexConfig config = createIvfPqConfigWithDistance("COSINE");
    try (IvfPqVectorIndexCreator creator = new IvfPqVectorIndexCreator("emb", cosineDir, config)) {
      for (float[] v : cosineVectors) {
        creator.add(v);
      }
      creator.seal();
    }

    // Search and verify rerank uses cosine distance
    try (IvfPqVectorIndexReader reader = new IvfPqVectorIndexReader("emb", cosineDir, n, 16)) {
      Map<String, String> debugInfo = reader.getIndexDebugInfo();
      assertEquals(debugInfo.get("distanceFunction"), "COSINE");

      // With rerank, the query vector itself should always be found
      ImmutableRoaringBitmap result = reader.getDocIds(cosineVectors[0], TOP_K, null);
      assertTrue(result.contains(0), "Should find query vector (docId 0) with cosine rerank");
      assertEquals(result.getCardinality(), TOP_K);

      // Find exact top-K by cosine distance and check recall
      int[] exactTopK = findExactTopKByDistance(cosineVectors[0], cosineVectors, TOP_K,
          IvfPqIndexFormat.DIST_COSINE);
      int hits = countHits(result, exactTopK);
      assertTrue(hits >= 5, "Recall@10 with cosine rerank should be >= 50%, got " + hits + "/10");
    }
  }

  @Test
  public void testInnerProductDistanceRerank()
      throws IOException {
    File ipDir = new File(_tempDir, "ip");
    ipDir.mkdirs();

    Random random = new Random(456);
    int n = 500;
    float[][] ipVectors = new float[n][DIMENSION];
    for (int i = 0; i < n; i++) {
      for (int d = 0; d < DIMENSION; d++) {
        ipVectors[i][d] = random.nextFloat();
      }
    }

    VectorIndexConfig config = createIvfPqConfigWithDistance("INNER_PRODUCT");
    try (IvfPqVectorIndexCreator creator = new IvfPqVectorIndexCreator("emb", ipDir, config)) {
      for (float[] v : ipVectors) {
        creator.add(v);
      }
      creator.seal();
    }

    try (IvfPqVectorIndexReader reader = new IvfPqVectorIndexReader("emb", ipDir, n, 16)) {
      Map<String, String> debugInfo = reader.getIndexDebugInfo();
      assertEquals(debugInfo.get("distanceFunction"), "INNER_PRODUCT");

      ImmutableRoaringBitmap result = reader.getDocIds(ipVectors[0], TOP_K, null);
      assertTrue(result.contains(0), "Should find query vector (docId 0) with IP rerank");
      assertEquals(result.getCardinality(), TOP_K);

      // Find exact top-K by inner product and check recall
      int[] exactTopK = findExactTopKByDistance(ipVectors[0], ipVectors, TOP_K,
          IvfPqIndexFormat.DIST_INNER_PRODUCT);
      int hits = countHits(result, exactTopK);
      assertTrue(hits >= 5, "Recall@10 with IP rerank should be >= 50%, got " + hits + "/10");
    }
  }

  private VectorIndexConfig createIvfPqConfigWithDistance(String distanceFunction) {
    Map<String, String> properties = new HashMap<>();
    properties.put("vectorIndexType", "IVF_PQ");
    properties.put("vectorDimension", String.valueOf(DIMENSION));
    properties.put("vectorDistanceFunction", distanceFunction);
    properties.put("nlist", "32");
    properties.put("pqM", "4");
    properties.put("pqNbits", "8");
    properties.put("trainSampleSize", "500");
    properties.put("trainingSeed", "42");
    VectorIndexConfig.VectorDistanceFunction df;
    switch (distanceFunction) {
      case "COSINE":
        df = VectorIndexConfig.VectorDistanceFunction.COSINE;
        break;
      case "INNER_PRODUCT":
        df = VectorIndexConfig.VectorDistanceFunction.INNER_PRODUCT;
        break;
      default:
        df = VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN;
        break;
    }
    return new VectorIndexConfig(false, "IVF_PQ", DIMENSION, 2, df, properties);
  }

  private int[] findExactTopKByDistance(float[] query, float[][] vectors, int k, int distFuncCode) {
    float[] distances = new float[vectors.length];
    int[] indices = new int[vectors.length];
    for (int i = 0; i < vectors.length; i++) {
      distances[i] = VectorDistanceUtil.computeDistance(query, vectors[i], query.length, distFuncCode);
      indices[i] = i;
    }
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

  private VectorIndexConfig createIvfPqConfig() {
    Map<String, String> properties = new HashMap<>();
    properties.put("vectorIndexType", "IVF_PQ");
    properties.put("vectorDimension", String.valueOf(DIMENSION));
    properties.put("vectorDistanceFunction", "EUCLIDEAN");
    properties.put("nlist", "64");
    properties.put("pqM", "4");
    properties.put("pqNbits", "8");
    properties.put("trainSampleSize", "1000");
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

  private int countHits(ImmutableRoaringBitmap result, int[] exactTopK) {
    int hits = 0;
    for (int docId : exactTopK) {
      if (result.contains(docId)) {
        hits++;
      }
    }
    return hits;
  }
}
