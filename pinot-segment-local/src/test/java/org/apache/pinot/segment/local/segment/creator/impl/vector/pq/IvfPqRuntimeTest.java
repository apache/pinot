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
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Tests for IVF_PQ runtime: NprobeAware, debug info, distance functions, index compactness.
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

    Random random = new Random(42);
    _vectors = new float[NUM_VECTORS][DIMENSION];
    for (int i = 0; i < NUM_VECTORS; i++) {
      for (int d = 0; d < DIMENSION; d++) {
        _vectors[i][d] = random.nextFloat();
      }
    }

    VectorIndexConfig config = createIvfPqConfig("EUCLIDEAN",
        VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN);
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
  public void testNprobeViaSetNprobe()
      throws IOException {
    try (IvfPqVectorIndexReader reader = new IvfPqVectorIndexReader("embedding", _tempDir, NUM_VECTORS, 4)) {
      // Default nprobe=4
      MutableRoaringBitmap lowNprobe = reader.getDocIds(_vectors[0], TOP_K);
      assertEquals(lowNprobe.getCardinality(), TOP_K);

      // Increase nprobe via NprobeAware interface
      reader.setNprobe(32);
      MutableRoaringBitmap highNprobe = reader.getDocIds(_vectors[0], TOP_K);
      assertEquals(highNprobe.getCardinality(), TOP_K);

      // Both should find the query vector itself
      assertTrue(lowNprobe.contains(0), "Low nprobe should find docId 0");
      assertTrue(highNprobe.contains(0), "High nprobe should find docId 0");
    }
  }

  @Test
  public void testHigherNprobeImprovesRecall()
      throws IOException {
    float[] query = _vectors[50];
    int[] exactTopK = findExactTopK(query, _vectors, TOP_K, IvfPqIndexFormat.DIST_EUCLIDEAN);

    try (IvfPqVectorIndexReader reader = new IvfPqVectorIndexReader("embedding", _tempDir, NUM_VECTORS, 1)) {
      // Very low nprobe=1
      reader.setNprobe(1);
      MutableRoaringBitmap lowResult = reader.getDocIds(query, TOP_K);

      // High nprobe=32
      reader.setNprobe(32);
      MutableRoaringBitmap highResult = reader.getDocIds(query, TOP_K);

      int lowHits = countHits(lowResult, exactTopK);
      int highHits = countHits(highResult, exactTopK);

      assertTrue(highHits >= lowHits,
          "Higher nprobe should have equal or better recall: low=" + lowHits + ", high=" + highHits);
    }
  }

  @Test
  public void testCompactIndexSize()
      throws IOException {
    // Index should NOT contain original vectors — only header + centroids + codebooks + docIds + PQ codes
    File indexFile = new File(_tempDir, "embedding" + IvfPqVectorIndexCreator.FILE_EXTENSION);
    long fileSize = indexFile.length();

    // Raw vectors would be: 1000 * 16 * 4 = 64,000 bytes
    // Compact index should be much smaller than raw vectors + overhead
    long rawVectorSize = (long) NUM_VECTORS * DIMENSION * Float.BYTES;
    assertTrue(fileSize < rawVectorSize,
        "Index (" + fileSize + " bytes) should be smaller than raw vectors (" + rawVectorSize + " bytes)");
  }

  @Test
  public void testCosineDistance()
      throws IOException {
    File cosineDir = new File(_tempDir, "cosine");
    cosineDir.mkdirs();

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

    VectorIndexConfig config = createIvfPqConfig("COSINE", VectorIndexConfig.VectorDistanceFunction.COSINE);
    try (IvfPqVectorIndexCreator creator = new IvfPqVectorIndexCreator("emb", cosineDir, config)) {
      for (float[] v : cosineVectors) {
        creator.add(v);
      }
      creator.seal();
    }

    try (IvfPqVectorIndexReader reader = new IvfPqVectorIndexReader("emb", cosineDir, n, 16)) {
      Map<String, String> debugInfo = reader.getIndexDebugInfo();
      assertEquals(debugInfo.get("distanceFunction"), "COSINE");

      MutableRoaringBitmap result = reader.getDocIds(cosineVectors[0], TOP_K);
      assertTrue(result.contains(0), "Should find query vector with cosine distance");
      assertEquals(result.getCardinality(), TOP_K);
    }
  }

  @Test
  public void testInnerProductDistance()
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

    VectorIndexConfig config = createIvfPqConfig("INNER_PRODUCT",
        VectorIndexConfig.VectorDistanceFunction.INNER_PRODUCT);
    try (IvfPqVectorIndexCreator creator = new IvfPqVectorIndexCreator("emb", ipDir, config)) {
      for (float[] v : ipVectors) {
        creator.add(v);
      }
      creator.seal();
    }

    try (IvfPqVectorIndexReader reader = new IvfPqVectorIndexReader("emb", ipDir, n, 16)) {
      Map<String, String> debugInfo = reader.getIndexDebugInfo();
      assertEquals(debugInfo.get("distanceFunction"), "INNER_PRODUCT");

      MutableRoaringBitmap result = reader.getDocIds(ipVectors[0], TOP_K);
      assertTrue(result.contains(0), "Should find query vector with IP distance");
      assertEquals(result.getCardinality(), TOP_K);
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
    }
  }

  private VectorIndexConfig createIvfPqConfig(String distFuncStr,
      VectorIndexConfig.VectorDistanceFunction distFunc) {
    Map<String, String> properties = new HashMap<>();
    properties.put("vectorIndexType", "IVF_PQ");
    properties.put("vectorDimension", String.valueOf(DIMENSION));
    properties.put("vectorDistanceFunction", distFuncStr);
    properties.put("nlist", "64");
    properties.put("pqM", "4");
    properties.put("pqNbits", "8");
    properties.put("trainSampleSize", "1000");
    properties.put("trainingSeed", "42");
    return new VectorIndexConfig(false, "IVF_PQ", DIMENSION, 2, distFunc, properties);
  }

  private int[] findExactTopK(float[] query, float[][] vectors, int k, int distFuncCode) {
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

  private int countHits(MutableRoaringBitmap result, int[] exactTopK) {
    int hits = 0;
    for (int docId : exactTopK) {
      if (result.contains(docId)) {
        hits++;
      }
    }
    return hits;
  }
}
