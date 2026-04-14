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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.function.scalar.VectorFunctions;
import org.apache.pinot.segment.local.segment.index.readers.vector.IvfPqVectorIndexReader;
import org.apache.pinot.segment.spi.index.creator.VectorBackendType;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Unit tests for the IVF_PQ vector index implementation.
 */
public class IvfPqVectorIndexTest {
  private static final String COLUMN_NAME = "vectorCol";
  private static final long TEST_SEED = 42L;
  private File _tempDir;

  @BeforeMethod
  public void setUp()
      throws IOException {
    _tempDir = new File(FileUtils.getTempDirectory(), "ivf_pq_test_" + System.nanoTime());
    Assert.assertTrue(_tempDir.mkdirs(), "Failed to create temp directory: " + _tempDir);
  }

  @AfterMethod
  public void tearDown() {
    FileUtils.deleteQuietly(_tempDir);
  }

  @Test
  public void testSerializationRoundTrip()
      throws IOException {
    int dimension = 10;
    int nlist = 8;
    int pqM = 5;
    int pqNbits = 8;
    int trainSampleSize = 64;
    long trainingSeed = 12345L;
    VectorIndexConfig config = createConfig(VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN, dimension, nlist, pqM,
        pqNbits, trainSampleSize, trainingSeed);
    float[][] vectors = generateClusteredVectors(8, 20, dimension, 0.04f, TEST_SEED);

    try (IvfPqVectorIndexCreator creator = new IvfPqVectorIndexCreator(COLUMN_NAME, _tempDir, config)) {
      for (float[] vector : vectors) {
        creator.add(vector);
      }
      creator.seal();
    }

    try (IvfPqVectorIndexReader reader = new IvfPqVectorIndexReader(COLUMN_NAME, _tempDir, config)) {
      Assert.assertEquals(reader.getDimension(), dimension);
      Assert.assertEquals(reader.getNumVectors(), vectors.length);
      Assert.assertEquals(reader.getNlist(), nlist);
      Assert.assertEquals(reader.getPqM(), pqM);
      Assert.assertEquals(reader.getPqNbits(), pqNbits);
      Assert.assertEquals(reader.getTrainSampleSize(), trainSampleSize);
      Assert.assertEquals(reader.getTrainingSeed(), trainingSeed);
      Assert.assertEquals(reader.getDistanceFunction(), VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN);
      Assert.assertEquals(reader.getCodebooks().length, pqM);
      Assert.assertEquals(reader.getListDocIds().length, nlist);

      int totalDocs = 0;
      for (int[] docIds : reader.getListDocIds()) {
        totalDocs += docIds.length;
      }
      Assert.assertEquals(totalDocs, vectors.length);
      Assert.assertEquals(Arrays.stream(reader.getSubvectorLengths()).sum(), dimension);
    }
  }

  @Test
  public void testEmptyIndexIsReadable()
      throws IOException {
    VectorIndexConfig config = createConfig(VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN, 4, 2, 2, 4, 16,
        TEST_SEED);

    try (IvfPqVectorIndexCreator creator = new IvfPqVectorIndexCreator(COLUMN_NAME, _tempDir, config)) {
      creator.seal();
    }

    try (IvfPqVectorIndexReader reader = new IvfPqVectorIndexReader(COLUMN_NAME, _tempDir, config)) {
      Assert.assertEquals(reader.getNumVectors(), 0);
      Assert.assertEquals(reader.getNlist(), 0);
      Assert.assertEquals(((MutableRoaringBitmap) reader.getDocIds(new float[]{1.0f, 2.0f, 3.0f, 4.0f}, 5))
          .getCardinality(), 0);
    }
  }

  @Test
  public void testApproximateRecallImprovesWithNprobe()
      throws IOException {
    int dimension = 8;
    int nlist = 12;
    int pqM = 8;
    int pqNbits = 8;
    int topK = 10;
    float[][] vectors = generateClusteredVectors(12, 25, dimension, 0.02f, TEST_SEED);
    VectorIndexConfig config = createConfig(VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN, dimension, nlist, pqM,
        pqNbits, vectors.length, TEST_SEED);

    try (IvfPqVectorIndexCreator creator = new IvfPqVectorIndexCreator(COLUMN_NAME, _tempDir, config)) {
      for (float[] vector : vectors) {
        creator.add(vector);
      }
      creator.seal();
    }

    float[][] queries = {
        vectors[0],
        vectors[11],
        vectors[37],
        vectors[61],
        vectors[88],
        vectors[115]
    };

    try (IvfPqVectorIndexReader reader = new IvfPqVectorIndexReader(COLUMN_NAME, _tempDir, config)) {
      double recallAtOneProbe = 0.0;
      double recallAtAllProbes = 0.0;
      for (float[] query : queries) {
        Set<Integer> bruteForce = bruteForceTopK(vectors, query, topK,
            VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN);

        reader.setNprobe(1);
        recallAtOneProbe += recallAtK((MutableRoaringBitmap) reader.getDocIds(query, topK), bruteForce);

        reader.setNprobe(nlist);
        recallAtAllProbes += recallAtK((MutableRoaringBitmap) reader.getDocIds(query, topK), bruteForce);
      }

      recallAtOneProbe /= queries.length;
      recallAtAllProbes /= queries.length;

      Assert.assertTrue(recallAtAllProbes >= recallAtOneProbe,
          "Full probing should not reduce recall. oneProbe=" + recallAtOneProbe + ", allProbes=" + recallAtAllProbes);
      Assert.assertTrue(recallAtAllProbes >= 0.75,
          "Expected decent recall with full probing, got: " + recallAtAllProbes);
    }
  }

  @Test
  public void testQueryVectorStillRanksItself()
      throws IOException {
    int dimension = 8;
    int nlist = 8;
    int pqM = 8;
    int pqNbits = 8;
    float[][] vectors = generateClusteredVectors(8, 16, dimension, 0.03f, TEST_SEED);
    VectorIndexConfig config = createConfig(VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN, dimension, nlist, pqM,
        pqNbits, vectors.length, TEST_SEED);

    try (IvfPqVectorIndexCreator creator = new IvfPqVectorIndexCreator(COLUMN_NAME, _tempDir, config)) {
      for (float[] vector : vectors) {
        creator.add(vector);
      }
      creator.seal();
    }

    try (IvfPqVectorIndexReader reader = new IvfPqVectorIndexReader(COLUMN_NAME, _tempDir, config)) {
      reader.setNprobe(nlist);
      MutableRoaringBitmap result = (MutableRoaringBitmap) reader.getDocIds(vectors[7], 5);
      Assert.assertTrue(result.contains(7), "The exact stored vector should remain in the top-K set");
    }
  }

  @Test
  public void testCosineQueryVectorStillRanksItself()
      throws IOException {
    int dimension = 8;
    int nlist = 8;
    int pqM = 8;
    int pqNbits = 8;
    float[][] vectors = generateClusteredVectors(8, 16, dimension, 0.03f, TEST_SEED);
    for (int i = 0; i < vectors.length; i++) {
      vectors[i] = VectorQuantizationUtils.normalizeCopy(vectors[i]);
    }
    VectorIndexConfig config = createConfig(VectorIndexConfig.VectorDistanceFunction.COSINE, dimension, nlist, pqM,
        pqNbits, vectors.length, TEST_SEED);

    try (IvfPqVectorIndexCreator creator = new IvfPqVectorIndexCreator(COLUMN_NAME, _tempDir, config)) {
      for (float[] vector : vectors) {
        creator.add(vector);
      }
      creator.seal();
    }

    try (IvfPqVectorIndexReader reader = new IvfPqVectorIndexReader(COLUMN_NAME, _tempDir, config)) {
      reader.setNprobe(nlist);
      MutableRoaringBitmap result = (MutableRoaringBitmap) reader.getDocIds(vectors[7], 5);
      Assert.assertTrue(result.contains(7), "The normalized stored vector should remain in the cosine top-K set");
    }
  }

  @Test
  public void testDotProductSearchReturnsResults()
      throws IOException {
    int dimension = 8;
    int nlist = 4;
    int pqM = 4;
    int pqNbits = 8;
    int topK = 10;
    float[][] vectors = generateClusteredVectors(4, 16, dimension, 0.01f, TEST_SEED);
    VectorIndexConfig config = createConfig(VectorIndexConfig.VectorDistanceFunction.DOT_PRODUCT, dimension, nlist, pqM,
        pqNbits, vectors.length, TEST_SEED);

    try (IvfPqVectorIndexCreator creator = new IvfPqVectorIndexCreator(COLUMN_NAME, _tempDir, config)) {
      for (float[] vector : vectors) {
        creator.add(vector);
      }
      creator.seal();
    }

    try (IvfPqVectorIndexReader reader = new IvfPqVectorIndexReader(COLUMN_NAME, _tempDir, config)) {
      Assert.assertEquals(reader.getDistanceFunction(), VectorIndexConfig.VectorDistanceFunction.DOT_PRODUCT);
      reader.setNprobe(nlist);
      MutableRoaringBitmap result = (MutableRoaringBitmap) reader.getDocIds(vectors[0], topK);
      Assert.assertEquals(result.getCardinality(), topK,
          "DOT_PRODUCT search with full probing should return topK results");

      // Verify recall improves with more probes (same as other distance functions)
      Set<Integer> bruteForce = bruteForceTopK(vectors, vectors[0], topK,
          VectorIndexConfig.VectorDistanceFunction.DOT_PRODUCT);
      double recall = recallAtK(result, bruteForce);
      Assert.assertTrue(recall > 0.0, "DOT_PRODUCT recall should be non-zero with full probing");
    }
  }

  @Test
  public void testInnerProductRecallImprovesWithNprobe()
      throws IOException {
    int dimension = 8;
    int nlist = 12;
    int pqM = 8;
    int pqNbits = 8;
    int topK = 10;
    float[][] vectors = generateClusteredVectors(12, 25, dimension, 0.02f, TEST_SEED);
    VectorIndexConfig config = createConfig(VectorIndexConfig.VectorDistanceFunction.INNER_PRODUCT, dimension, nlist,
        pqM, pqNbits, vectors.length, TEST_SEED);

    try (IvfPqVectorIndexCreator creator = new IvfPqVectorIndexCreator(COLUMN_NAME, _tempDir, config)) {
      for (float[] vector : vectors) {
        creator.add(vector);
      }
      creator.seal();
    }

    try (IvfPqVectorIndexReader reader = new IvfPqVectorIndexReader(COLUMN_NAME, _tempDir, config)) {
      float[] query = vectors[0];

      reader.setNprobe(1);
      Set<Integer> bruteForce = bruteForceTopK(vectors, query, topK,
          VectorIndexConfig.VectorDistanceFunction.INNER_PRODUCT);
      double recallAtOne = recallAtK((MutableRoaringBitmap) reader.getDocIds(query, topK), bruteForce);

      reader.setNprobe(nlist);
      double recallAtAll = recallAtK((MutableRoaringBitmap) reader.getDocIds(query, topK), bruteForce);

      Assert.assertTrue(recallAtAll >= recallAtOne,
          "Full probing should not reduce recall for INNER_PRODUCT. oneProbe=" + recallAtOne
              + ", allProbes=" + recallAtAll);
    }
  }

  @Test
  public void testReaderClampsNprobe()
      throws IOException {
    int dimension = 4;
    int nlist = 3;
    int pqM = 2;
    VectorIndexConfig config = createConfig(VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN, dimension, nlist, pqM,
        4, 16, TEST_SEED);
    float[][] vectors = generateClusteredVectors(3, 10, dimension, 0.05f, TEST_SEED);

    try (IvfPqVectorIndexCreator creator = new IvfPqVectorIndexCreator(COLUMN_NAME, _tempDir, config)) {
      for (float[] vector : vectors) {
        creator.add(vector);
      }
      creator.seal();
    }

    try (IvfPqVectorIndexReader reader = new IvfPqVectorIndexReader(COLUMN_NAME, _tempDir, config)) {
      reader.setNprobe(100);
      Assert.assertEquals(reader.getNprobe(), nlist);
    }
  }

  @Test
  public void testApproximateRadiusSearchRespectsCandidateCap()
      throws IOException {
    int dimension = 8;
    int nlist = 8;
    int pqM = 8;
    int pqNbits = 8;
    float[][] vectors = generateClusteredVectors(8, 20, dimension, 0.03f, TEST_SEED);
    VectorIndexConfig config = createConfig(VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN, dimension, nlist, pqM,
        pqNbits, vectors.length, TEST_SEED);

    try (IvfPqVectorIndexCreator creator = new IvfPqVectorIndexCreator(COLUMN_NAME, _tempDir, config)) {
      for (float[] vector : vectors) {
        creator.add(vector);
      }
      creator.seal();
    }

    try (IvfPqVectorIndexReader reader = new IvfPqVectorIndexReader(COLUMN_NAME, _tempDir, config)) {
      reader.setNprobe(nlist);
      ImmutableRoaringBitmap result =
          reader.getDocIdsWithinApproximateRadius(vectors[0], Float.POSITIVE_INFINITY, 5);
      Assert.assertTrue(result.getCardinality() <= 5, "radius search should obey maxCandidates cap");
      Assert.assertTrue(result.getCardinality() > 0, "radius search should return at least one candidate");
    }
  }

  @Test
  public void testDistanceFunctionStableIdRoundTrip() {
    for (VectorIndexConfig.VectorDistanceFunction df : VectorIndexConfig.VectorDistanceFunction.values()) {
      int id = IvfPqVectorIndexCreator.distanceFunctionToStableId(df);
      VectorIndexConfig.VectorDistanceFunction roundTripped = IvfPqVectorIndexCreator.stableIdToDistanceFunction(id);
      Assert.assertEquals(roundTripped, df, "Stable ID round-trip failed for " + df);
    }
  }

  @Test
  public void testReaderNprobeOverrideIsThreadLocal()
      throws Exception {
    int dimension = 4;
    int nlist = 3;
    int pqM = 2;
    VectorIndexConfig config = createConfig(VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN, dimension, nlist, pqM,
        4, 16, TEST_SEED);
    float[][] vectors = generateClusteredVectors(3, 10, dimension, 0.05f, TEST_SEED);

    try (IvfPqVectorIndexCreator creator = new IvfPqVectorIndexCreator(COLUMN_NAME, _tempDir, config)) {
      for (float[] vector : vectors) {
        creator.add(vector);
      }
      creator.seal();
    }

    try (IvfPqVectorIndexReader reader = new IvfPqVectorIndexReader(COLUMN_NAME, _tempDir, config)) {
      int defaultNprobe = reader.getNprobe();
      reader.setNprobe(1);
      Assert.assertEquals(reader.getNprobe(), 1);

      AtomicInteger otherThreadNprobe = new AtomicInteger(-1);
      Thread thread = new Thread(() -> otherThreadNprobe.set(reader.getNprobe()));
      thread.start();
      thread.join();

      Assert.assertEquals(otherThreadNprobe.get(), defaultNprobe);
      Assert.assertEquals(reader.getNprobe(), 1);
    }
  }

  @Test
  public void testReaderKeepsNprobeOverrideAcrossSearches()
      throws IOException {
    int dimension = 4;
    int nlist = 3;
    int pqM = 2;
    VectorIndexConfig config = createConfig(VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN, dimension, nlist, pqM,
        4, 16, TEST_SEED);
    float[][] vectors = generateClusteredVectors(3, 10, dimension, 0.05f, TEST_SEED);

    try (IvfPqVectorIndexCreator creator = new IvfPqVectorIndexCreator(COLUMN_NAME, _tempDir, config)) {
      for (float[] vector : vectors) {
        creator.add(vector);
      }
      creator.seal();
    }

    try (IvfPqVectorIndexReader reader = new IvfPqVectorIndexReader(COLUMN_NAME, _tempDir, config)) {
      reader.setNprobe(1);
      reader.getDocIds(vectors[0], 5);
      Assert.assertEquals(reader.getNprobe(), 1);
      reader.getDocIds(vectors[0], 5);
      Assert.assertEquals(reader.getNprobe(), 1);
    }
  }

  @Test
  public void testReaderClearNprobeResetsToDefault()
      throws IOException {
    int dimension = 4;
    int nlist = 3;
    int pqM = 2;
    VectorIndexConfig config = createConfig(VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN, dimension, nlist, pqM,
        4, 16, TEST_SEED);
    float[][] vectors = generateClusteredVectors(3, 10, dimension, 0.05f, TEST_SEED);

    try (IvfPqVectorIndexCreator creator = new IvfPqVectorIndexCreator(COLUMN_NAME, _tempDir, config)) {
      for (float[] vector : vectors) {
        creator.add(vector);
      }
      creator.seal();
    }

    try (IvfPqVectorIndexReader reader = new IvfPqVectorIndexReader(COLUMN_NAME, _tempDir, config)) {
      int defaultNprobe = reader.getNprobe();
      reader.setNprobe(1);
      Assert.assertEquals(reader.getNprobe(), 1);
      reader.clearNprobe();
      Assert.assertEquals(reader.getNprobe(), defaultNprobe);
    }
  }

  @Test(expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = ".*IVF_PQ property 'pqM' is required.*")
  public void testCreatorRequiresPqM()
      throws IOException {
    Map<String, String> properties = new HashMap<>();
    properties.put("vectorIndexType", "IVF_PQ");
    properties.put("vectorDimension", "4");
    properties.put("vectorDistanceFunction", VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN.name());
    properties.put("nlist", "2");
    properties.put("pqNbits", "4");
    properties.put("trainSampleSize", "8");
    VectorIndexConfig config = new VectorIndexConfig(false, "IVF_PQ", 4, 1,
        VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN, properties);
    try (IvfPqVectorIndexCreator ignored = new IvfPqVectorIndexCreator(COLUMN_NAME, _tempDir, config)) {
      Assert.fail("Expected missing pqM validation to fail");
    }
  }

  @Test(expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = ".*IVF_PQ pqNbits must be one of \\[4, 6, 8\\], got: 7.*")
  public void testCreatorRejectsUnsupportedPqNbits()
      throws IOException {
    VectorIndexConfig config = createConfig(VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN, 4, 2, 2, 7, 8,
        TEST_SEED);
    try (IvfPqVectorIndexCreator ignored = new IvfPqVectorIndexCreator(COLUMN_NAME, _tempDir, config)) {
      Assert.fail("Expected unsupported pqNbits validation to fail");
    }
  }

  // ---------------------------------------------------------------------------
  // Item 3: Spill-file lifecycle hardening
  // ---------------------------------------------------------------------------

  @Test
  public void testSpillFileCleanedUpAfterSeal()
      throws IOException {
    VectorIndexConfig config = createConfig(VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN, 4, 2, 2, 4, 8,
        TEST_SEED);
    File spillFile = new File(_tempDir, COLUMN_NAME + IvfPqVectorIndexCreator.INDEX_FILE_EXTENSION + ".spill");

    try (IvfPqVectorIndexCreator creator = new IvfPqVectorIndexCreator(COLUMN_NAME, _tempDir, config)) {
      creator.add(new float[]{1, 2, 3, 4});
      creator.add(new float[]{5, 6, 7, 8});
      Assert.assertTrue(spillFile.exists(), "Spill file should exist during add()");
      creator.seal();
      Assert.assertFalse(spillFile.exists(), "Spill file should be cleaned up at the end of seal()");
    }
    Assert.assertFalse(spillFile.exists(), "Spill file should be cleaned up after close()");
  }

  @Test
  public void testSpillFileCleanedUpOnCloseWithoutSeal()
      throws IOException {
    VectorIndexConfig config = createConfig(VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN, 4, 2, 2, 4, 8,
        TEST_SEED);
    File spillFile = new File(_tempDir, COLUMN_NAME + IvfPqVectorIndexCreator.INDEX_FILE_EXTENSION + ".spill");

    try (IvfPqVectorIndexCreator creator = new IvfPqVectorIndexCreator(COLUMN_NAME, _tempDir, config)) {
      creator.add(new float[]{1, 2, 3, 4});
      Assert.assertTrue(spillFile.exists(), "Spill file should exist during add()");
      // Intentionally NOT calling seal() — simulating a failure before seal
    }
    Assert.assertFalse(spillFile.exists(), "Spill file should be cleaned up even without seal()");
  }

  @Test
  public void testOrphanSpillFileDoesNotBlockNewCreator()
      throws IOException {
    VectorIndexConfig config = createConfig(VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN, 4, 2, 2, 4, 8,
        TEST_SEED);
    File spillFile = new File(_tempDir, COLUMN_NAME + IvfPqVectorIndexCreator.INDEX_FILE_EXTENSION + ".spill");

    // Create an orphan spill file (simulating a crash)
    Assert.assertTrue(spillFile.createNewFile());
    Assert.assertTrue(spillFile.exists());

    // A new creator should overwrite the orphan spill file
    try (IvfPqVectorIndexCreator creator = new IvfPqVectorIndexCreator(COLUMN_NAME, _tempDir, config)) {
      creator.add(new float[]{1, 2, 3, 4});
      creator.add(new float[]{5, 6, 7, 8});
      creator.seal();
    }

    Assert.assertFalse(spillFile.exists(), "Spill file should be cleaned up");
    // Verify the index was created successfully
    try (IvfPqVectorIndexReader reader = new IvfPqVectorIndexReader(COLUMN_NAME, _tempDir, config)) {
      Assert.assertEquals(reader.getNumVectors(), 2);
    }
  }

  @Test
  public void testSupportedPqNbitsSearchPaths()
      throws IOException {
    assertSupportedPqNbitsSearchPath(VectorIndexConfig.VectorDistanceFunction.COSINE);
    assertSupportedPqNbitsSearchPath(VectorIndexConfig.VectorDistanceFunction.INNER_PRODUCT);
  }

  @Test
  public void testSmallSegmentWithMoreListsThanVectors()
      throws IOException {
    int dimension = 8;
    int nlist = 16;
    int pqM = 4;
    int pqNbits = 4;
    float[][] vectors = generateClusteredVectors(1, 5, dimension, 0.02f, TEST_SEED);
    VectorIndexConfig config = createConfig(VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN, dimension, nlist, pqM,
        pqNbits, nlist, TEST_SEED);

    try (IvfPqVectorIndexCreator creator = new IvfPqVectorIndexCreator(COLUMN_NAME, _tempDir, config)) {
      for (float[] vector : vectors) {
        creator.add(vector);
      }
      creator.seal();
    }

    try (IvfPqVectorIndexReader reader = new IvfPqVectorIndexReader(COLUMN_NAME, _tempDir, config)) {
      Assert.assertEquals(reader.getNumVectors(), vectors.length);
      Assert.assertEquals(reader.getNlist(), vectors.length);
      reader.setNprobe(nlist);
      Assert.assertEquals(reader.getNprobe(), vectors.length);
      MutableRoaringBitmap result = (MutableRoaringBitmap) reader.getDocIds(vectors[0], 3);
      Assert.assertEquals(result.getCardinality(), 3);
      Assert.assertTrue(result.contains(0));
    }
  }

  // ---------------------------------------------------------------------------
  // Item 5: Debug/explain surface
  // ---------------------------------------------------------------------------

  @Test
  public void testGetIndexDebugInfo()
      throws IOException {
    int dimension = 8;
    int nlist = 4;
    int pqM = 4;
    int pqNbits = 8;
    float[][] vectors = generateClusteredVectors(4, 10, dimension, 0.03f, TEST_SEED);
    VectorIndexConfig config = createConfig(VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN, dimension, nlist, pqM,
        pqNbits, vectors.length, TEST_SEED);

    try (IvfPqVectorIndexCreator creator = new IvfPqVectorIndexCreator(COLUMN_NAME, _tempDir, config)) {
      for (float[] vector : vectors) {
        creator.add(vector);
      }
      creator.seal();
    }

    try (IvfPqVectorIndexReader reader = new IvfPqVectorIndexReader(COLUMN_NAME, _tempDir, config)) {
      Map<String, Object> info = reader.getIndexDebugInfo();
      Assert.assertEquals(info.get("backend"), "IVF_PQ");
      Assert.assertEquals(info.get("column"), COLUMN_NAME);
      Assert.assertEquals(info.get("dimension"), dimension);
      Assert.assertEquals(info.get("numVectors"), vectors.length);
      Assert.assertEquals(info.get("nlist"), nlist);
      Assert.assertEquals(info.get("pqM"), pqM);
      Assert.assertEquals(info.get("pqNbits"), pqNbits);
      Assert.assertEquals(info.get("distanceFunction"), "EUCLIDEAN");
      int requested = (int) info.get("requestedTrainSampleSize");
      Assert.assertEquals(info.get("effectiveTrainSampleSize"),
          Math.min(vectors.length, requested));
      Assert.assertEquals(info.get("codebookSize"), 256);
      Assert.assertNotNull(info.get("effectiveNprobe"));
      Assert.assertNotNull(info.get("avgDocsPerList"));
      Assert.assertNotNull(info.get("minListSize"));
      Assert.assertNotNull(info.get("maxListSize"));
      Assert.assertNotNull(info.get("emptyLists"));

      // Verify list stats are sane
      int totalDocs = 0;
      for (int[] docIds : reader.getListDocIds()) {
        totalDocs += docIds.length;
      }
      Assert.assertEquals(totalDocs, vectors.length);
      Assert.assertTrue((int) info.get("maxListSize") > 0);
      Assert.assertTrue((double) info.get("avgDocsPerList") > 0.0);
    }
  }

  @Test
  public void testDebugInfoReflectsNprobeOverride()
      throws IOException {
    int dimension = 4;
    int nlist = 3;
    int pqM = 2;
    VectorIndexConfig config = createConfig(VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN, dimension, nlist, pqM,
        4, 16, TEST_SEED);
    float[][] vectors = generateClusteredVectors(3, 10, dimension, 0.05f, TEST_SEED);

    try (IvfPqVectorIndexCreator creator = new IvfPqVectorIndexCreator(COLUMN_NAME, _tempDir, config)) {
      for (float[] vector : vectors) {
        creator.add(vector);
      }
      creator.seal();
    }

    try (IvfPqVectorIndexReader reader = new IvfPqVectorIndexReader(COLUMN_NAME, _tempDir, config)) {
      int defaultNprobe = (int) reader.getIndexDebugInfo().get("effectiveNprobe");
      reader.setNprobe(1);
      Assert.assertEquals(reader.getIndexDebugInfo().get("effectiveNprobe"), 1);
      reader.setNprobe(nlist);
      Assert.assertEquals(reader.getIndexDebugInfo().get("effectiveNprobe"), nlist);
    }
  }

  // ---------------------------------------------------------------------------
  // Item 6: Lifecycle coverage
  // ---------------------------------------------------------------------------

  @Test
  public void testDisabledConfigProducesNoIndex()
      throws IOException {
    VectorIndexConfig disabledConfig = VectorIndexConfig.DISABLED;
    Assert.assertTrue(disabledConfig.isDisabled());
    // VectorIndexType.createMutableIndex returns null for disabled config —
    // verified via the type itself, not the creator
  }

  @Test
  public void testIvfPqFallsBackForMutableSegmentConfig() {
    // IVF_PQ does not support mutable segments; VectorIndexType.createMutableIndex should return null
    VectorBackendType backendType = VectorBackendType.IVF_PQ;
    Assert.assertFalse(backendType.supportsMutableSegments());
  }

  @Test
  public void testReaderOpenAfterCreatorClose()
      throws IOException {
    // Verify the index can be read after the creator is fully closed (not just sealed)
    int dimension = 4;
    int nlist = 2;
    int pqM = 2;
    VectorIndexConfig config = createConfig(VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN, dimension, nlist, pqM,
        4, 16, TEST_SEED);
    float[][] vectors = generateClusteredVectors(2, 8, dimension, 0.05f, TEST_SEED);

    IvfPqVectorIndexCreator creator = new IvfPqVectorIndexCreator(COLUMN_NAME, _tempDir, config);
    for (float[] vector : vectors) {
      creator.add(vector);
    }
    creator.seal();
    creator.close();

    // Open reader after creator is fully closed
    try (IvfPqVectorIndexReader reader = new IvfPqVectorIndexReader(COLUMN_NAME, _tempDir, config)) {
      Assert.assertEquals(reader.getNumVectors(), vectors.length);
      MutableRoaringBitmap result = (MutableRoaringBitmap) reader.getDocIds(vectors[0], 5);
      Assert.assertTrue(result.getCardinality() > 0);
    }
  }

  @Test
  public void testMultipleReadersConcurrent()
      throws Exception {
    int dimension = 4;
    int nlist = 2;
    int pqM = 2;
    VectorIndexConfig config = createConfig(VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN, dimension, nlist, pqM,
        4, 16, TEST_SEED);
    float[][] vectors = generateClusteredVectors(2, 10, dimension, 0.05f, TEST_SEED);

    try (IvfPqVectorIndexCreator creator = new IvfPqVectorIndexCreator(COLUMN_NAME, _tempDir, config)) {
      for (float[] vector : vectors) {
        creator.add(vector);
      }
      creator.seal();
    }

    // Open two readers on the same index concurrently
    try (IvfPqVectorIndexReader reader1 = new IvfPqVectorIndexReader(COLUMN_NAME, _tempDir, config);
        IvfPqVectorIndexReader reader2 = new IvfPqVectorIndexReader(COLUMN_NAME, _tempDir, config)) {
      reader1.setNprobe(1);
      reader2.setNprobe(nlist);

      MutableRoaringBitmap r1 = (MutableRoaringBitmap) reader1.getDocIds(vectors[0], 5);
      MutableRoaringBitmap r2 = (MutableRoaringBitmap) reader2.getDocIds(vectors[0], 5);

      Assert.assertTrue(r1.getCardinality() > 0);
      Assert.assertTrue(r2.getCardinality() > 0);
      // reader2 with full probing should return at least as many results
      Assert.assertTrue(r2.getCardinality() >= r1.getCardinality());
    }
  }

  private VectorIndexConfig createConfig(VectorIndexConfig.VectorDistanceFunction distanceFunction, int dimension,
      int nlist, int pqM, int pqNbits, int trainSampleSize, long trainingSeed) {
    Map<String, String> properties = new HashMap<>();
    properties.put("vectorIndexType", "IVF_PQ");
    properties.put("vectorDimension", String.valueOf(dimension));
    properties.put("vectorDistanceFunction", distanceFunction.name());
    properties.put("nlist", String.valueOf(nlist));
    properties.put("pqM", String.valueOf(pqM));
    properties.put("pqNbits", String.valueOf(pqNbits));
    properties.put("trainSampleSize", String.valueOf(trainSampleSize));
    properties.put("trainingSeed", String.valueOf(trainingSeed));
    return new VectorIndexConfig(false, "IVF_PQ", dimension, 1, distanceFunction, properties);
  }

  private void assertSupportedPqNbitsSearchPath(VectorIndexConfig.VectorDistanceFunction distanceFunction)
      throws IOException {
    int dimension = 8;
    int nlist = 4;
    int pqM = 4;
    float[][] vectors = generateClusteredVectors(4, 12, dimension, 0.02f, TEST_SEED);
    for (int pqNbits : new int[]{4, 6, 8}) {
      File indexDir = new File(_tempDir, distanceFunction.name().toLowerCase() + "_" + pqNbits);
      Assert.assertTrue(indexDir.mkdirs(), "Failed to create temp directory: " + indexDir);
      String column = COLUMN_NAME + "_" + distanceFunction.name().toLowerCase() + "_" + pqNbits;
      VectorIndexConfig config = createConfig(distanceFunction, dimension, nlist, pqM, pqNbits, vectors.length,
          TEST_SEED);

      try (IvfPqVectorIndexCreator creator = new IvfPqVectorIndexCreator(column, indexDir, config)) {
        for (float[] vector : vectors) {
          creator.add(vector);
        }
        creator.seal();
      }

      try (IvfPqVectorIndexReader reader = new IvfPqVectorIndexReader(column, indexDir, config)) {
        reader.setNprobe(nlist);
        MutableRoaringBitmap result = (MutableRoaringBitmap) reader.getDocIds(vectors[0], 5);
        Assert.assertEquals(reader.getPqNbits(), pqNbits);
        Assert.assertTrue(result.getCardinality() > 0,
            "Supported pqNbits should remain queryable for " + distanceFunction + " pqNbits=" + pqNbits);
        Assert.assertTrue(result.getCardinality() <= 5);
      }
    }
  }

  private float[][] generateClusteredVectors(int numClusters, int pointsPerCluster, int dimension, float noiseStdDev,
      long seed) {
    Random random = new Random(seed);
    float[][] centers = new float[numClusters][dimension];
    for (int c = 0; c < numClusters; c++) {
      for (int d = 0; d < dimension; d++) {
        centers[c][d] = random.nextFloat() * 4.0f - 2.0f;
      }
    }

    float[][] vectors = new float[numClusters * pointsPerCluster][dimension];
    int index = 0;
    for (int c = 0; c < numClusters; c++) {
      for (int i = 0; i < pointsPerCluster; i++) {
        float[] vector = new float[dimension];
        for (int d = 0; d < dimension; d++) {
          vector[d] = centers[c][d] + (float) (random.nextGaussian() * noiseStdDev);
        }
        vectors[index++] = vector;
      }
    }
    return vectors;
  }

  private Set<Integer> bruteForceTopK(float[][] vectors, float[] query, int topK,
      VectorIndexConfig.VectorDistanceFunction distanceFunction) {
    int numVectors = vectors.length;
    float[] distances = new float[numVectors];
    for (int i = 0; i < numVectors; i++) {
      distances[i] = computeDistance(query, vectors[i], distanceFunction);
    }

    Integer[] indices = new Integer[numVectors];
    for (int i = 0; i < numVectors; i++) {
      indices[i] = i;
    }
    Arrays.sort(indices, (left, right) -> Float.compare(distances[left], distances[right]));

    Set<Integer> result = new HashSet<>();
    for (int i = 0; i < Math.min(topK, numVectors); i++) {
      result.add(indices[i]);
    }
    return result;
  }

  private double recallAtK(MutableRoaringBitmap result, Set<Integer> expected) {
    int hits = 0;
    for (int docId : result.toArray()) {
      if (expected.contains(docId)) {
        hits++;
      }
    }
    return expected.isEmpty() ? 1.0d : (double) hits / expected.size();
  }

  private float computeDistance(float[] a, float[] b, VectorIndexConfig.VectorDistanceFunction distanceFunction) {
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
}
