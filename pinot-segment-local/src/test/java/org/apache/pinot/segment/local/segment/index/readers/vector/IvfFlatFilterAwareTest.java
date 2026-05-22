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
package org.apache.pinot.segment.local.segment.index.readers.vector;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.index.vector.IvfFlatVectorIndexCreator;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Tests for the filter-aware (FILTER_THEN_ANN) functionality of {@link IvfFlatVectorIndexReader}.
 *
 * <p>Verifies that pre-filter ANN search correctly restricts results to the filtered document set
 * and returns correct top-K results across various filter selectivities.</p>
 */
public class IvfFlatFilterAwareTest {
  private static final String COLUMN_NAME = "vectorCol";
  private static final long TEST_SEED = 42L;
  private File _tempDir;

  @BeforeMethod
  public void setUp()
      throws IOException {
    _tempDir = new File(FileUtils.getTempDirectory(), "ivf_flat_filter_test_" + System.nanoTime());
    Assert.assertTrue(_tempDir.mkdirs(), "Failed to create temp directory: " + _tempDir);
  }

  @AfterMethod
  public void tearDown() {
    FileUtils.deleteQuietly(_tempDir);
  }

  @Test
  public void testPreFilterReturnsOnlyFilteredDocs()
      throws Exception {
    // Create a simple index with known vectors
    int numVectors = 50;
    int dimension = 4;
    int nlist = 4;
    float[][] vectors = generateVectors(numVectors, dimension, new Random(TEST_SEED));

    createIndex(vectors, dimension, nlist, VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN);

    VectorIndexConfig config = createConfig(dimension, nlist, VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN);
    try (IvfFlatVectorIndexReader reader = new IvfFlatVectorIndexReader(COLUMN_NAME, _tempDir, config)) {
      // Create a pre-filter that only includes even-numbered docs
      MutableRoaringBitmap preFilter = new MutableRoaringBitmap();
      for (int i = 0; i < numVectors; i += 2) {
        preFilter.add(i);
      }

      float[] queryVector = vectors[0]; // Search for the first vector
      ImmutableRoaringBitmap result = reader.getDocIds(queryVector, 5, preFilter);

      Assert.assertTrue(result.getCardinality() <= 5, "Should return at most 5 results");
      Assert.assertTrue(result.getCardinality() > 0, "Should return at least 1 result");

      // All returned doc IDs must be in the pre-filter
      org.roaringbitmap.IntIterator it = result.getIntIterator();
      while (it.hasNext()) {
        int docId = it.next();
        Assert.assertTrue(preFilter.contains(docId),
            "Returned doc ID " + docId + " is not in the pre-filter bitmap");
      }

      // Doc 0 should definitely be in the results since we searched for its own vector
      Assert.assertTrue(result.contains(0), "Doc 0 should be in results when searching for its own vector");
    }
  }

  @Test
  public void testPreFilterWithEmptyBitmapReturnsEmpty()
      throws Exception {
    int numVectors = 20;
    int dimension = 4;
    int nlist = 2;
    float[][] vectors = generateVectors(numVectors, dimension, new Random(TEST_SEED));

    createIndex(vectors, dimension, nlist, VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN);

    VectorIndexConfig config = createConfig(dimension, nlist, VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN);
    try (IvfFlatVectorIndexReader reader = new IvfFlatVectorIndexReader(COLUMN_NAME, _tempDir, config)) {
      MutableRoaringBitmap emptyFilter = new MutableRoaringBitmap();
      float[] queryVector = vectors[0];
      ImmutableRoaringBitmap result = reader.getDocIds(queryVector, 5, emptyFilter);
      Assert.assertEquals(result.getCardinality(), 0, "Empty pre-filter should return empty results");
    }
  }

  @Test
  public void testPreFilterWithSingleDocReturnsAtMostOne()
      throws Exception {
    int numVectors = 20;
    int dimension = 4;
    int nlist = 2;
    float[][] vectors = generateVectors(numVectors, dimension, new Random(TEST_SEED));

    createIndex(vectors, dimension, nlist, VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN);

    VectorIndexConfig config = createConfig(dimension, nlist, VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN);
    try (IvfFlatVectorIndexReader reader = new IvfFlatVectorIndexReader(COLUMN_NAME, _tempDir, config)) {
      MutableRoaringBitmap singleDocFilter = new MutableRoaringBitmap();
      singleDocFilter.add(5);

      float[] queryVector = vectors[5];
      ImmutableRoaringBitmap result = reader.getDocIds(queryVector, 10, singleDocFilter);

      // Even though topK=10, only 1 doc passes the filter that is in the probed centroids
      Assert.assertTrue(result.getCardinality() <= 1,
          "With single-doc pre-filter, should return at most 1 result");

      if (result.getCardinality() == 1) {
        Assert.assertTrue(result.contains(5), "If a result is returned, it must be doc 5");
      }
    }
  }

  @Test
  public void testHighSelectivityFilter()
      throws Exception {
    // Test with a filter that includes almost all docs (high selectivity = many docs pass)
    int numVectors = 30;
    int dimension = 4;
    int nlist = 3;
    float[][] vectors = generateVectors(numVectors, dimension, new Random(TEST_SEED));

    createIndex(vectors, dimension, nlist, VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN);

    VectorIndexConfig config = createConfig(dimension, nlist, VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN);
    try (IvfFlatVectorIndexReader reader = new IvfFlatVectorIndexReader(COLUMN_NAME, _tempDir, config)) {
      // Include all docs except doc 0
      MutableRoaringBitmap highSelectFilter = new MutableRoaringBitmap();
      for (int i = 1; i < numVectors; i++) {
        highSelectFilter.add(i);
      }

      float[] queryVector = vectors[0];
      int topK = 5;

      // With high selectivity, results should be similar to unfiltered but without doc 0
      ImmutableRoaringBitmap filteredResult = reader.getDocIds(queryVector, topK, highSelectFilter);
      Assert.assertFalse(filteredResult.contains(0),
          "Doc 0 should not appear in results when excluded by filter");
      Assert.assertTrue(filteredResult.getCardinality() <= topK);
      Assert.assertTrue(filteredResult.getCardinality() > 0);
    }
  }

  @Test
  public void testLowSelectivityFilter()
      throws Exception {
    // Test with a very selective filter (few docs pass)
    int numVectors = 100;
    int dimension = 4;
    int nlist = 4;
    float[][] vectors = generateVectors(numVectors, dimension, new Random(TEST_SEED));

    createIndex(vectors, dimension, nlist, VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN);

    VectorIndexConfig config = createConfig(dimension, nlist, VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN);
    try (IvfFlatVectorIndexReader reader = new IvfFlatVectorIndexReader(COLUMN_NAME, _tempDir, config)) {
      reader.setNprobe(nlist); // Probe all centroids to ensure we find filtered docs

      // Only include 3 docs
      MutableRoaringBitmap lowSelectFilter = new MutableRoaringBitmap();
      lowSelectFilter.add(10);
      lowSelectFilter.add(50);
      lowSelectFilter.add(90);

      float[] queryVector = vectors[50];
      ImmutableRoaringBitmap result = reader.getDocIds(queryVector, 5, lowSelectFilter);

      // All returned results must be in the filter
      org.roaringbitmap.IntIterator it = result.getIntIterator();
      while (it.hasNext()) {
        int docId = it.next();
        Assert.assertTrue(lowSelectFilter.contains(docId),
            "Returned doc ID " + docId + " should be in the pre-filter");
      }
    }
  }

  @Test
  public void testFilteredResultsMatchUnfilteredForFullFilter()
      throws Exception {
    // When the filter includes all docs, filtered search should return same results as unfiltered
    int numVectors = 30;
    int dimension = 4;
    int nlist = 3;
    float[][] vectors = generateVectors(numVectors, dimension, new Random(TEST_SEED));

    createIndex(vectors, dimension, nlist, VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN);

    VectorIndexConfig config = createConfig(dimension, nlist, VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN);
    try (IvfFlatVectorIndexReader reader = new IvfFlatVectorIndexReader(COLUMN_NAME, _tempDir, config)) {
      MutableRoaringBitmap fullFilter = new MutableRoaringBitmap();
      for (int i = 0; i < numVectors; i++) {
        fullFilter.add(i);
      }

      float[] queryVector = vectors[0];
      int topK = 5;

      ImmutableRoaringBitmap unfilteredResult = reader.getDocIds(queryVector, topK);
      ImmutableRoaringBitmap filteredResult = reader.getDocIds(queryVector, topK, fullFilter);

      Assert.assertEquals(filteredResult.getCardinality(), unfilteredResult.getCardinality(),
          "Filtered with full bitmap should return same count as unfiltered");
      // Check that results are the same
      org.roaringbitmap.IntIterator it = unfilteredResult.getIntIterator();
      while (it.hasNext()) {
        int docId = it.next();
        Assert.assertTrue(filteredResult.contains(docId),
            "Doc " + docId + " in unfiltered result should also be in filtered result with full bitmap");
      }
    }
  }

  @Test
  public void testSupportsPreFilterReturnsTrue()
      throws Exception {
    int numVectors = 10;
    int dimension = 4;
    int nlist = 2;
    float[][] vectors = generateVectors(numVectors, dimension, new Random(TEST_SEED));

    createIndex(vectors, dimension, nlist, VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN);

    VectorIndexConfig config = createConfig(dimension, nlist, VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN);
    try (IvfFlatVectorIndexReader reader = new IvfFlatVectorIndexReader(COLUMN_NAME, _tempDir, config)) {
      Assert.assertTrue(reader.supportsPreFilter(), "IvfFlatVectorIndexReader should support pre-filter");
    }
  }

  // -----------------------------------------------------------------------
  // Helpers
  // -----------------------------------------------------------------------

  private float[][] generateVectors(int count, int dimension, Random random) {
    float[][] vectors = new float[count][dimension];
    for (int i = 0; i < count; i++) {
      for (int d = 0; d < dimension; d++) {
        vectors[i][d] = random.nextFloat() * 10 - 5;
      }
    }
    return vectors;
  }

  private void createIndex(float[][] vectors, int dimension, int nlist,
      VectorIndexConfig.VectorDistanceFunction distanceFunction)
      throws Exception {
    Map<String, String> properties = new HashMap<>();
    properties.put("nlist", String.valueOf(nlist));
    VectorIndexConfig config = new VectorIndexConfig(false, "IVF_FLAT", dimension, 1, distanceFunction, properties);

    try (IvfFlatVectorIndexCreator creator = new IvfFlatVectorIndexCreator(
        COLUMN_NAME, _tempDir, config)) {
      for (float[] vector : vectors) {
        creator.add(vector);
      }
      creator.seal();
    }
  }

  private VectorIndexConfig createConfig(int dimension, int nlist,
      VectorIndexConfig.VectorDistanceFunction distanceFunction) {
    Map<String, String> properties = new HashMap<>();
    properties.put("nlist", String.valueOf(nlist));
    return new VectorIndexConfig(false, "IVF_FLAT", dimension, 1, distanceFunction, properties);
  }
}
