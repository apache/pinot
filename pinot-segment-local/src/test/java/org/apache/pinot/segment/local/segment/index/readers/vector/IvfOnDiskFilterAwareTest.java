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
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.index.vector.IvfFlatVectorIndexCreator;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;
import org.apache.pinot.segment.spi.index.creator.VectorQuantizerType;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Tests for IVF_ON_DISK filter-aware ANN search behavior.
 */
public class IvfOnDiskFilterAwareTest {
  private static final String COLUMN_NAME = "vectorCol";
  private static final long TEST_SEED = 1234L;

  private File _tempDir;

  @BeforeMethod
  public void setUp()
      throws IOException {
    _tempDir = new File(FileUtils.getTempDirectory(), "ivf_on_disk_filter_test_" + System.nanoTime());
    Assert.assertTrue(_tempDir.mkdirs(), "Failed to create temp directory: " + _tempDir);
  }

  @AfterMethod
  public void tearDown() {
    FileUtils.deleteQuietly(_tempDir);
  }

  @Test
  public void testPreFilterReturnsOnlyFilteredDocs()
      throws Exception {
    int numVectors = 80;
    int dimension = 4;
    int nlist = 8;
    float[][] vectors = generateVectors(numVectors, dimension, new Random(TEST_SEED));
    createIvfFlatIndex(vectors, dimension, nlist, VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN);

    VectorIndexConfig readerConfig =
        createReaderConfig(dimension, nlist, VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN);
    try (IvfOnDiskVectorIndexReader reader = new IvfOnDiskVectorIndexReader(COLUMN_NAME, _tempDir, readerConfig)) {
      Assert.assertTrue(reader.supportsPreFilter(), "IVF_ON_DISK reader should report pre-filter support");
      reader.setNprobe(nlist);

      MutableRoaringBitmap preFilter = new MutableRoaringBitmap();
      for (int i = 0; i < numVectors; i += 3) {
        preFilter.add(i);
      }

      ImmutableRoaringBitmap result = reader.getDocIds(vectors[0], 10, preFilter);
      Assert.assertTrue(result.getCardinality() <= 10);
      Assert.assertTrue(result.getCardinality() > 0);

      org.roaringbitmap.IntIterator it = result.getIntIterator();
      while (it.hasNext()) {
        int docId = it.next();
        Assert.assertTrue(preFilter.contains(docId),
            "Returned doc ID " + docId + " is not in the pre-filter bitmap");
      }
    }
  }

  @Test
  public void testPreFilterSkipsDistanceForRejectedDocs()
      throws Exception {
    int numVectors = 16;
    int dimension = 4;
    int nlist = 1;
    float[][] vectors = generateVectors(numVectors, dimension, new Random(TEST_SEED));
    createIvfFlatIndex(vectors, dimension, nlist, VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN);

    VectorIndexConfig readerConfig =
        createReaderConfig(dimension, nlist, VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN);
    try (CountingIvfOnDiskVectorIndexReader reader =
             new CountingIvfOnDiskVectorIndexReader(COLUMN_NAME, _tempDir, readerConfig)) {
      reader.setNprobe(nlist);

      MutableRoaringBitmap preFilter = new MutableRoaringBitmap();
      preFilter.add(0);

      ImmutableRoaringBitmap result = reader.getDocIds(vectors[0], 5, preFilter);
      Assert.assertEquals(result.getCardinality(), 1);
      Assert.assertTrue(result.contains(0));
      Assert.assertEquals(reader.getDistanceComputationCount(), 1,
          "Only docs that survive the pre-filter should be decoded and scored");
    }
  }

  @Test
  public void testPreFilterEmptyBitmapReturnsEmpty()
      throws Exception {
    int numVectors = 24;
    int dimension = 4;
    int nlist = 4;
    float[][] vectors = generateVectors(numVectors, dimension, new Random(TEST_SEED));
    createIvfFlatIndex(vectors, dimension, nlist, VectorIndexConfig.VectorDistanceFunction.COSINE);

    VectorIndexConfig readerConfig =
        createReaderConfig(dimension, nlist, VectorIndexConfig.VectorDistanceFunction.COSINE);
    try (IvfOnDiskVectorIndexReader reader = new IvfOnDiskVectorIndexReader(COLUMN_NAME, _tempDir, readerConfig)) {
      MutableRoaringBitmap emptyFilter = new MutableRoaringBitmap();
      ImmutableRoaringBitmap result = reader.getDocIds(vectors[0], 5, emptyFilter);
      Assert.assertEquals(result.getCardinality(), 0);
    }
  }

  @Test
  public void testDebugInfoReportsFilterAwareCounters()
      throws Exception {
    int numVectors = 40;
    int dimension = 4;
    int nlist = 4;
    float[][] vectors = generateVectors(numVectors, dimension, new Random(TEST_SEED));
    createIvfFlatIndex(vectors, dimension, nlist, VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN);

    VectorIndexConfig readerConfig =
        createReaderConfig(dimension, nlist, VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN);
    try (IvfOnDiskVectorIndexReader reader = new IvfOnDiskVectorIndexReader(COLUMN_NAME, _tempDir, readerConfig)) {
      reader.getDocIds(vectors[0], 5);

      MutableRoaringBitmap preFilter = new MutableRoaringBitmap();
      preFilter.add(0);
      preFilter.add(1);
      preFilter.add(2);
      reader.getDocIds(vectors[0], 5, preFilter);

      Map<String, Object> debugInfo = reader.getIndexDebugInfo();
      Assert.assertEquals(((Number) debugInfo.get("totalSearches")).longValue(), 2L);
      Assert.assertEquals(((Number) debugInfo.get("unfilteredSearches")).longValue(), 1L);
      Assert.assertEquals(((Number) debugInfo.get("filteredSearches")).longValue(), 1L);
      Assert.assertEquals(debugInfo.get("supportsPreFilter"), Boolean.TRUE);
    }
  }

  @Test
  public void testQuantizedOnDiskSearchPreservesQuantizerMetadata()
      throws Exception {
    int numVectors = 48;
    int dimension = 4;
    int nlist = 6;
    float[][] vectors = generateVectors(numVectors, dimension, new Random(TEST_SEED));
    createIvfFlatIndex(vectors, dimension, nlist, VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN,
        VectorQuantizerType.SQ8);

    VectorIndexConfig readerConfig =
        createReaderConfig(dimension, nlist, VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN,
            VectorQuantizerType.SQ8);
    try (IvfOnDiskVectorIndexReader reader = new IvfOnDiskVectorIndexReader(COLUMN_NAME, _tempDir, readerConfig)) {
      reader.setNprobe(nlist);
      ImmutableRoaringBitmap result = reader.getDocIds(vectors[0], 5);
      Assert.assertTrue(result.contains(0), "Quantized IVF_ON_DISK search should still retrieve the stored vector");
      Assert.assertEquals(reader.getIndexDebugInfo().get("quantizer"), VectorQuantizerType.SQ8.name());
    }
  }

  private float[][] generateVectors(int count, int dimension, Random random) {
    float[][] vectors = new float[count][dimension];
    for (int i = 0; i < count; i++) {
      for (int d = 0; d < dimension; d++) {
        vectors[i][d] = random.nextFloat() * 10 - 5;
      }
    }
    return vectors;
  }

  private void createIvfFlatIndex(float[][] vectors, int dimension, int nlist,
      VectorIndexConfig.VectorDistanceFunction distanceFunction)
      throws Exception {
    createIvfFlatIndex(vectors, dimension, nlist, distanceFunction, VectorQuantizerType.FLAT);
  }

  private void createIvfFlatIndex(float[][] vectors, int dimension, int nlist,
      VectorIndexConfig.VectorDistanceFunction distanceFunction, VectorQuantizerType quantizerType)
      throws Exception {
    Map<String, String> properties = new HashMap<>();
    properties.put("nlist", String.valueOf(nlist));
    properties.put("quantizer", quantizerType.name());
    VectorIndexConfig creatorConfig =
        new VectorIndexConfig(false, "IVF_FLAT", dimension, 1, distanceFunction, properties);
    try (IvfFlatVectorIndexCreator creator = new IvfFlatVectorIndexCreator(COLUMN_NAME, _tempDir, creatorConfig)) {
      for (float[] vector : vectors) {
        creator.add(vector);
      }
      creator.seal();
    }
  }

  private VectorIndexConfig createReaderConfig(int dimension, int nlist,
      VectorIndexConfig.VectorDistanceFunction distanceFunction) {
    return createReaderConfig(dimension, nlist, distanceFunction, VectorQuantizerType.FLAT);
  }

  private VectorIndexConfig createReaderConfig(int dimension, int nlist,
      VectorIndexConfig.VectorDistanceFunction distanceFunction, VectorQuantizerType quantizerType) {
    Map<String, String> properties = new HashMap<>();
    properties.put("nlist", String.valueOf(nlist));
    properties.put("quantizer", quantizerType.name());
    return new VectorIndexConfig(false, "IVF_ON_DISK", dimension, 1, distanceFunction, properties);
  }

  private static final class CountingIvfOnDiskVectorIndexReader extends IvfOnDiskVectorIndexReader {
    private final AtomicInteger _distanceComputations = new AtomicInteger();

    private CountingIvfOnDiskVectorIndexReader(String column, File indexDir, VectorIndexConfig config) {
      super(column, indexDir, config);
    }

    @Override
    protected float readDistanceForCurrentVector(ByteBuffer buf, float[] query, byte[] encodedVector) {
      _distanceComputations.incrementAndGet();
      return super.readDistanceForCurrentVector(buf, query, encodedVector);
    }

    private int getDistanceComputationCount() {
      return _distanceComputations.get();
    }
  }
}
