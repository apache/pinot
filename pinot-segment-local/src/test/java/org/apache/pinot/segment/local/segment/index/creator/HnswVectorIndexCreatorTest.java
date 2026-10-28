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
package org.apache.pinot.segment.local.segment.index.creator;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.creator.impl.vector.HnswVectorIndexCreator;
import org.apache.pinot.segment.local.segment.index.readers.vector.HnswVectorIndexReader;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class HnswVectorIndexCreatorTest {
  private static final File INDEX_DIR =
      new File(FileUtils.getTempDirectory(), HnswVectorIndexCreatorTest.class.toString());
  private VectorIndexConfig _config;

  @BeforeMethod
  public void setUp()
      throws IOException {
    FileUtils.forceMkdir(INDEX_DIR);

    Map<String, String> properties = new HashMap<>();

    properties.put("vectorIndexType", "HNSW");
    properties.put("vectorDimension", "1536");

    _config = new VectorIndexConfig(properties);
    try (HnswVectorIndexCreator creator = new HnswVectorIndexCreator("foo", INDEX_DIR, _config)) {
      float[] values1 = new float[] {5.0F, 42.0F, 54.33333F, 42.24F, 1001.045F};
      creator.add(values1);
      float[] values2 = new float[] {42.0F, 23423.0F, 42431.32532F, 6785676.3242F, 42.3F};
      creator.add(values2);
      float[] values3 = new float[] {1.0F, 2.0F, 3.0F, 4.0F, 5.0F};
      creator.add(values3);
      float[] values4 = new float[] {42.678F, 23423423.0F, 42431.32523432F, 6723485.3242F, 42342.3F};
      creator.add(values4);
      creator.seal();
    }
  }

  @AfterMethod
  public void tearDown()
      throws IOException {
    FileUtils.deleteDirectory(INDEX_DIR);
  }

  @Test
  public void testIndexWriterReaderWithTop3()
      throws IOException {
    // Use VectorIndex reader to validate that reads work
    try (HnswVectorIndexReader reader = new HnswVectorIndexReader("foo", INDEX_DIR, 4, _config)) {
      int[] matchedDocIds = reader.getDocIds(new float[]{5.0F, 42.0F, 54.33333F, 42.24F, 3413.4F}, 3).toArray();
      // Expect to get 3 matching docIds since topK = 3 is used
      Assert.assertEquals(matchedDocIds.length, 3);
      Assert.assertEquals(matchedDocIds[0], 0);
      Assert.assertEquals(matchedDocIds[1], 2);
      Assert.assertEquals(matchedDocIds[1], 2);
    }
  }

  @Test
  public void testIndexWriterReaderWithTop1()
      throws IOException {
    // Use VectorIndex reader to validate that reads work
    try (HnswVectorIndexReader reader = new HnswVectorIndexReader("foo", INDEX_DIR, 4, _config)) {
      int[] matchedDocIds = reader.getDocIds(new float[]{1.0F, 2.0F, 3.0F, 4.0F, 5.0F}, 1).toArray();
      // Expect to get 1 matching docId since topK = 1 is used
      Assert.assertEquals(matchedDocIds.length, 1);
      Assert.assertEquals(matchedDocIds[0], 2);
    }
  }

  @Test
  public void testFilteredReaderReturnsKAllowedDocuments()
      throws IOException {
    MutableRoaringBitmap allowedDocIds = new MutableRoaringBitmap();
    allowedDocIds.add(1);
    allowedDocIds.add(3);
    try (HnswVectorIndexReader reader = new HnswVectorIndexReader("foo", INDEX_DIR, 4, _config)) {
      float[] queryVector = {5.0F, 42.0F, 54.33333F, 42.24F, 1001.045F};
      Assert.assertEquals(reader.getDocIds(queryVector, 1).toArray(), new int[]{0},
          "The nearest physical document should be outside the allowed set");
      int[] matchedDocIds = reader.getDocIds(queryVector, 2, allowedDocIds).toArray();
      Assert.assertEquals(matchedDocIds, new int[]{1, 3});
    }
  }

  /// A null bitmap must not fall through to an unfiltered search, which would return doc ids outside the filter.
  /// The reader rejects it with its contract message rather than a bare NullPointerException from inside Lucene.
  @Test
  public void testFilteredReaderRejectsNullBitmap()
      throws IOException {
    try (HnswVectorIndexReader reader = new HnswVectorIndexReader("foo", INDEX_DIR, 4, _config)) {
      float[] queryVector = {5.0F, 42.0F, 54.33333F, 42.24F, 1001.045F};
      NullPointerException thrown = Assert.expectThrows(NullPointerException.class,
          () -> reader.getDocIds(queryVector, 2, (ImmutableRoaringBitmap) null));
      Assert.assertNotNull(thrown.getMessage(), "The rejection must carry the pre-filter contract message");
      Assert.assertTrue(thrown.getMessage().contains("must not be null"),
          "Expected the pre-filter contract message, got: " + thrown.getMessage());
    }
  }

  /// An empty filter admits nothing, so the search is skipped entirely. Asserting only that the result is empty
  /// would not discriminate -- Lucene returns no hits for a zero-match filter anyway. Disabling the bounded queue
  /// without an efSearch makes runtime-control validation throw when a query is actually built, so reaching an
  /// empty result here proves the short-circuit ran before query construction.
  @Test
  public void testFilteredReaderShortCircuitsEmptyBitmapBeforeBuildingQuery()
      throws IOException {
    try (HnswVectorIndexReader reader = new HnswVectorIndexReader("foo", INDEX_DIR, 4, _config)) {
      float[] queryVector = {5.0F, 42.0F, 54.33333F, 42.24F, 1001.045F};
      reader.setUseBoundedQueue(false);
      Assert.assertTrue(reader.getDocIds(queryVector, 2, new MutableRoaringBitmap()).isEmpty(),
          "An empty filter must short-circuit before query construction");
    }
  }

  @Test
  public void testEfSearchChangesRuntimeSearchBehavior()
      throws IOException {
    try (HnswVectorIndexReader reader = new HnswVectorIndexReader("foo", INDEX_DIR, 4, _config)) {
      reader.setEfSearch(1);
      int[] matchedDocIds = reader.getDocIds(new float[]{5.0F, 42.0F, 54.33333F, 42.24F, 3413.4F}, 3).toArray();
      Assert.assertEquals(matchedDocIds.length, 1,
          "efSearch=1 should cap the Lucene HNSW visit budget and reduce the returned result set");
      Assert.assertEquals(reader.getIndexDebugInfo().get("effectiveEfSearch"), 1);
    }
  }

  @Test
  public void testDisableBoundedQueueRequiresEfSearch()
      throws IOException {
    try (HnswVectorIndexReader reader = new HnswVectorIndexReader("foo", INDEX_DIR, 4, _config)) {
      reader.setUseBoundedQueue(false);
      IllegalArgumentException error = Assert.expectThrows(IllegalArgumentException.class,
          () -> reader.getDocIds(new float[]{1.0F, 2.0F, 3.0F, 4.0F, 5.0F}, 2));
      Assert.assertTrue(error.getMessage().contains("vectorEfSearch"));
    }
  }

  @Test
  public void testRuntimeControlDebugInfoReflectsOverrides()
      throws IOException {
    try (HnswVectorIndexReader reader = new HnswVectorIndexReader("foo", INDEX_DIR, 4, _config)) {
      reader.setEfSearch(6);
      reader.setUseRelativeDistance(false);
      reader.setUseBoundedQueue(false);

      Map<String, Object> debugInfo = reader.getIndexDebugInfo();
      Assert.assertEquals(debugInfo.get("effectiveEfSearch"), 6);
      Assert.assertEquals(debugInfo.get("effectiveHnswUseRelativeDistance"), Boolean.FALSE);
      Assert.assertEquals(debugInfo.get("effectiveHnswUseBoundedQueue"), Boolean.FALSE);
      Assert.assertEquals(debugInfo.get("supportsPreFilter"), reader.supportsPreFilter(),
          "Debug info must report the reader's actual pre-filter capability, not a hardcoded literal");
    }
  }

  @DataProvider
  public Object[][] parallelBuilds() {
    // buildThreads, forceMerge, parallelBuildMaxDocs: the last case outgrows the buffer and falls back to one writer
    return new Object[][]{{1, true, 50_000}, {4, true, 50_000}, {4, false, 50_000}, {3, true, 100}};
  }

  /// Every build mode, including the default single writer, must index each row under its own doc id: searching for a
  /// stored vector finds that row.
  @Test(dataProvider = "parallelBuilds")
  public void testParallelBuildKeepsDocIds(int buildThreads, boolean forceMerge, int parallelBuildMaxDocs)
      throws IOException {
    File indexDir = new File(INDEX_DIR, "parallel");
    FileUtils.forceMkdir(indexDir);
    Map<String, String> properties = new HashMap<>();
    properties.put("vectorIndexType", "HNSW");
    properties.put("vectorDimension", "8");
    properties.put("buildThreads", String.valueOf(buildThreads));
    properties.put("forceMerge", String.valueOf(forceMerge));
    properties.put("parallelBuildMaxDocs", String.valueOf(parallelBuildMaxDocs));
    VectorIndexConfig config = new VectorIndexConfig(properties);
    int numDocs = 1000;
    float[][] vectors = new float[numDocs][8];
    Random random = new Random(42);
    try (HnswVectorIndexCreator creator = new HnswVectorIndexCreator("bar", indexDir, config)) {
      float[] reused = new float[8];
      for (int i = 0; i < numDocs; i++) {
        for (int d = 0; d < 8; d++) {
          vectors[i][d] = random.nextFloat();
          reused[d] = vectors[i][d];
        }
        // The creator must copy the row, since callers reuse the array
        creator.add(reused);
      }
      creator.seal();
    }
    try (HnswVectorIndexReader reader = new HnswVectorIndexReader("bar", indexDir, numDocs, config)) {
      for (int docId = 0; docId < numDocs; docId += 37) {
        // k widens the HNSW search beam; with k = 1 the approximate search can miss even an exact match
        Assert.assertTrue(reader.getDocIds(vectors[docId], 10).contains(docId), "Doc " + docId + " not found");
      }
    }
    String[] leftovers = indexDir.list((dir, name) -> name.contains(".hnsw.part"));
    Assert.assertEquals(leftovers.length, 0, "Per-thread build directories must be removed");
  }
}
