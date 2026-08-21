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
package org.apache.pinot.segment.local.realtime.impl.vector;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pinot.segment.local.realtime.impl.invertedindex.RealtimeLuceneTextIndexSearcherPool;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class MutableVectorIndexTest {
  private static final String COLUMN_NAME = "embedding";
  private static final String OTHER_COLUMN_NAME = "otherEmbedding";

  @BeforeClass
  public void setUpSearcherPool() {
    RealtimeLuceneTextIndexSearcherPool.init(1);
  }

  @Test
  public void testEfSearchChangesRuntimeSearchBehavior() {
    MutableVectorIndex index = createIndex();
    try {
      int[] defaultMatches = index.getDocIds(new float[]{5.0F, 42.0F, 54.33333F, 42.24F, 3413.4F}, 3).toArray();
      Assert.assertEquals(defaultMatches.length, 3);

      index.setEfSearch(1);
      int[] limitedMatches = index.getDocIds(new float[]{5.0F, 42.0F, 54.33333F, 42.24F, 3413.4F}, 3).toArray();
      Assert.assertEquals(limitedMatches.length, 1,
          "efSearch=1 should cap mutable HNSW traversal the same way as immutable HNSW");
    } finally {
      index.close();
    }
  }

  @Test
  public void testDisableBoundedQueueRequiresEfSearch() {
    MutableVectorIndex index = createIndex();
    try {
      index.setUseBoundedQueue(false);
      IllegalArgumentException error = Assert.expectThrows(IllegalArgumentException.class,
          () -> index.getDocIds(new float[]{1.0F, 2.0F, 3.0F, 4.0F, 5.0F}, 2));
      Assert.assertTrue(error.getMessage().contains("vectorEfSearch"));
    } finally {
      index.close();
    }
  }

  @Test
  public void testRuntimeControlDebugInfoReflectsOverrides() {
    MutableVectorIndex index = createIndex();
    try {
      index.setEfSearch(6);
      index.setUseRelativeDistance(false);
      index.setUseBoundedQueue(false);

      Map<String, Object> debugInfo = index.getIndexDebugInfo();
      Assert.assertEquals(debugInfo.get("backend"), "HNSW");
      Assert.assertEquals(debugInfo.get("column"), COLUMN_NAME);
      Assert.assertEquals(debugInfo.get("numDocs"), 4);
      Assert.assertEquals(debugInfo.get("effectiveEfSearch"), 6);
      Assert.assertEquals(debugInfo.get("effectiveHnswUseRelativeDistance"), Boolean.FALSE);
      Assert.assertEquals(debugInfo.get("effectiveHnswUseBoundedQueue"), Boolean.FALSE);
      Assert.assertEquals(debugInfo.get("supportsPreFilter"), Boolean.TRUE);
    } finally {
      index.close();
    }
  }

  // -----------------------------------------------------------------------
  // Filtered search (upsert doc-ids snapshot enforcement)
  // -----------------------------------------------------------------------

  /// 2-D corpus with distinct distances from the query vector {1, 0}:
  /// docs 0 and 1 are nearest (the "upsert-obsoleted" rows), docs 2 and 3 are the valid rows.
  private static MutableVectorIndex create2DIndex(long commitDocs, int docIdOffset) {
    Map<String, String> properties = new HashMap<>();
    properties.put("commitDocs", String.valueOf(commitDocs));
    properties.put("vectorIndexType", "HNSW");
    properties.put("vectorDimension", "2");
    VectorIndexConfig config = new VectorIndexConfig(false, "HNSW", 2, 1,
        VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN, properties);
    MutableVectorIndex index =
        new MutableVectorIndex("mutableVectorIndexFilterTest_" + System.nanoTime(), COLUMN_NAME, config);
    addVector(index, new float[]{1.0F, 0.0F}, docIdOffset);
    addVector(index, new float[]{0.99F, 0.01F}, docIdOffset + 1);
    addVector(index, new float[]{0.0F, 1.0F}, docIdOffset + 2);
    addVector(index, new float[]{0.0F, -1.0F}, docIdOffset + 3);
    return index;
  }

  @Test
  public void testFilteredSearchExcludesNearestDisallowedDocs() {
    // commitDocs=4 commits on the 4th add, so the unfiltered committed-view sanity check below sees all rows
    MutableVectorIndex index = create2DIndex(4, 0);
    try {
      // Sanity: unfiltered top-2 returns the physically nearest ("obsolete") docs 0 and 1
      ImmutableRoaringBitmap unfiltered = index.getDocIds(new float[]{1.0F, 0.0F}, 2);
      Assert.assertEquals(unfiltered, ImmutableRoaringBitmap.bitmapOf(0, 1));

      // Filtered top-2 restricted to docs 2 and 3 must return exactly those docs. A post-intersection
      // implementation would return empty here (the unfiltered top-2 has no overlap with the allowed set),
      // so this assertion genuinely discriminates filtered candidate generation.
      ImmutableRoaringBitmap filtered =
          index.getDocIds(new float[]{1.0F, 0.0F}, 2, ImmutableRoaringBitmap.bitmapOf(2, 3));
      Assert.assertEquals(filtered, ImmutableRoaringBitmap.bitmapOf(2, 3),
          "Filtered search must return the allowed docs, not the nearest disallowed ones");
    } finally {
      index.close();
    }
  }

  @Test
  public void testConcurrentFilteredSearchWithLiveWriter()
      throws Exception {
    // Single writer, concurrent reader: filtered searches must stay correct (results always a subset of
    // the filter bitmap) while rows are being added and commits fire mid-run
    Map<String, String> properties = new HashMap<>();
    properties.put("commitDocs", "7");
    properties.put("vectorIndexType", "HNSW");
    properties.put("vectorDimension", "2");
    VectorIndexConfig config = new VectorIndexConfig(false, "HNSW", 2, 1,
        VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN, properties);
    MutableVectorIndex index =
        new MutableVectorIndex("mutableVectorIndexConcurrentTest_" + System.nanoTime(), COLUMN_NAME, config);
    int numDocs = 200;
    ImmutableRoaringBitmap allowed = ImmutableRoaringBitmap.bitmapOf(2, 3);
    AtomicReference<Throwable> failure =
        new AtomicReference<>();
    try {
      addVector(index, new float[]{0.0F, 1.0F}, 0);
      addVector(index, new float[]{0.0F, -1.0F}, 1);
      addVector(index, new float[]{1.0F, 0.0F}, 2);
      addVector(index, new float[]{0.99F, 0.01F}, 3);

      Thread writer = new Thread(() -> {
        try {
          for (int docId = 4; docId < numDocs; docId++) {
            addVector(index, new float[]{-1.0F, 0.0F}, docId);
          }
        } catch (Throwable t) {
          failure.compareAndSet(null, t);
        }
      });
      writer.start();
      while (writer.isAlive() && failure.get() == null) {
        ImmutableRoaringBitmap filtered = index.getDocIds(new float[]{1.0F, 0.0F}, 2, allowed);
        Assert.assertEquals(filtered, allowed,
            "Filtered search under a live writer must return exactly the allowed nearest docs");
      }
      writer.join();
      Assert.assertNull(failure.get(), "Writer thread failed: " + failure.get());
      ImmutableRoaringBitmap filtered = index.getDocIds(new float[]{1.0F, 0.0F}, 2, allowed);
      Assert.assertEquals(filtered, allowed);
    } finally {
      index.close();
    }
  }

  @Test
  public void testFilteredSearchSeesUncommittedDocs() {
    // commitDocs is far larger than the number of added docs and commitIntervalMs defaults to 10s, so no
    // commit has happened: NRT visibility is what makes the filtered search see the rows at all
    MutableVectorIndex index = create2DIndex(1_000_000, 0);
    try {
      ImmutableRoaringBitmap filtered =
          index.getDocIds(new float[]{1.0F, 0.0F}, 4, ImmutableRoaringBitmap.bitmapOf(0, 1, 2, 3));
      Assert.assertEquals(filtered.getCardinality(), 4,
          "Filtered search must see uncommitted rows through the NRT reader");
    } finally {
      index.close();
    }
  }

  @Test
  public void testFilteredSearchTranslatesSuppliedPinotDocIds() {
    // Pinot doc ids offset by 100: results must be the SUPPLIED doc ids, proving no reliance on
    // ScoreDoc.doc == Pinot docId
    MutableVectorIndex index = create2DIndex(1000, 100);
    try {
      ImmutableRoaringBitmap filtered =
          index.getDocIds(new float[]{1.0F, 0.0F}, 2, ImmutableRoaringBitmap.bitmapOf(102, 103));
      Assert.assertEquals(filtered, ImmutableRoaringBitmap.bitmapOf(102, 103));
    } finally {
      index.close();
    }
  }

  @Test
  public void testFilteredSearchWithEmptyBitmapReturnsEmpty() {
    MutableVectorIndex index = create2DIndex(1000, 0);
    try {
      ImmutableRoaringBitmap filtered =
          index.getDocIds(new float[]{1.0F, 0.0F}, 2, ImmutableRoaringBitmap.bitmapOf());
      Assert.assertEquals(filtered.getCardinality(), 0);
    } finally {
      index.close();
    }
  }

  @Test
  public void testUnfilteredSearchTranslatesSuppliedPinotDocIds() {
    // commitDocs=4 commits on the 4th add, so the committed-view unfiltered path sees all rows; with doc ids
    // offset by 100 the results must be the supplied ids
    MutableVectorIndex index = create2DIndex(4, 100);
    try {
      int[] matches = index.getDocIds(new float[]{1.0F, 0.0F}, 2).toArray();
      Assert.assertEquals(matches.length, 2);
      Assert.assertEquals(matches[0], 100);
      Assert.assertEquals(matches[1], 101);
    } finally {
      index.close();
    }
  }

  @Test
  public void testCloseOnlyRemovesIndexOfClosedColumn() {
    String segmentName = "mutableVectorIndexTest_" + System.nanoTime();
    MutableVectorIndex index = createIndex(segmentName, COLUMN_NAME);
    MutableVectorIndex otherIndex = createIndex(segmentName, OTHER_COLUMN_NAME);
    try {
      index.close();
      int[] matches = otherIndex.getDocIds(new float[]{5.0F, 42.0F, 54.33333F, 42.24F, 3413.4F}, 3).toArray();
      Assert.assertEquals(matches.length, 3);
    } finally {
      otherIndex.close();
    }
  }

  private static MutableVectorIndex createIndex() {
    return createIndex("mutableVectorIndexTest_" + System.nanoTime(), COLUMN_NAME);
  }

  private static MutableVectorIndex createIndex(String segmentName, String column) {
    MutableVectorIndex index = new MutableVectorIndex(segmentName, column, createConfig());
    addVector(index, new float[]{5.0F, 42.0F, 54.33333F, 42.24F, 1001.045F}, 0);
    addVector(index, new float[]{42.0F, 23423.0F, 42431.32532F, 6785676.3242F, 42.3F}, 1);
    addVector(index, new float[]{1.0F, 2.0F, 3.0F, 4.0F, 5.0F}, 2);
    addVector(index, new float[]{42.678F, 23423423.0F, 42431.32523432F, 6723485.3242F, 42342.3F}, 3);
    return index;
  }

  private static void addVector(MutableVectorIndex index, float[] values, int docId) {
    Object[] boxed = new Object[values.length];
    for (int i = 0; i < values.length; i++) {
      boxed[i] = values[i];
    }
    index.add(boxed, null, docId);
  }

  private static VectorIndexConfig createConfig() {
    Map<String, String> properties = new HashMap<>();
    properties.put("commitDocs", "4");
    properties.put("vectorIndexType", "HNSW");
    properties.put("vectorDimension", "5");
    return new VectorIndexConfig(false, "HNSW", 5, 1, VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN,
        properties);
  }
}
