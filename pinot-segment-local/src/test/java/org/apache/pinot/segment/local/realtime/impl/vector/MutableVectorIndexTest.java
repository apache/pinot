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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.segment.local.realtime.impl.invertedindex.RealtimeLuceneTextIndexSearcherPool;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
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

  @Test
  public void testSearchReturnsSuppliedPinotDocumentIds() {
    MutableVectorIndex index = createEmptyIndex(createConfig(1, 3_600_000L));
    try {
      addVector(index, new float[]{1.0F, 0.0F, 0.0F, 0.0F, 0.0F}, 17);
      addVector(index, new float[]{0.0F, 1.0F, 0.0F, 0.0F, 0.0F}, 91);

      Assert.assertEquals(index.getDocIds(new float[]{1.0F, 0.0F, 0.0F, 0.0F, 0.0F}, 1).toArray(),
          new int[]{17}, "Search results must contain the supplied Pinot ID, not Lucene's internal document ID");
      Assert.assertEquals(index.getDocIds(new float[]{0.0F, 1.0F, 0.0F, 0.0F, 0.0F}, 2).toArray(),
          new int[]{17, 91},
          "Pinot ID translation must work when relevance order is the reverse of Lucene document order");
    } finally {
      index.close();
    }
  }

  @Test
  public void testFilteredSearchExcludesNearerDisallowedDocuments() {
    MutableVectorIndex index = createEmptyIndex(createConfig(1, 3_600_000L));
    try {
      addFilterFixture(index);
      float[] queryVector = {1.0F, 0.0F, 0.0F, 0.0F, 0.0F};
      Assert.assertEquals(index.getDocIds(queryVector, 1).toArray(), new int[]{101},
          "The nearest unfiltered document should be outside the allowed set");

      MutableRoaringBitmap allowedDocIds = bitmapOf(7, 42);
      Assert.assertEquals(index.getDocIds(queryVector, 2, allowedDocIds).toArray(), new int[]{7, 42});
      Assert.assertEquals(allowedDocIds, bitmapOf(7, 42), "Filtered search must not mutate the caller bitmap");
    } finally {
      index.close();
    }
  }

  @Test
  public void testUncommittedAdditionIsNearRealTimeVisible() {
    MutableVectorIndex index = createEmptyIndex(createConfig(1_000, 3_600_000L));
    try {
      addVector(index, new float[]{1.0F, 0.0F, 0.0F, 0.0F, 0.0F}, 23);

      Assert.assertEquals(index.getDocIds(new float[]{1.0F, 0.0F, 0.0F, 0.0F, 0.0F}, 1).toArray(),
          new int[]{23}, "A completed addition must be visible before the commit threshold is reached");
      Assert.assertEquals(index.getIndexDebugInfo().get("numDocs"), 1);
    } finally {
      index.close();
    }
  }

  @Test
  public void testFilteredSearchSeesUncommittedVectorAndDocValueInSameReader() {
    MutableVectorIndex index = createEmptyIndex(createConfig(1_000, 3_600_000L));
    try {
      addVector(index, new float[]{0.0F, 1.0F, 0.0F, 0.0F, 0.0F}, 5);
      addVector(index, new float[]{1.0F, 0.0F, 0.0F, 0.0F, 0.0F}, 137);

      Assert.assertEquals(
          index.getDocIds(new float[]{1.0F, 0.0F, 0.0F, 0.0F, 0.0F}, 1, bitmapOf(137)).toArray(),
          new int[]{137},
          "Filtered NRT search must see the vector and its Pinot-ID doc value in one reader generation");
      Assert.assertEquals(index.getIndexDebugInfo().get("numDocs"), 2);
    } finally {
      index.close();
    }
  }

  @Test
  public void testSameSegmentAndColumnIndexesUseIsolatedDirectories() {
    String segmentName = "mutableVectorIndexTest_" + System.nanoTime();
    MutableVectorIndex firstIndex = createIndex(segmentName, COLUMN_NAME);
    boolean firstIndexClosed = false;
    try {
      MutableVectorIndex secondIndex = createIndex(segmentName, COLUMN_NAME);
      try {
        firstIndex.close();
        firstIndexClosed = true;
        int[] matches =
            secondIndex.getDocIds(new float[]{5.0F, 42.0F, 54.33333F, 42.24F, 3413.4F}, 3).toArray();
        Assert.assertEquals(matches.length, 3,
            "Closing one instance must not remove another instance's same-segment, same-column index");
      } finally {
        secondIndex.close();
      }
    } finally {
      if (!firstIndexClosed) {
        firstIndex.close();
      }
    }
  }

  @Test
  public void testFilteredSearchCopiesBitmapBeforeAsyncDispatch()
      throws Exception {
    MutableVectorIndex index = createEmptyIndex(createConfig(1, 3_600_000L));
    ExecutorService callerExecutor = Executors.newSingleThreadExecutor();
    CountDownLatch searcherBlocked = new CountDownLatch(1);
    CountDownLatch releaseSearcher = new CountDownLatch(1);
    Future<?> blocker = RealtimeLuceneTextIndexSearcherPool.getInstance().getExecutorService().submit(() -> {
      searcherBlocked.countDown();
      try {
        releaseSearcher.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    });
    try {
      Assert.assertTrue(searcherBlocked.await(10, TimeUnit.SECONDS));
      addFilterFixture(index);
      CountDownLatch bitmapCopied = new CountDownLatch(1);
      CopySignalingBitmap allowedDocIds = new CopySignalingBitmap(bitmapCopied);
      allowedDocIds.add(7);
      allowedDocIds.add(42);

      Future<ImmutableRoaringBitmap> search = callerExecutor.submit(
          () -> index.getDocIds(new float[]{1.0F, 0.0F, 0.0F, 0.0F, 0.0F}, 2, allowedDocIds));
      Assert.assertTrue(bitmapCopied.await(10, TimeUnit.SECONDS));
      allowedDocIds.clear();
      allowedDocIds.add(101);
      releaseSearcher.countDown();

      Assert.assertEquals(search.get(10, TimeUnit.SECONDS), bitmapOf(7, 42),
          "Queued search must use the bitmap state captured before dispatch");
      blocker.get(10, TimeUnit.SECONDS);
    } finally {
      releaseSearcher.countDown();
      callerExecutor.shutdownNow();
      index.close();
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

  private static MutableVectorIndex createEmptyIndex(VectorIndexConfig config) {
    return new MutableVectorIndex("mutableVectorIndexTest_" + System.nanoTime(), COLUMN_NAME, config);
  }

  private static void addFilterFixture(MutableVectorIndex index) {
    addVector(index, new float[]{1.0F, 0.0F, 0.0F, 0.0F, 0.0F}, 101);
    addVector(index, new float[]{0.0F, 1.0F, 0.0F, 0.0F, 0.0F}, 7);
    addVector(index, new float[]{-1.0F, 0.0F, 0.0F, 0.0F, 0.0F}, 42);
  }

  private static void addVector(MutableVectorIndex index, float[] values, int docId) {
    Object[] boxed = new Object[values.length];
    for (int i = 0; i < values.length; i++) {
      boxed[i] = values[i];
    }
    index.add(boxed, null, docId);
  }

  private static VectorIndexConfig createConfig() {
    return createConfig(4, MutableVectorIndex.DEFAULT_COMMIT_INTERVAL_MS);
  }

  private static VectorIndexConfig createConfig(long commitDocs, long commitIntervalMs) {
    Map<String, String> properties = new HashMap<>();
    properties.put("commitDocs", Long.toString(commitDocs));
    properties.put("commitIntervalMs", Long.toString(commitIntervalMs));
    properties.put("vectorIndexType", "HNSW");
    properties.put("vectorDimension", "5");
    return new VectorIndexConfig(false, "HNSW", 5, 1, VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN,
        properties);
  }

  private static MutableRoaringBitmap bitmapOf(int... docIds) {
    MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
    bitmap.add(docIds);
    return bitmap;
  }

  private static final class CopySignalingBitmap extends MutableRoaringBitmap {
    private final CountDownLatch _bitmapCopied;

    private CopySignalingBitmap(CountDownLatch bitmapCopied) {
      _bitmapCopied = bitmapCopied;
    }

    @Override
    public MutableRoaringBitmap toMutableRoaringBitmap() {
      MutableRoaringBitmap copy = super.toMutableRoaringBitmap();
      _bitmapCopied.countDown();
      return copy;
    }
  }
}
