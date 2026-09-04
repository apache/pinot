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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
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
  private static final int SEARCHER_POOL_SIZE = 8;

  @BeforeClass
  public void setUpSearcherPool() {
    RealtimeLuceneTextIndexSearcherPool.init(SEARCHER_POOL_SIZE);
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
      Assert.assertTrue(index.supportsPreFilter(),
          "The reader must advertise filtered search: that is what makes the planner choose it over an exact scan");
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

  @Test(timeOut = 60_000)
  public void testConcurrentFilteredSearchWithLiveWriter()
      throws Exception {
    // Single writer, concurrent reader: each phase publishes one new allowed row, then makes the reader search for
    // it while the writer continues adding disallowed rows. This proves both NRT visibility and filtered-search
    // correctness while ingestion and commits are active.
    Map<String, String> properties = new HashMap<>();
    properties.put("commitDocs", "7");
    properties.put("vectorIndexType", "HNSW");
    properties.put("vectorDimension", "2");
    VectorIndexConfig config = new VectorIndexConfig(false, "HNSW", 2, 1,
        VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN, properties);
    MutableVectorIndex index =
        new MutableVectorIndex("mutableVectorIndexConcurrentTest_" + System.nanoTime(), COLUMN_NAME, config);
    int numPhases = 8;
    int docsPerPhase = 25;
    Phaser phaser = new Phaser(2);
    CountDownLatch[] queryEnteredFilter = new CountDownLatch[numPhases];
    CountDownLatch[] concurrentWritesFinished = new CountDownLatch[numPhases];
    for (int phase = 0; phase < numPhases; phase++) {
      queryEnteredFilter[phase] = new CountDownLatch(1);
      concurrentWritesFinished[phase] = new CountDownLatch(1);
    }
    AtomicReference<Throwable> failure = new AtomicReference<>();
    Thread writer = null;
    try {
      addVector(index, new float[]{0.0F, 1.0F}, 0);
      addVector(index, new float[]{0.0F, -1.0F}, 1);
      addVector(index, new float[]{1.0F, 0.0F}, 2);
      addVector(index, new float[]{0.99F, 0.01F}, 3);

      writer = new Thread(() -> {
        try {
          for (int phase = 0; phase < numPhases; phase++) {
            int firstDocId = 4 + phase * docsPerPhase;
            // Publish the row this phase's filtered search must observe before releasing the reader.
            addVector(index, new float[]{1.0F, 0.0F}, firstDocId);
            if (!advancePhase(phaser)) {
              return;
            }
            // Wait until the searcher is evaluating the filter, then write while that query is in progress.
            // The test bitmap blocks the search at its first membership check until these writes finish.
            if (!queryEnteredFilter[phase].await(10, TimeUnit.SECONDS)) {
              throw new TimeoutException("Filtered query did not enter its bitmap in phase " + phase);
            }
            for (int docId = firstDocId + 1; docId < firstDocId + docsPerPhase; docId++) {
              addVector(index, new float[]{-1.0F, 0.0F}, docId);
            }
            concurrentWritesFinished[phase].countDown();
            if (!advancePhase(phaser)) {
              return;
            }
          }
        } catch (Throwable t) {
          failure.compareAndSet(null, t);
          for (CountDownLatch writesFinished : concurrentWritesFinished) {
            writesFinished.countDown();
          }
          phaser.forceTermination();
        }
      }, "mutable-vector-index-test-writer");
      writer.start();

      for (int phase = 0; phase < numPhases; phase++) {
        Assert.assertTrue(advancePhase(phaser), "Writer terminated before publishing phase " + phase);
        Assert.assertNull(failure.get(), "Writer thread failed: " + failure.get());
        int allowedDocId = 4 + phase * docsPerPhase;
        ImmutableRoaringBitmap allowed = new ConcurrentWriteCoordinatingBitmap(allowedDocId,
            queryEnteredFilter[phase], concurrentWritesFinished[phase]);
        ImmutableRoaringBitmap filtered = index.getDocIds(new float[]{1.0F, 0.0F}, 1, allowed);
        Assert.assertEquals(filtered, allowed,
            "Filtered search must see the newly added allowed row while the writer remains active");
        Assert.assertTrue(advancePhase(phaser), "Writer terminated while completing phase " + phase);
      }

      writer.join();
      Assert.assertNull(failure.get(), "Writer thread failed: " + failure.get());
    } finally {
      phaser.forceTermination();
      if (writer != null) {
        writer.interrupt();
        writer.join(TimeUnit.SECONDS.toMillis(10));
        Assert.assertFalse(writer.isAlive(), "Writer thread did not terminate during test cleanup");
      }
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
      Assert.assertEquals(filtered.toArray(), new int[]{0, 1, 2, 3},
          "Filtered search must see uncommitted rows through the NRT reader, translated back to their Pinot "
              + "doc ids");
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

  @Test(timeOut = 60_000)
  public void testFilteredSearchWithEmptyBitmapReturnsEmpty()
      throws Exception {
    MutableVectorIndex index = create2DIndex(1000, 0);
    RealtimeLuceneTextIndexSearcherPool searcherPool = RealtimeLuceneTextIndexSearcherPool.getInstance();
    searcherPool.resize(1);
    ExecutorService searcherExecutor = searcherPool.getExecutorService();
    CountDownLatch blockerStarted = new CountDownLatch(1);
    CountDownLatch releaseBlocker = new CountDownLatch(1);
    ExecutorService caller = Executors.newSingleThreadExecutor();
    Future<?> blocker = searcherExecutor.submit(() -> {
      blockerStarted.countDown();
      if (!releaseBlocker.await(30, TimeUnit.SECONDS)) {
        throw new TimeoutException("Timed out waiting to release blocked search worker");
      }
      return null;
    });
    try {
      Assert.assertTrue(blockerStarted.await(10, TimeUnit.SECONDS), "Failed to occupy the search worker");

      Future<ImmutableRoaringBitmap> emptySearch = caller.submit(
          () -> index.getDocIds(new float[]{1.0F, 0.0F}, 2, ImmutableRoaringBitmap.bitmapOf()));
      ImmutableRoaringBitmap filtered = emptySearch.get(10, TimeUnit.SECONDS);
      Assert.assertEquals(filtered.getCardinality(), 0);
      Assert.assertEquals(index.getSearcherRefreshCount(), 0L,
          "An empty filter must return before search submission or NRT refresh");
    } finally {
      releaseBlocker.countDown();
      try {
        blocker.get(10, TimeUnit.SECONDS);
      } catch (Exception e) {
        Assert.fail("Failed to clean up blocked search worker", e);
      } finally {
        searcherPool.resize(SEARCHER_POOL_SIZE);
      }
      caller.shutdownNow();
      try {
        Assert.assertTrue(caller.awaitTermination(10, TimeUnit.SECONDS), "Empty-filter caller did not terminate");
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        Assert.fail("Interrupted while cleaning up empty-filter caller", e);
      }
      index.close();
    }
  }

  @Test(timeOut = 60_000)
  public void testConcurrentFilteredSearchesCoalesceRefresh()
      throws Exception {
    int numCallers = SEARCHER_POOL_SIZE;
    ExecutorService callers = Executors.newFixedThreadPool(numCallers);
    CoordinatedRefreshMutableVectorIndex index = createCoordinatedIndexWithoutCommits(1);
    try (index) {
      float[] query = {1.0F, 0.0F, 0.0F, 0.0F, 0.0F};
      Assert.assertEquals(index.getDocIds(query, 1, bitmapOf(0)).toArray(), new int[]{0});
      long initialRefreshCount = index.getSearcherRefreshCount();
      addVector(index, query, 10);
      index.coordinateNextRefresh();

      CyclicBarrier startTogether = new CyclicBarrier(numCallers);
      List<Future<ImmutableRoaringBitmap>> searches = new ArrayList<>(numCallers);
      for (int i = 0; i < numCallers; i++) {
        searches.add(callers.submit(() -> {
          startTogether.await(10, TimeUnit.SECONDS);
          return index.getDocIds(query, 1, bitmapOf(10));
        }));
      }
      index.awaitWinningRefresher();
      index.awaitWaitingCallers();
      index.releaseWinningRefresher();
      for (Future<ImmutableRoaringBitmap> search : searches) {
        Assert.assertEquals(search.get(10, TimeUnit.SECONDS).toArray(), new int[]{10});
      }
      Assert.assertEquals(index.getSearcherRefreshCount(), initialRefreshCount + 1,
          "Concurrent readers targeting one writer generation must share exactly one refresh");
      // The refresh count alone does not prove coalescing: a caller arriving after publication would skip refresh
      // and still leave the count at one. Require actual participation in the production waiter path.
      Assert.assertTrue(index.getObservedRefreshWaiters() > 0,
          "Expected concurrent callers to coalesce onto the in-flight refresh, but none entered the waiter path");
    } finally {
      index.releaseWinningRefresher();
      callers.shutdownNow();
      Assert.assertTrue(callers.awaitTermination(10, TimeUnit.SECONDS), "Concurrent callers did not terminate");
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

  /// A consuming segment spends its first seconds with nothing committed: the constructor commits an empty
  /// index and the next commit only fires on commitDocs/commitIntervalMs. The unfiltered path reads that
  /// committed view, which has no leaves and therefore no doc values, so it must return no results rather than
  /// fail while trying to translate them. The filtered path reads a near-real-time view and does see the rows,
  /// which is the difference that makes it usable for enforcing the query's visible-document set.
  @Test
  public void testSearchOnIndexWithNothingCommitted() {
    try (MutableVectorIndex index = createIndexWithoutCommits()) {
      float[] query = {1.0F, 0.0F, 0.0F, 0.0F, 0.0F};
      Assert.assertEquals(index.getDocIds(query, 2).toArray(), new int[0],
          "The committed view has no leaves yet, so an unfiltered search finds nothing");
      Assert.assertEquals(index.getDocIds(query, 2, bitmapOf(0, 1)).toArray(), new int[]{0, 1},
          "The near-real-time view must see rows that have not been committed yet");
    }
  }

  @Test
  public void testSearchOnEmptyIndexReturnsEmpty() {
    try (MutableVectorIndex index =
        new MutableVectorIndex("mutableVectorIndexTest_" + System.nanoTime(), COLUMN_NAME, createConfig())) {
      float[] query = {1.0F, 0.0F, 0.0F, 0.0F, 0.0F};
      Assert.assertEquals(index.getDocIds(query, 2).toArray(), new int[0]);
      Assert.assertEquals(index.getDocIds(query, 2, bitmapOf(0, 1)).toArray(), new int[0]);
    }
  }

  /// Lucene short-circuits to an exact scan when the accepted-doc count is at most topK, so a filtered test
  /// with |allowed| <= topK never walks the HNSW graph. This one keeps the allowed set larger than topK so the
  /// graph traversal itself is exercised against acceptDocs.
  @Test
  public void testFilteredApproximateSearchWithMoreAllowedDocsThanTopK() {
    try (MutableVectorIndex index = createIndex()) {
      for (int docId = 4; docId < 40; docId++) {
        addVector(index, new float[]{1.0F, docId, 0.0F, 0.0F, 0.0F}, docId);
      }
      MutableRoaringBitmap allowed = new MutableRoaringBitmap();
      for (int docId = 10; docId < 40; docId++) {
        allowed.add(docId);
      }

      int[] matches = index.getDocIds(new float[]{1.0F, 10.0F, 0.0F, 0.0F, 0.0F}, 3, allowed).toArray();
      Assert.assertEquals(matches.length, 3);
      for (int match : matches) {
        Assert.assertTrue(allowed.contains(match), "Filtered search returned a disallowed doc: " + match);
      }
    }
  }

  private static MutableRoaringBitmap bitmapOf(int... docIds) {
    MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
    bitmap.add(docIds);
    return bitmap;
  }

  private static boolean advancePhase(Phaser phaser)
      throws InterruptedException, TimeoutException {
    int phase = phaser.arrive();
    return phaser.awaitAdvanceInterruptibly(phase, 10, TimeUnit.SECONDS) >= 0;
  }

  /// Holds the winning refresher after it publishes `_searcherRefreshInProgress`, so at least one concurrent search
  /// is proven to enter the production waiter path before the refresh is allowed to finish.
  private static class CoordinatedRefreshMutableVectorIndex extends MutableVectorIndex {
    private final CountDownLatch _winningRefresherEntered = new CountDownLatch(1);
    private final CountDownLatch _releaseWinningRefresher = new CountDownLatch(1);
    private final AtomicInteger _observedRefreshWaiters = new AtomicInteger();
    private final CountDownLatch _waitingCallers;
    private volatile boolean _coordinateRefresh;

    CoordinatedRefreshMutableVectorIndex(String segmentName, VectorIndexConfig config, int expectedWaitingCallers) {
      super(segmentName, COLUMN_NAME, config);
      _waitingCallers = new CountDownLatch(expectedWaitingCallers);
    }

    void coordinateNextRefresh() {
      _coordinateRefresh = true;
    }

    void awaitWinningRefresher()
        throws InterruptedException, TimeoutException {
      if (!_winningRefresherEntered.await(10, TimeUnit.SECONDS)) {
        throw new TimeoutException("No filtered-search caller became the winning refresher");
      }
    }

    void awaitWaitingCallers()
        throws InterruptedException, TimeoutException {
      if (!_waitingCallers.await(10, TimeUnit.SECONDS)) {
        throw new TimeoutException("No concurrent caller entered the refresh waiter path");
      }
    }

    void releaseWinningRefresher() {
      _releaseWinningRefresher.countDown();
    }

    @Override
    void beforeSearcherRefresh()
        throws IOException {
      if (!_coordinateRefresh) {
        return;
      }
      _winningRefresherEntered.countDown();
      try {
        if (!_releaseWinningRefresher.await(10, TimeUnit.SECONDS)) {
          throw new IOException("Timed out waiting to release the winning refresher");
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("Interrupted while holding the winning refresher", e);
      } finally {
        _coordinateRefresh = false;
      }
    }

    @Override
    void onSearcherRefreshWait() {
      _observedRefreshWaiters.incrementAndGet();
      if (_coordinateRefresh) {
        _waitingCallers.countDown();
      }
    }

    int getObservedRefreshWaiters() {
      return _observedRefreshWaiters.get();
    }
  }

  /// Blocks the first filter membership check until the writer has added its concurrent rows. This puts the
  /// synchronization point inside Lucene's actual filtered search, rather than merely racing two caller threads.
  private static class ConcurrentWriteCoordinatingBitmap extends MutableRoaringBitmap {
    private final CountDownLatch _queryEnteredFilter;
    private final CountDownLatch _concurrentWritesFinished;

    ConcurrentWriteCoordinatingBitmap(int allowedDocId, CountDownLatch queryEnteredFilter,
        CountDownLatch concurrentWritesFinished) {
      add(allowedDocId);
      _queryEnteredFilter = queryEnteredFilter;
      _concurrentWritesFinished = concurrentWritesFinished;
    }

    @Override
    public boolean contains(int docId) {
      _queryEnteredFilter.countDown();
      try {
        if (!_concurrentWritesFinished.await(10, TimeUnit.SECONDS)) {
          throw new AssertionError("Concurrent writer did not finish while the filtered query was active");
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new AssertionError("Interrupted while coordinating the filtered query and writer", e);
      }
      return super.contains(docId);
    }
  }

  /// Config whose commit thresholds are high enough that no commit fires during the test.
  private static MutableVectorIndex createIndexWithoutCommits() {
    Map<String, String> properties = new HashMap<>();
    properties.put("commitDocs", String.valueOf(Integer.MAX_VALUE));
    properties.put("commitIntervalMs", String.valueOf(TimeUnit.DAYS.toMillis(1)));
    properties.put("vectorIndexType", "HNSW");
    properties.put("vectorDimension", "5");
    MutableVectorIndex index = new MutableVectorIndex("mutableVectorIndexTest_" + System.nanoTime(), COLUMN_NAME,
        new VectorIndexConfig(false, "HNSW", 5, 1, VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN, properties));
    addVector(index, new float[]{1.0F, 0.0F, 0.0F, 0.0F, 0.0F}, 0);
    addVector(index, new float[]{0.0F, 1.0F, 0.0F, 0.0F, 0.0F}, 1);
    return index;
  }

  private static CoordinatedRefreshMutableVectorIndex createCoordinatedIndexWithoutCommits(int expectedWaiters) {
    Map<String, String> properties = new HashMap<>();
    properties.put("commitDocs", String.valueOf(Integer.MAX_VALUE));
    properties.put("commitIntervalMs", String.valueOf(TimeUnit.DAYS.toMillis(1)));
    properties.put("vectorIndexType", "HNSW");
    properties.put("vectorDimension", "5");
    VectorIndexConfig config = new VectorIndexConfig(false, "HNSW", 5, 1,
        VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN, properties);
    CoordinatedRefreshMutableVectorIndex index = new CoordinatedRefreshMutableVectorIndex(
        "mutableVectorIndexCoalescingTest_" + System.nanoTime(), config, expectedWaiters);
    addVector(index, new float[]{1.0F, 0.0F, 0.0F, 0.0F, 0.0F}, 0);
    addVector(index, new float[]{0.0F, 1.0F, 0.0F, 0.0F, 0.0F}, 1);
    return index;
  }

  /// MutableIndex#add allows rows in arbitrary doc-id order, so freshness cannot be tracked by a doc-id
  /// watermark: a row added later with a lower doc id would be judged already visible and the refresh skipped,
  /// silently omitting it. Adds a high doc id, searches (refreshing through it), then adds a lower one.
  @Test
  public void testFilteredSearchSeesOutOfOrderUncommittedDoc() {
    try (MutableVectorIndex index = createIndexWithoutCommits()) {
      float[] query = {1.0F, 0.0F, 0.0F, 0.0F, 0.0F};
      addVector(index, new float[]{1.0F, 0.0F, 0.0F, 0.0F, 0.0F}, 10);
      Assert.assertEquals(index.getDocIds(query, 1, bitmapOf(10)).toArray(), new int[]{10},
          "The searcher must be refreshed through the row just added");

      addVector(index, new float[]{1.0F, 0.0F, 0.0F, 0.0F, 0.0F}, 5);
      Assert.assertEquals(index.getDocIds(query, 1, bitmapOf(5)).toArray(), new int[]{5},
          "A row added out of doc-id order must still be visible to a later filtered search");
    }
  }

  /// Hit translation walks doc values forward, so hits must be visited in Lucene doc-id order. Querying nearest
  /// to the last-added row makes score order the reverse of doc-id order, which exercises that sort.
  @Test
  public void testFilteredSearchWithHitsInDescendingScoreOrder() {
    try (MutableVectorIndex index = createIndexWithoutCommits()) {
      addVector(index, new float[]{0.0F, 0.0F, 0.0F, 0.0F, 1.0F}, 20);
      addVector(index, new float[]{0.0F, 0.0F, 0.0F, 1.0F, 0.0F}, 21);
      addVector(index, new float[]{0.0F, 0.0F, 1.0F, 0.0F, 0.0F}, 22);
      int[] matches = index.getDocIds(new float[]{0.0F, 0.0F, 1.0F, 0.0F, 0.0F}, 3,
          bitmapOf(20, 21, 22)).toArray();
      Assert.assertEquals(matches, new int[]{20, 21, 22},
          "Every allowed doc must translate to its supplied Pinot doc id regardless of score order");
    }
  }

  /// A null bitmap must not silently degrade into an unfiltered search, which would return doc ids the query is
  /// not allowed to see. The reader rejects it with its contract message rather than throwing a bare
  /// NullPointerException from deep inside Lucene.
  @Test
  public void testFilteredSearchRejectsNullBitmap() {
    try (MutableVectorIndex index = createIndexWithoutCommits()) {
      NullPointerException thrown = Assert.expectThrows(NullPointerException.class,
          () -> index.getDocIds(new float[]{1.0F, 0.0F, 0.0F, 0.0F, 0.0F}, 1, (ImmutableRoaringBitmap) null));
      Assert.assertNotNull(thrown.getMessage(), "The rejection must carry the pre-filter contract message");
      Assert.assertTrue(thrown.getMessage().contains("must not be null"),
          "Expected the pre-filter contract message, got: " + thrown.getMessage());
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
