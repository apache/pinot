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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pinot.segment.local.realtime.impl.invertedindex.RealtimeLuceneTextIndexSearcherPool;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
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
      Assert.assertEquals(index.getSearcherRefreshWaitCount(), 0L,
          "An empty filter must return before search submission or entering the NRT wait path");
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
  public void testConcurrentFilteredSearchesShareOneReopen()
      throws Exception {
    // Exercises the generation handshake directly rather than through getDocIds. The searcher pool is a shared,
    // scaling singleton that serializes blocking searches in this JVM, so routing through it would only ever put
    // one caller in the wait path at a time and the assertion would measure the pool, not the sharing.
    //
    // A long refreshMinIntervalMs makes the overlap deterministic: the first waiter's reopen cannot start until
    // the limiter elapses, so both callers are guaranteed to be parked before it runs, and one reopen must serve
    // them both.
    int numCallers = 2;
    ExecutorService callers = Executors.newFixedThreadPool(numCallers);
    MutableVectorIndex index = createIndexWithRefreshTuning("1000");
    try (index) {
      float[] query = {1.0F, 0.0F, 0.0F, 0.0F, 0.0F};
      Assert.assertEquals(index.getDocIds(query, 1, bitmapOf(0)).toArray(), new int[]{0});
      long initialRefreshCount = index.getSearcherRefreshCount();
      long initialWaits = index.getSearcherRefreshWaitCount();
      addVector(index, query, 10);
      long target = index.getLastAddedSequenceNumber();

      CyclicBarrier startTogether = new CyclicBarrier(numCallers);
      List<Future<?>> waits = new ArrayList<>(numCallers);
      for (int i = 0; i < numCallers; i++) {
        waits.add(callers.submit(() -> {
          startTogether.await(10, TimeUnit.SECONDS);
          index.awaitSearcherGeneration(target);
          return null;
        }));
      }
      for (Future<?> wait : waits) {
        wait.get(30, TimeUnit.SECONDS);
      }
      Assert.assertEquals(index.getSearcherRefreshWaitCount() - initialWaits, numCallers,
          "Every caller must have blocked on the shared reopen rather than being served without waiting");
      Assert.assertEquals(index.getSearcherRefreshCount(), initialRefreshCount + 1,
          "Callers targeting one writer generation must cost exactly one reopen between them");
      // And the searcher really does cover the row, not merely claim the generation.
      Assert.assertEquals(index.getDocIds(query, 1, bitmapOf(10)).toArray(), new int[]{10});
    } finally {
      callers.shutdownNow();
      Assert.assertTrue(callers.awaitTermination(10, TimeUnit.SECONDS), "Concurrent callers did not terminate");
    }
  }

  /// The reason this path does not delegate to Lucene's ControlledRealTimeReopenThread: a reopen that threw must
  /// not publish the generation it merely attempted. Publishing it would let the next query search a searcher
  /// that does not hold the rows its filter names, and return fewer results with no error at all.
  @Test(timeOut = 60_000)
  public void testFailedReopenNeitherPublishesItsGenerationNorHangsTheQuery()
      throws Exception {
    AtomicBoolean failReopen = new AtomicBoolean(true);
    Map<String, String> properties = new HashMap<>();
    properties.put("commitDocs", String.valueOf(Integer.MAX_VALUE));
    properties.put("commitIntervalMs", String.valueOf(TimeUnit.DAYS.toMillis(1)));
    properties.put("vectorIndexType", "HNSW");
    properties.put("vectorDimension", "5");
    properties.put(MutableVectorIndex.REFRESH_MIN_INTERVAL_MS, "0");
    VectorIndexConfig config = new VectorIndexConfig(false, "HNSW", 5, 1,
        VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN, properties);
    try (MutableVectorIndex index =
        new MutableVectorIndex("mutableVectorIndexReopenFailureTest_" + System.nanoTime(), COLUMN_NAME, config) {
          @Override
          void doReopen()
              throws IOException {
            if (failReopen.get()) {
              throw new IOException("injected reopen failure");
            }
            super.doReopen();
          }
        }) {
      float[] query = {1.0F, 0.0F, 0.0F, 0.0F, 0.0F};
      addVector(index, query, 10);
      long generation = index.getLastAddedSequenceNumber();

      IOException thrown = Assert.expectThrows(IOException.class, () -> index.awaitSearcherGeneration(generation));
      Assert.assertTrue(thrown.getMessage().contains("reopen failed"),
          "A query must fail when the reopen it needs failed, got: " + thrown.getMessage());
      Assert.assertEquals(index.getSearcherRefreshCount(), 0L,
          "A reopen that threw must not be counted as having produced a searcher");
      // The decisive assertion: the failed attempt must not have advanced the published generation, or a later
      // query would take the fast path and search a searcher that never received the row.
      IOException second = Assert.expectThrows(IOException.class, () -> index.awaitSearcherGeneration(generation));
      Assert.assertNotNull(second.getMessage());

      // And a transient failure recovers on the next request rather than disabling the segment.
      failReopen.set(false);
      addVector(index, query, 11);
      Assert.assertEquals(index.getDocIds(query, 1, bitmapOf(11)).toArray(), new int[]{11});
    }
  }

  /// A reopen that never completes must surface as a failed query rather than an unbounded wait holding a thread
  /// of the shared searcher pool.
  @Test(timeOut = 60_000)
  public void testFilteredSearchTimesOutRatherThanWaitingForever()
      throws Exception {
    Map<String, String> properties = new HashMap<>();
    properties.put("commitDocs", String.valueOf(Integer.MAX_VALUE));
    properties.put("commitIntervalMs", String.valueOf(TimeUnit.DAYS.toMillis(1)));
    properties.put("vectorIndexType", "HNSW");
    properties.put("vectorDimension", "5");
    // The spacing outlives the timeout, so the reopen this query needs cannot start before it gives up.
    properties.put(MutableVectorIndex.REFRESH_MIN_INTERVAL_MS, String.valueOf(TimeUnit.SECONDS.toMillis(30)));
    properties.put(MutableVectorIndex.REFRESH_WAIT_TIMEOUT_MS, "50");
    VectorIndexConfig config = new VectorIndexConfig(false, "HNSW", 5, 1,
        VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN, properties);
    try (MutableVectorIndex index =
        new MutableVectorIndex("mutableVectorIndexTimeoutTest_" + System.nanoTime(), COLUMN_NAME, config)) {
      addVector(index, new float[]{1.0F, 0.0F, 0.0F, 0.0F, 0.0F}, 0);
      // Spend the first reopen, which is never delayed: only from the second on does the spacing apply, and it is
      // the spacing that keeps the awaited reopen from starting before the wait gives up.
      Assert.assertEquals(index.getDocIds(new float[]{1.0F, 0.0F, 0.0F, 0.0F, 0.0F}, 1, bitmapOf(0)).toArray(),
          new int[]{0});
      addVector(index, new float[]{1.0F, 0.0F, 0.0F, 0.0F, 0.0F}, 10);
      IOException thrown = Assert.expectThrows(IOException.class,
          () -> index.awaitSearcherGeneration(index.getLastAddedSequenceNumber()));
      Assert.assertTrue(thrown.getMessage().contains("Timed out after 50ms"),
          "Expected the bounded-wait failure, got: " + thrown.getMessage());
    }
  }

  /// The spacing must hold even while queries keep arriving. Each arriving query notifies the reopen thread, so a
  /// single timed wait would return on that notification and reopen early -- leaving the interval unenforced
  /// exactly under the load it exists to bound.
  @Test(timeOut = 60_000)
  public void testReopenSpacingHoldsWhileQueriesKeepArriving()
      throws Exception {
    long spacingMs = 1000L;
    try (MutableVectorIndex index = createIndexWithRefreshTuning(String.valueOf(spacingMs))) {
      float[] query = {1.0F, 0.0F, 0.0F, 0.0F, 0.0F};
      Assert.assertEquals(index.getDocIds(query, 1, bitmapOf(0)).toArray(), new int[]{0});
      long reopensAfterFirst = index.getSearcherRefreshCount();

      // Keep registering fresh requests for most of one spacing window; every one of them notifies the loop.
      long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(spacingMs / 2);
      int docId = 100;
      while (System.nanoTime() < deadline) {
        addVector(index, query, docId++);
        long generation = index.getLastAddedSequenceNumber();
        Thread nudge = new Thread(() -> {
          try {
            index.awaitSearcherGeneration(generation);
          } catch (IOException e) {
            // The wait may outlive the window; the notification it sent is what this test is about.
          }
        });
        nudge.setDaemon(true);
        nudge.start();
        Thread.sleep(20);
      }
      Assert.assertEquals(index.getSearcherRefreshCount(), reopensAfterFirst,
          "No reopen may run inside the spacing window, however many queries notify the loop");
    }
  }

  /// A query interrupted while waiting must surface that as a failure carrying the cause, not return as though
  /// the generation had arrived.
  @Test(timeOut = 60_000)
  public void testInterruptedWaitFailsRatherThanReturningEarly()
      throws Exception {
    try (MutableVectorIndex index = createIndexWithRefreshTuning(String.valueOf(TimeUnit.SECONDS.toMillis(30)))) {
      float[] query = {1.0F, 0.0F, 0.0F, 0.0F, 0.0F};
      Assert.assertEquals(index.getDocIds(query, 1, bitmapOf(0)).toArray(), new int[]{0});
      addVector(index, query, 10);
      long generation = index.getLastAddedSequenceNumber();

      AtomicReference<Throwable> failure = new AtomicReference<>();
      CountDownLatch waiting = new CountDownLatch(1);
      Thread waiter = new Thread(() -> {
        waiting.countDown();
        try {
          index.awaitSearcherGeneration(generation);
          failure.set(new AssertionError("Interrupted wait returned normally"));
        } catch (Throwable t) {
          failure.set(t);
        }
      });
      waiter.setDaemon(true);
      waiter.start();
      Assert.assertTrue(waiting.await(10, TimeUnit.SECONDS), "Waiter did not start");
      awaitWaitCount(index, 2);
      waiter.interrupt();
      waiter.join(TimeUnit.SECONDS.toMillis(10));

      Assert.assertTrue(failure.get() instanceof IOException,
          "An interrupted wait must fail, got: " + failure.get());
      Assert.assertTrue(failure.get().getMessage().contains("Interrupted while waiting"),
          "Expected the interrupt failure, got: " + failure.get().getMessage());
    }
  }

  /// One reopen thread exists per consuming segment per vector column, so a close that failed to stop it would
  /// leak a thread per partition -- invisible at runtime because the thread is a daemon.
  @Test(timeOut = 60_000)
  public void testCloseStopsTheReopenThread()
      throws Exception {
    String segmentName = "mutableVectorIndexCloseTest_" + System.nanoTime();
    String threadName = "vector-nrt-reopen-" + segmentName + "-" + COLUMN_NAME;
    Map<String, String> properties = new HashMap<>();
    properties.put("vectorIndexType", "HNSW");
    properties.put("vectorDimension", "5");
    MutableVectorIndex index = new MutableVectorIndex(segmentName, COLUMN_NAME,
        new VectorIndexConfig(false, "HNSW", 5, 1, VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN, properties));
    Assert.assertTrue(hasLiveThread(threadName), "The reopen thread must run while the index is open");
    index.close();
    Assert.assertFalse(hasLiveThread(threadName),
        "close() must stop the reopen thread, or every consuming segment leaks one");
  }

  /// A query must never be told a generation is available when the reopen that would have produced it failed:
  /// answering from the stale searcher would silently drop rows the filter names. Closing the index mid-wait is
  /// the reachable version of that -- the awaited generation can no longer arrive, so the query must fail.
  @Test(timeOut = 60_000)
  public void testFilteredSearchFailsWhenTheAwaitedGenerationCannotArrive()
      throws Exception {
    MutableVectorIndex index = createIndexWithRefreshTuning(String.valueOf(TimeUnit.SECONDS.toMillis(5)));
    float[] query = {1.0F, 0.0F, 0.0F, 0.0F, 0.0F};
    Assert.assertEquals(index.getDocIds(query, 1, bitmapOf(0)).toArray(), new int[]{0});
    addVector(index, query, 10);
    ExecutorService caller = Executors.newSingleThreadExecutor();
    try {
      // The 30s limiter keeps the reopen pending, so this call is parked in the wait path when close() lands.
      Future<ImmutableRoaringBitmap> search = caller.submit(() -> index.getDocIds(query, 1, bitmapOf(10)));
      awaitWaitCount(index, 2);
      index.close();
      ExecutionException thrown = Assert.expectThrows(ExecutionException.class,
          () -> search.get(30, TimeUnit.SECONDS));
      Assert.assertTrue(hasCauseContaining(thrown, "closed while waiting for the searcher to reopen"),
          "A query whose generation can never arrive must fail for that reason: " + thrown.getCause());
    } finally {
      caller.shutdownNow();
      Assert.assertTrue(caller.awaitTermination(10, TimeUnit.SECONDS), "Caller did not terminate");
    }
  }

  @Test(dataProvider = "invalidRefreshTuning")
  public void testRejectsInvalidRefreshTuning(String key, String value) {
    Map<String, String> properties = new HashMap<>();
    properties.put("vectorIndexType", "HNSW");
    properties.put("vectorDimension", "5");
    properties.put(key, value);
    VectorIndexConfig config = new VectorIndexConfig(false, "HNSW", 5, 1,
        VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN, properties);
    Assert.expectThrows(IllegalArgumentException.class,
        () -> new MutableVectorIndex("mutableVectorIndexConfigTest_" + System.nanoTime(), COLUMN_NAME, config));
  }

  @DataProvider(name = "invalidRefreshTuning")
  public Object[][] invalidRefreshTuning() {
    return new Object[][]{
        {MutableVectorIndex.REFRESH_MIN_INTERVAL_MS, "-1"},
        {MutableVectorIndex.REFRESH_MIN_INTERVAL_MS, "abc"},
        {MutableVectorIndex.REFRESH_WAIT_TIMEOUT_MS, "0"},
        {MutableVectorIndex.REFRESH_WAIT_TIMEOUT_MS, "-5"}
    };
  }

  private static boolean hasCauseContaining(Throwable thrown, String fragment) {
    for (Throwable cause = thrown; cause != null; cause = cause.getCause()) {
      if (cause.getMessage() != null && cause.getMessage().contains(fragment)) {
        return true;
      }
    }
    return false;
  }

  private static boolean hasLiveThread(String threadName) {
    return Thread.getAllStackTraces().keySet().stream()
        .anyMatch(thread -> thread.isAlive() && threadName.equals(thread.getName()));
  }

  private static void awaitWaitCount(MutableVectorIndex index, long expected)
      throws InterruptedException, TimeoutException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (index.getSearcherRefreshWaitCount() < expected) {
      if (System.nanoTime() > deadline) {
        throw new TimeoutException("Only " + index.getSearcherRefreshWaitCount() + " of " + expected + " waits seen");
      }
      Thread.sleep(5);
    }
  }

  /// Queries already covered by the searcher's current generation must neither reopen nor enter the wait path.
  /// This guards the regression where a filtered query drives a reopen unconditionally, which is what makes the
  /// per-query flush cost unbounded. It does not on its own distinguish publishing the true generation reached
  /// from the previous per-caller watermark: with no rows added between these queries, both skip the refresh.
  @Test
  public void testFilteredSearchesAfterOneReopenDoNotRefreshAgain()
      throws Exception {
    try (MutableVectorIndex index = createIndexWithRefreshTuning()) {
      float[] query = {1.0F, 0.0F, 0.0F, 0.0F, 0.0F};
      Assert.assertEquals(index.getDocIds(query, 1, bitmapOf(0)).toArray(), new int[]{0});
      addVector(index, query, 10);

      Assert.assertEquals(index.getDocIds(query, 1, bitmapOf(10)).toArray(), new int[]{10});
      long refreshesAfterFirstSearch = index.getSearcherRefreshCount();
      long waitersAfterFirstSearch = index.getSearcherRefreshWaitCount();

      for (int i = 0; i < 20; i++) {
        Assert.assertEquals(index.getDocIds(query, 1, bitmapOf(10)).toArray(), new int[]{10});
      }
      Assert.assertEquals(index.getSearcherRefreshCount(), refreshesAfterFirstSearch,
          "Queries already covered by the published generation must not trigger another reopen");
      Assert.assertEquals(index.getSearcherRefreshWaitCount(), waitersAfterFirstSearch,
          "Queries already covered by the published generation must not even enter the waiter path");
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

  /// An index whose reopen cadence is pinned for assertions: no commits, no idle reopen inside the test
  /// window, and a waiting query allowed to trigger its reopen immediately.
  private static MutableVectorIndex createIndexWithRefreshTuning() {
    return createIndexWithRefreshTuning("0");
  }

  private static MutableVectorIndex createIndexWithRefreshTuning(String refreshMinIntervalMs) {
    Map<String, String> properties = new HashMap<>();
    properties.put("commitDocs", String.valueOf(Integer.MAX_VALUE));
    properties.put("commitIntervalMs", String.valueOf(TimeUnit.DAYS.toMillis(1)));
    properties.put("vectorIndexType", "HNSW");
    properties.put("vectorDimension", "5");
    // No rate limiting, so a waiting query's reopen starts immediately and the test never sits on the limiter.
    properties.put(MutableVectorIndex.REFRESH_MIN_INTERVAL_MS, refreshMinIntervalMs);
    VectorIndexConfig config = new VectorIndexConfig(false, "HNSW", 5, 1,
        VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN, properties);
    MutableVectorIndex index =
        new MutableVectorIndex("mutableVectorIndexCoalescingTest_" + System.nanoTime(), COLUMN_NAME, config);
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
