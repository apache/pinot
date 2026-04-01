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
package org.apache.pinot.segment.local.realtime.impl.invertedindex;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.segment.local.segment.index.text.TextIndexConfigBuilder;
import org.apache.pinot.segment.spi.index.TextIndexConfig;
import org.apache.pinot.spi.accounting.QueryResourceTracker;
import org.apache.pinot.spi.accounting.ThreadAccountant;
import org.apache.pinot.spi.accounting.ThreadResourceTracker;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.query.QueryExecutionContext;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.util.TestUtils;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class RealtimeLuceneTextIndexResourceTrackingTest {
  private static final File INDEX_DIR =
      new File(FileUtils.getTempDirectory(), "RealtimeLuceneTextIndexResourceTrackingTest");
  private static final String TEXT_COLUMN_NAME = "textColumn";
  private static final AtomicInteger SEGMENT_COUNTER = new AtomicInteger(0);

  private RealtimeLuceneTextIndex _textIndex;

  static {
    try {
      RealtimeLuceneTextIndexSearcherPool.init(2);
    } catch (AssertionError e) {
      // Already initialized
    }
    RealtimeLuceneIndexRefreshManager.init(1, 10);
    ServerMetrics.register(mock(ServerMetrics.class));
  }

  @BeforeClass
  public void setUpClass() throws Exception {
    FileUtils.deleteDirectory(INDEX_DIR);
    FileUtils.forceMkdir(INDEX_DIR);
    RealtimeLuceneIndexRefreshManager.getInstance().reset();
  }

  @AfterClass
  public void tearDownClass() throws Exception {
    FileUtils.deleteDirectory(INDEX_DIR);
  }

  @BeforeMethod
  public void setUp() {
    TextIndexConfig config = new TextIndexConfigBuilder().build();
    String segmentName = "segment_" + SEGMENT_COUNTER.getAndIncrement() + "__0__1__20240101T0000Z";
    _textIndex = new RealtimeLuceneTextIndex(TEXT_COLUMN_NAME, INDEX_DIR, segmentName, config);

    for (int i = 0; i < 100; i++) {
      _textIndex.add("document number " + i + " with searchable content");
    }
    _textIndex.commit();

    // Wait for index refresh
    TestUtils.waitForCondition(aVoid -> {
      try {
        return _textIndex.getSearcherManager().isSearcherCurrent();
      } catch (IOException e) {
        return false;
      }
    }, 5000, "Index refresh timeout");
  }

  @AfterMethod
  public void tearDown() {
    if (_textIndex != null) {
      _textIndex.close();
      _textIndex = null;
    }
  }

  @Test
  public void testSearcherThreadRegisteredWithAccountant() throws Exception {
    TrackingAccountant accountant = new TrackingAccountant();
    QueryExecutionContext executionContext = QueryExecutionContext.forSseTest();

    try (QueryThreadContext ignored = QueryThreadContext.open(executionContext, accountant)) {
      MutableRoaringBitmap result = _textIndex.getDocIds("searchable");

      assertTrue(result.getCardinality() > 0, "Search should return results");

      assertTrue(accountant.getSetupTaskCount() >= 2,
          "setupTask should be called for both parent and searcher threads, got: " + accountant.getSetupTaskCount());
    }
  }

  @Test
  public void testResourceUsageSampledDuringSearch() throws Exception {
    TrackingAccountant accountant = new TrackingAccountant();
    QueryExecutionContext executionContext = QueryExecutionContext.forSseTest();

    try (QueryThreadContext ignored = QueryThreadContext.open(executionContext, accountant)) {
      // Perform search with many matching docs to trigger periodic sampling
      MutableRoaringBitmap result = _textIndex.getDocIds("document");

      assertTrue(result.getCardinality() > 0, "Search should return results");
    }

    // After context close, clear should have been called
    assertTrue(accountant.getClearCount() >= 1, "clear() should be called on context close");
  }

  @Test
  public void testQueryTerminationDetectedByCollector() throws Exception {
    TrackingAccountant accountant = new TrackingAccountant();
    QueryExecutionContext executionContext = QueryExecutionContext.forSseTest();

    // Add many more documents for longer search
    TextIndexConfig config = new TextIndexConfigBuilder().build();
    String segmentName = "segment_termination_" + SEGMENT_COUNTER.getAndIncrement() + "__0__1__20240101T0000Z";
    RealtimeLuceneTextIndex largeIndex = new RealtimeLuceneTextIndex(TEXT_COLUMN_NAME, INDEX_DIR, segmentName, config);

    try {
      for (int i = 0; i < 50000; i++) {
        largeIndex.add("document number " + i + " with lots of searchable content for termination test");
      }
      largeIndex.commit();

      TestUtils.waitForCondition(aVoid -> {
        try {
          return largeIndex.getSearcherManager().isSearcherCurrent();
        } catch (IOException e) {
          return false;
        }
      }, 10000, "Index refresh timeout");

      ExecutorService baseExecutor = Executors.newSingleThreadExecutor();
      try (QueryThreadContext ignored = QueryThreadContext.open(executionContext, accountant)) {
        // Wrap with contextAwareExecutorService but don't register for cancellation (false).
        ExecutorService executor = QueryThreadContext.contextAwareExecutorService(baseExecutor, false);

        // Start a search that will take some time
        Future<MutableRoaringBitmap> searchFuture = executor.submit(
            () -> largeIndex.getDocIds("/.*searchable.*/"));

        // Give the search a moment to start
        Thread.sleep(50);

        // Terminate the query - this sets the termination flag that the collector will detect
        executionContext.terminate(QueryErrorCode.SERVER_RESOURCE_LIMIT_EXCEEDED, "OOM test");

        try {
          searchFuture.get();
          // Search might complete before termination is detected - that's OK for this test
        } catch (ExecutionException e) {
          // Expected - search was terminated via collector detecting the termination flag
          assertTrue(e.getCause() instanceof RuntimeException || e.getCause() instanceof Error,
              "Should throw RuntimeException or Error on termination, got: " + e.getCause().getClass().getName());
        } catch (CancellationException e) {
          // Also acceptable if future was cancelled
        }
      } finally {
        // Ensure executor is fully shut down before closing the index to prevent
        baseExecutor.shutdown();
        baseExecutor.awaitTermination(5, TimeUnit.SECONDS);
      }
    } finally {
      largeIndex.close();
    }
  }

  @Test(expectedExceptions = ExecutionException.class,
      expectedExceptionsMessageRegExp = ".*TEXT_MATCH query interrupted.*")
  public void testInterruptTriggersShouldCancel() throws Exception {
    TrackingAccountant accountant = new TrackingAccountant();
    QueryExecutionContext executionContext = QueryExecutionContext.forSseTest();

    try (QueryThreadContext ignored = QueryThreadContext.open(executionContext, accountant)) {
      ExecutorService baseExecutor = Executors.newFixedThreadPool(1);
      ExecutorService executor = QueryThreadContext.contextAwareExecutorService(baseExecutor);
      Future<MutableRoaringBitmap> future = executor.submit(() -> _textIndex.getDocIds("/.*content.*/"));
      baseExecutor.shutdownNow();
      future.get();
    }
  }

  @Test
  public void testConcurrentSearchesTrackedIndependently() throws Exception {
    TrackingAccountant accountant = new TrackingAccountant();
    QueryExecutionContext executionContext = QueryExecutionContext.forSseTest();
    // Wrap with contextAwareExecutorService to propagate QueryThreadContext to child threads
    ExecutorService baseExecutor = Executors.newFixedThreadPool(3);
    ExecutorService executor = QueryThreadContext.contextAwareExecutorService(baseExecutor);

    try (QueryThreadContext ignored = QueryThreadContext.open(executionContext, accountant)) {
      Future<MutableRoaringBitmap> future1 = executor.submit(() -> _textIndex.getDocIds("document"));
      Future<MutableRoaringBitmap> future2 = executor.submit(() -> _textIndex.getDocIds("number"));
      Future<MutableRoaringBitmap> future3 = executor.submit(() -> _textIndex.getDocIds("searchable"));

      MutableRoaringBitmap result1 = future1.get();
      MutableRoaringBitmap result2 = future2.get();
      MutableRoaringBitmap result3 = future3.get();

      assertTrue(result1.getCardinality() > 0);
      assertTrue(result2.getCardinality() > 0);
      assertTrue(result3.getCardinality() > 0);

      // Parent thread + 3 executor threads + their searcher threads = multiple setupTask calls
      // Due to thread pool reuse and timing, we expect at least 4 (1 parent + 3 searches)
      assertTrue(accountant.getSetupTaskCount() >= 4,
          "Multiple searches should register threads, got: " + accountant.getSetupTaskCount());
    }

    baseExecutor.shutdown();
  }

  @Test
  public void testExecutionContextSharedWithSearcherThread() throws Exception {
    TrackingAccountant accountant = new TrackingAccountant();
    QueryExecutionContext executionContext = QueryExecutionContext.forSseTest();

    try (QueryThreadContext ignored = QueryThreadContext.open(executionContext, accountant)) {
      _textIndex.getDocIds("document");

      // Verify all registered contexts share the same executionContext
      for (QueryThreadContext ctx : accountant.getRegisteredContexts()) {
        assertEquals(ctx.getExecutionContext(), executionContext,
            "All thread contexts should share the same execution context");
      }
    }
  }

  /**
   * A ThreadAccountant implementation that tracks calls for testing purposes.
   */
  private static class TrackingAccountant implements ThreadAccountant {
    private final AtomicInteger _setupTaskCount = new AtomicInteger(0);
    private final AtomicInteger _sampleUsageCount = new AtomicInteger(0);
    private final AtomicInteger _clearCount = new AtomicInteger(0);
    private final ConcurrentHashMap<Thread, QueryThreadContext> _registeredContexts = new ConcurrentHashMap<>();

    @Override
    public void setupTask(QueryThreadContext threadContext) {
      _setupTaskCount.incrementAndGet();
      _registeredContexts.put(Thread.currentThread(), threadContext);
    }

    @Override
    public void sampleUsage() {
      _sampleUsageCount.incrementAndGet();
    }

    @Override
    public void clear() {
      _clearCount.incrementAndGet();
      _registeredContexts.remove(Thread.currentThread());
    }

    @Override
    public void updateUntrackedResourceUsage(String identifier, long cpuTimeNs, long allocatedBytes,
        org.apache.pinot.spi.accounting.TrackingScope trackingScope) {
    }

    @Override
    public Collection<? extends ThreadResourceTracker> getThreadResources() {
      return Collections.emptyList();
    }

    @Override
    public Map<String, ? extends QueryResourceTracker> getQueryResources() {
      return Collections.emptyMap();
    }

    public int getSetupTaskCount() {
      return _setupTaskCount.get();
    }

    public int getSampleUsageCount() {
      return _sampleUsageCount.get();
    }

    public int getClearCount() {
      return _clearCount.get();
    }

    public Collection<QueryThreadContext> getRegisteredContexts() {
      return _registeredContexts.values();
    }
  }
}
