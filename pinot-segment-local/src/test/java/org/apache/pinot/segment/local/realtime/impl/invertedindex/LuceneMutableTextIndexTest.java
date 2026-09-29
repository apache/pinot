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
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.segment.local.segment.index.text.TextIndexConfigBuilder;
import org.apache.pinot.segment.spi.index.TextIndexConfig;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.util.TestUtils;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


public class LuceneMutableTextIndexTest {
  private static final AtomicInteger SEGMENT_NAME_SUFFIX_COUNTER = new AtomicInteger(0);
  private static final File INDEX_DIR =
      new File(FileUtils.getTempDirectory(), "LuceneMutableIndexTest-" + UUID.randomUUID());
  private static final String TEXT_COLUMN_NAME = "testColumnName";
  private static final String CUSTOM_ANALYZER_FQCN = CustomAnalyzer.class.getName();
  private static final String CUSTOM_QUERY_PARSER_FQCN = CustomQueryParser.class.getName();
  private static final RealtimeLuceneTextIndexSearcherPool SEARCHER_POOL =
      RealtimeLuceneTextIndexSearcherPool.init(1);
  private RealtimeLuceneTextIndex _realtimeLuceneTextIndex;
  private QueryThreadContext _queryThreadContext;

  public LuceneMutableTextIndexTest() {
    RealtimeLuceneIndexRefreshManager.init(1, 10);
    ServerMetrics.register(mock(ServerMetrics.class));
  }

  @BeforeMethod
  public void setUpMethod() {
    // Give each test a fresh refresh queue and worker. Closing and immediately replacing indexes in the
    // same queue can race the worker's empty-list exit and leave the replacement without a refresher.
    RealtimeLuceneIndexRefreshManager.getInstance().reset();
    _queryThreadContext = QueryThreadContext.openForSseTest();
  }

  @AfterMethod(alwaysRun = true)
  public void tearDownMethod() {
    try {
      closeCurrentIndex();
    } finally {
      if (_queryThreadContext != null) {
        _queryThreadContext.close();
        _queryThreadContext = null;
      }
    }
  }

  @Test
  public void testDefaultAnalyzerAndDefaultQueryParser() {
    // Test queries with standard analyzer with default configurations used by Pinot
    configureIndex(null, null, null, null);
    assertEquals(_realtimeLuceneTextIndex.getDocIds("stream"), ImmutableRoaringBitmap.bitmapOf(0));
    assertEquals(_realtimeLuceneTextIndex.getDocIds("/.*house.*/"), ImmutableRoaringBitmap.bitmapOf(1));
    assertEquals(_realtimeLuceneTextIndex.getDocIds("invalid"), ImmutableRoaringBitmap.bitmapOf());
  }

  @Test
  public void testCustomAnalyzerWithNoArgsAndDefaultQueryParser() {
    // Test query with CustomKeywordAnalyzer without any args
    configureIndex(CUSTOM_ANALYZER_FQCN, null, null, null);
    assertEquals(_realtimeLuceneTextIndex.getDocIds(
            "/.*processing for data ware.*/"), ImmutableRoaringBitmap.bitmapOf(1));
    assertEquals(_realtimeLuceneTextIndex.getDocIds(
            "columnar processing for data warehouses"), ImmutableRoaringBitmap.bitmapOf(1));
  }

  @Test
  public void testCustomAnalyzerWithNoArgsAndCustomQueryParser() {
    // Test queries with CustomKeywordAnalyzer without any args and CustomQueryParser
    configureIndex(CUSTOM_ANALYZER_FQCN, null, null, CUSTOM_QUERY_PARSER_FQCN);
    assertEquals(_realtimeLuceneTextIndex.getDocIds(
            "/.*processing for data ware.*/"), ImmutableRoaringBitmap.bitmapOf(1));
    assertEquals(_realtimeLuceneTextIndex.getDocIds(
            "columnar processing for data warehouses"), ImmutableRoaringBitmap.bitmapOf(1));
  }

  @Test
  public void testCustomAnalyzerWithTwoStringArgsAndCustomQueryParser() {
    // Test queries with CustomKeywordAnalyzer with two java.lang.String args and CustomQueryParser
    configureIndex(CUSTOM_ANALYZER_FQCN,
            "a,b", "java.lang.String, java.lang.String", CUSTOM_QUERY_PARSER_FQCN);
    assertEquals(_realtimeLuceneTextIndex.getDocIds(
            "/.*processing for data ware.*/"), ImmutableRoaringBitmap.bitmapOf(1));
    assertEquals(_realtimeLuceneTextIndex.getDocIds(
            "columnar processing for data warehouses"), ImmutableRoaringBitmap.bitmapOf(1));
  }

  @Test
  public void testCustomAnalyzerWithOneStringOneIntegerParametersAndCustomQueryParser() {
    // Test queries with CustomKeywordAnalyzer w/ two String.class args and ExtendedQueryParser
    configureIndex(CUSTOM_ANALYZER_FQCN,
            "a,123", "java.lang.String,java.lang.Integer", CUSTOM_QUERY_PARSER_FQCN);
    assertEquals(_realtimeLuceneTextIndex.getDocIds(
            "/.*processing for data ware.*/"), ImmutableRoaringBitmap.bitmapOf(1));
    assertEquals(_realtimeLuceneTextIndex.getDocIds(
            "columnar processing for data warehouses"), ImmutableRoaringBitmap.bitmapOf(1));
  }

  @Test
  public void testCustomAnalyzerWithOnePrimitiveIntParametersAndCustomQueryParser() {
    // Test queries with CustomKeywordAnalyzer w/ two String.class args and ExtendedQueryParser
    configureIndex(CUSTOM_ANALYZER_FQCN,
            "123", "java.lang.Integer.TYPE", CUSTOM_QUERY_PARSER_FQCN);
    assertEquals(_realtimeLuceneTextIndex.getDocIds(
            "/.*processing for data ware.*/"), ImmutableRoaringBitmap.bitmapOf(1));
    assertEquals(_realtimeLuceneTextIndex.getDocIds(
            "columnar processing for data warehouses"), ImmutableRoaringBitmap.bitmapOf(1));
  }

  private static class CustomQueryParser extends QueryParser {
    public CustomQueryParser(String field, Analyzer analyzer) {
      super(field, analyzer);
    }
  }

  public static class CustomAnalyzer extends Analyzer {
    public CustomAnalyzer() {
      super();
    }

    public CustomAnalyzer(String stringArg1, String stringArg2) {
      super();
    }

    public CustomAnalyzer(String stringArg1, Integer integerArg2) {
      super();
    }

    public CustomAnalyzer(int intArg2) {
      super();
    }

    protected Analyzer.TokenStreamComponents createComponents(String fieldName) {
      return new Analyzer.TokenStreamComponents(new KeywordTokenizer());
    }
  }

  private String[][] getTextData() {
    return new String[][]{
        {"realtime stream processing"}, {"publish subscribe", "columnar processing for data warehouses", "concurrency"}
    };
  }

  private String[][] getRepeatedData() {
    return new String[][]{
        {"distributed storage", "multi-threading"}
    };
  }

  private void configureIndex(String analyzerClass, String analyzerClassArgs, String analyzerClassArgTypes,
                              String queryParserClass) {
    closeCurrentIndex();
    TextIndexConfigBuilder builder = new TextIndexConfigBuilder();
    if (null != analyzerClass) {
      builder.withLuceneAnalyzerClass(analyzerClass);
    }
    if (null != analyzerClassArgs) {
      builder.withLuceneAnalyzerClassArgs(analyzerClassArgs);
    }
    if (null != analyzerClassArgTypes) {
      builder.withLuceneAnalyzerClassArgTypes(analyzerClassArgTypes);
    }
    if (null != queryParserClass) {
      builder.withLuceneQueryParserClass(queryParserClass);
    }
    TextIndexConfig config = builder.withUseANDForMultiTermQueries(false).build();

    // Note that segment name must be unique on each query setup, otherwise `testQueryCancellationIsSuccessful` method
    // will cause unit test to fail due to inability to release a lock.
    _realtimeLuceneTextIndex = new RealtimeLuceneTextIndex(TEXT_COLUMN_NAME, INDEX_DIR,
        "table__0__1__20240601T1818Z" + SEGMENT_NAME_SUFFIX_COUNTER.getAndIncrement(), config);
    String[][] documents = getTextData();
    String[][] repeatedDocuments = getRepeatedData();

    for (String[] row : documents) {
      _realtimeLuceneTextIndex.add(row);
    }

    for (int i = 0; i < 1000; i++) {
      for (String[] row : repeatedDocuments) {
        _realtimeLuceneTextIndex.add(row);
      }
    }

    // ensure searches work after .commit() is called
    _realtimeLuceneTextIndex.commit();

    // Wait for the async NRT index refresh to make the committed documents searchable. A fixed
    // sleep is flaky under CPU load (the refresh thread may not run in time), so poll until a
    // sentinel query is visible, up to a generous timeout.
    //
    // The sentinel is the regex /.*house.*/ -> doc 1 ("...data warehouses"), which every test in
    // this class also asserts and which matches under both the default StandardAnalyzer and the
    // custom KeywordTokenizer (regex matches the single keyword-tokenized term). A term sentinel
    // like "stream" would never match the keyword-tokenized custom-analyzer cases, making the
    // barrier spin the full timeout for those tests.
    awaitIndexRefreshed("/.*house.*/", ImmutableRoaringBitmap.bitmapOf(1));
  }

  private void awaitIndexRefreshed(String sentinelQuery, ImmutableRoaringBitmap expected) {
    long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
    while (true) {
      if (expected.equals(_realtimeLuceneTextIndex.getDocIds(sentinelQuery))) {
        return;
      }
      if (System.nanoTime() >= deadlineNanos) {
        // Fall through and let the caller's assertions report the actual mismatch.
        return;
      }
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    RealtimeLuceneIndexRefreshManager.getInstance().reset();
  }

  @AfterClass
  public void tearDown() {
    try {
      closeCurrentIndex();
    } finally {
      FileUtils.deleteQuietly(INDEX_DIR);
    }
  }

  private void closeCurrentIndex() {
    if (_realtimeLuceneTextIndex != null) {
      _realtimeLuceneTextIndex.close();
      _realtimeLuceneTextIndex = null;
    }
  }

  @Test
  public void testGetSearchableDocCount()
      throws IOException {
    TextIndexConfig config = new TextIndexConfigBuilder().withUseANDForMultiTermQueries(false).build();
    RealtimeLuceneTextIndex index = new RealtimeLuceneTextIndex(TEXT_COLUMN_NAME, INDEX_DIR,
        "table__0__" + SEGMENT_NAME_SUFFIX_COUNTER.getAndIncrement() + "__20240601T1818Z", config);
    try {
      // Before any refresh, no docs are visible to the searcher
      assertEquals(index.getSearchableDocCount(), 0);

      index.add(new String[]{"hello world"});
      index.add(new String[]{"foo bar"});
      index.add(new String[]{"baz qux"});

      // Block until the refresh attempt completes so the listener has recorded the current doc count
      index.getSearcherManager().maybeRefreshBlocking();

      assertEquals(index.getSearchableDocCount(), 3);
    } finally {
      index.close();
    }
  }

  @Test
  public void testQueries() {
    configureIndex(null, null, null, null);
    TestUtils.waitForCondition(aVoid -> {
      try {
        return _realtimeLuceneTextIndex.getSearcherManager().isSearcherCurrent();
      } catch (IOException e) {
        return false;
      }
    }, 10000,
        "Background pool did not refresh the searcher manager in time");
    assertEquals(_realtimeLuceneTextIndex.getDocIds("stream"), ImmutableRoaringBitmap.bitmapOf(0));
    assertEquals(_realtimeLuceneTextIndex.getDocIds("/.*house.*/"), ImmutableRoaringBitmap.bitmapOf(1));
    assertEquals(_realtimeLuceneTextIndex.getDocIds("invalid"), ImmutableRoaringBitmap.bitmapOf());
  }

  @Test
  public void testQueryCancellationIsSuccessful()
      throws Exception {
    configureIndex(null, null, null, null);
    CountDownLatch searcherTaskStarted = new CountDownLatch(1);
    CountDownLatch releaseSearcherTask = new CountDownLatch(1);
    Future<?> searcherTask = SEARCHER_POOL.getExecutorService().submit(() -> {
      searcherTaskStarted.countDown();
      awaitUninterruptibly(releaseSearcherTask);
    });

    // Avoid early finalization by not using Executors.newSingleThreadExecutor (java <= 20, JDK-8145304)
    ExecutorService baseExecutor = Executors.newFixedThreadPool(1);
    // Wrap with contextAwareExecutorService to propagate QueryThreadContext to child threads
    ExecutorService executor = QueryThreadContext.contextAwareExecutorService(baseExecutor);
    CountDownLatch queryTaskStarted = new CountDownLatch(1);
    CountDownLatch runQuery = new CountDownLatch(1);

    try {
      assertTrue(searcherTaskStarted.await(10, TimeUnit.SECONDS), "Timed out waiting for searcher task to start");
      Future<MutableRoaringBitmap> result = executor.submit(() -> {
        queryTaskStarted.countDown();
        awaitUninterruptibly(runQuery);
        return _realtimeLuceneTextIndex.getDocIds("/.*read.*/");
      });
      assertTrue(queryTaskStarted.await(10, TimeUnit.SECONDS), "Timed out waiting for query task to start");

      // Interrupt the query worker before it submits to the saturated searcher pool, reproducing the CI race.
      baseExecutor.shutdownNow();
      runQuery.countDown();

      ExecutionException exception =
          expectThrows(ExecutionException.class, () -> result.get(10, TimeUnit.SECONDS));
      Throwable cause = exception.getCause();
      assertTrue(cause instanceof RuntimeException, "Expected RuntimeException but got: " + cause);
      assertTrue(cause.getMessage().contains("TEXT_MATCH query interrupted while querying the consuming segment"),
          "Unexpected exception: " + cause);
    } finally {
      runQuery.countDown();
      baseExecutor.shutdownNow();
      boolean queryExecutorTerminated = baseExecutor.awaitTermination(10, TimeUnit.SECONDS);

      // Queue a marker behind the nested Lucene search, then release the single searcher thread. Waiting for the
      // marker guarantees the search completed before tearDownMethod closes the index.
      Future<?> searcherDrain;
      try {
        searcherDrain = SEARCHER_POOL.getExecutorService().submit(() -> { });
      } finally {
        releaseSearcherTask.countDown();
      }
      searcherTask.get(10, TimeUnit.SECONDS);
      searcherDrain.get(10, TimeUnit.SECONDS);
      assertTrue(queryExecutorTerminated, "Query executor did not terminate");
    }
  }

  private static void awaitUninterruptibly(CountDownLatch latch) {
    boolean interrupted = false;
    while (true) {
      try {
        latch.await();
        break;
      } catch (InterruptedException e) {
        interrupted = true;
      }
    }
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
  }
}
