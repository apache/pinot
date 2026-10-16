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
package org.apache.pinot.segment.local.segment.index.readers.text;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.search.Query;
import org.apache.pinot.segment.local.segment.creator.impl.text.LuceneTextIndexCreator;
import org.apache.pinot.segment.local.segment.index.text.TextIndexConfigBuilder;
import org.apache.pinot.segment.spi.index.TextIndexConfig;
import org.apache.pinot.segment.spi.index.reader.MultiColumnTextIndexReader;
import org.apache.pinot.segment.spi.index.reader.QueryScopedCache;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


/// Covers the query-scoped reuse of parsed queries in [LuceneTextIndexReader].
public class LuceneTextIndexQueryCacheTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(),
      LuceneTextIndexQueryCacheTest.class.getSimpleName() + "-" + UUID.randomUUID());
  private static final File INDEX_DIR = new File(TEMP_DIR, "index");
  private static final String COLUMN = "querycachebody";

  private static final String[] DOCS = {
      "failed to place order for user alice",
      "failed to charge card for user bob",
      "connection refused by payment service",
      "order confirmation email sent to carol"
  };

  /// Stands in for `QueryContext`, which lives in pinot-core and is not visible here. It uses the same
  /// `computeIfAbsent` contract, and counts how many times a key is actually computed so a test can
  /// assert that a parse happened once rather than once per caller.
  private static final class CountingCache implements QueryScopedCache {
    private final ConcurrentHashMap<Object, Object> _values = new ConcurrentHashMap<>();
    private final AtomicInteger _computeCount = new AtomicInteger();

    @SuppressWarnings("unchecked")
    @Override
    public <K, V> V getOrComputeSharedValue(Class<V> type, K key, Function<K, V> mapper) {
      return (V) _values.computeIfAbsent(key, k -> {
        _computeCount.incrementAndGet();
        return mapper.apply((K) k);
      });
    }

    int computeCount() {
      return _computeCount.get();
    }
  }

  @BeforeClass
  public void setUp()
      throws IOException {
    FileUtils.forceMkdir(INDEX_DIR);
    TextIndexConfig config = new TextIndexConfigBuilder().build();
    try (LuceneTextIndexCreator creator = new LuceneTextIndexCreator(COLUMN, INDEX_DIR, true, false, null, null,
        config)) {
      for (String doc : DOCS) {
        creator.add(doc);
      }
      creator.seal();
    }
  }

  @AfterClass
  public void tearDown() {
    FileUtils.deleteQuietly(TEMP_DIR);
  }

  private static TextIndexConfig configWithProperties(Map<String, String> properties) {
    return (TextIndexConfig) new TextIndexConfigBuilder().withProperties(properties).build();
  }

  private static TextIndexConfig configWithStopWords(List<String> stopWordsInclude) {
    TextIndexConfig.AbstractBuilder builder = new TextIndexConfigBuilder();
    builder.withStopWordsInclude(stopWordsInclude);
    return (TextIndexConfig) builder.build();
  }

  private LuceneTextIndexReader reader(TextIndexConfig config) {
    return new LuceneTextIndexReader(COLUMN, INDEX_DIR, DOCS.length, config);
  }

  @Test
  public void testParseIsSharedWithinOneQuery()
      throws Exception {
    CountingCache cache = new CountingCache();
    try (LuceneTextIndexReader first = reader(configWithProperties(Map.of()));
        LuceneTextIndexReader second = reader(configWithProperties(Map.of()))) {
      Query a = first.buildQuery("order", null, cache);
      Query b = second.buildQuery("order", null, cache);
      assertSame(b, a, "equivalent readers in one query should share the parsed query");
      assertEquals(cache.computeCount(), 1, "the query should have been parsed exactly once");
      assertNotSame(first.parseQuery("order", null), a, "parseQuery must bypass the cache");
    }
  }

  @Test
  public void testNoCacheMeansNoSharing()
      throws Exception {
    try (LuceneTextIndexReader reader = reader(configWithProperties(Map.of()))) {
      Query a = reader.buildQuery("order", null, null);
      Query b = reader.buildQuery("order", null, null);
      assertNotSame(b, a);
      assertEquals(b, a);
      assertEquals(reader.getNumMatchingDocs("order", null, null), 2);
    }
  }

  /// Regression test for a flattened cache key. `["failed", "order"]` and `["failed, order"]` are
  /// different stop-word sets -- the first makes `failed` a stop word, the second does not -- yet both
  /// render as `[failed, order]`. A key built from rendered text would let the second configuration
  /// reuse the first's empty query and silently lose matches.
  @Test
  public void testStopWordListsThatRenderIdenticallyDoNotCollide()
      throws Exception {
    CountingCache cache = new CountingCache();
    try (LuceneTextIndexReader twoStopWords = reader(configWithStopWords(List.of("failed", "order")));
        LuceneTextIndexReader oneStopWord = reader(configWithStopWords(List.of("failed, order")))) {
      Query withFailedAsStopWord = twoStopWords.buildQuery("failed", null, cache);
      Query withFailedAsTerm = oneStopWord.buildQuery("failed", null, cache);
      assertNotEquals(withFailedAsTerm, withFailedAsStopWord,
          "configurations whose stop-word lists only render the same must not share a cache entry");
      assertEquals(cache.computeCount(), 2, "each configuration must be parsed on its own");
      assertEquals(withFailedAsStopWord, twoStopWords.parseQuery("failed", null));
      assertEquals(withFailedAsTerm, oneStopWord.parseQuery("failed", null));
    }
  }

  @Test
  public void testMultiTermConfigDoesNotCollide()
      throws Exception {
    CountingCache cache = new CountingCache();
    try (LuceneTextIndexReader orReader = reader(
        configWithProperties(Map.of(FieldConfig.TEXT_INDEX_USE_AND_FOR_MULTI_TERM_QUERIES, "false")));
        LuceneTextIndexReader andReader = reader(
            configWithProperties(Map.of(FieldConfig.TEXT_INDEX_USE_AND_FOR_MULTI_TERM_QUERIES, "true")))) {
      assertNotEquals(andReader.buildQuery("failed order", null, cache),
          orReader.buildQuery("failed order", null, cache));
      assertEquals(orReader.getNumMatchingDocs("failed order", null, cache), 3);
      assertEquals(andReader.getNumMatchingDocs("failed order", null, cache), 1);
    }
  }

  @Test
  public void testOptionsStringIsPartOfTheKey()
      throws Exception {
    CountingCache cache = new CountingCache();
    try (LuceneTextIndexReader reader = reader(configWithProperties(Map.of()))) {
      assertNotEquals(reader.buildQuery("failed order", "parser=MATCH,operator=AND", cache),
          reader.buildQuery("failed order", null, cache));
      assertEquals(cache.computeCount(), 2);
    }
  }

  /// A cold query hit by every segment thread at once must still be parsed once, not once per thread.
  @Test
  public void testConcurrentMissesParseOnce()
      throws Exception {
    int threads = 16;
    CountingCache cache = new CountingCache();
    ExecutorService executor = Executors.newFixedThreadPool(threads);
    try {
      // Opening a reader is far more expensive than the parse under test. Releasing the start gate
      // straight after submission would let an early worker reach the cache while others were still
      // in their constructor, so the test could pass without ever racing. Gate on readiness instead,
      // so every worker is one call away from the cache when the gate opens.
      CountDownLatch ready = new CountDownLatch(threads);
      CountDownLatch start = new CountDownLatch(1);
      Future<Query>[] futures = new Future[threads];
      for (int i = 0; i < threads; i++) {
        futures[i] = executor.submit(() -> {
          try (LuceneTextIndexReader reader = reader(configWithProperties(Map.of()))) {
            ready.countDown();
            start.await();
            return reader.buildQuery("connection", null, cache);
          }
        });
      }
      assertTrue(ready.await(60, TimeUnit.SECONDS), "all workers should open their readers");
      start.countDown();
      Query first = futures[0].get(60, TimeUnit.SECONDS);
      for (Future<Query> future : futures) {
        assertSame(future.get(60, TimeUnit.SECONDS), first, "all threads should observe one parsed query");
      }
      assertEquals(cache.computeCount(), 1, "a cold key must be parsed once, not once per thread");
    } finally {
      executor.shutdownNow();
    }
  }

  /// Guards overload resolution for [MultiColumnTextIndexReader].
  ///
  /// Cast null options to String to select the column-aware overloads instead of the overloads
  /// ending in a query-scoped cache.
  @Test
  public void testMultiColumnNullOptionsWithExplicitCast() {
    MultiColumnTextIndexReader reader = multiColumnReaderReturning(new MutableRoaringBitmap());
    assertEquals(reader.getDocIds("body", "error", (String) null).getCardinality(), 0);
    assertEquals(reader.getNumMatchingDocs("body", "error", (String) null), 0);
  }

  private static MultiColumnTextIndexReader multiColumnReaderReturning(MutableRoaringBitmap docIds) {
    return new MultiColumnTextIndexReader() {
      @Override
      public MutableRoaringBitmap getDocIds(String column, String searchQuery, String optionsString) {
        return docIds;
      }

      @Override
      public MutableRoaringBitmap getDocIds(String searchQuery) {
        return docIds;
      }

      @Override
      public ImmutableRoaringBitmap getDictIds(String searchQuery) {
        throw new UnsupportedOperationException();
      }

      @Override
      public void close() {
      }
    };
  }
}
