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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.segment.local.segment.index.text.TextIndexConfigBuilder;
import org.apache.pinot.segment.spi.index.TextIndexConfig;
import org.apache.pinot.util.TestUtils;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;


public class LuceneMutableTextIndexTest {
  private static final AtomicInteger SEGMENT_NAME_SUFFIX_COUNTER = new AtomicInteger(0);
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "LuceneMutableIndexTest");
  private static final String TEXT_COLUMN_NAME = "testColumnName";
  private static final String CUSTOM_ANALYZER_FQCN = CustomAnalyzer.class.getName();
  private static final String CUSTOM_QUERY_PARSER_FQCN = CustomQueryParser.class.getName();
  private static final RealtimeLuceneTextIndexSearcherPool SEARCHER_POOL =
      RealtimeLuceneTextIndexSearcherPool.init(1);
  private RealtimeLuceneTextIndex _realtimeLuceneTextIndex;

  public LuceneMutableTextIndexTest() {
    RealtimeLuceneIndexRefreshManager.init(1, 10);
    ServerMetrics.register(mock(ServerMetrics.class));
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

    // sleep for index refresh
    try {
      Thread.sleep(100);
    } catch (Exception e) {
      // no-op
    }
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    RealtimeLuceneIndexRefreshManager.getInstance().reset();
  }

  @AfterClass
  public void tearDown() {
    _realtimeLuceneTextIndex.close();
  }

  @Test
  public void testQueries() {
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

  @Test(expectedExceptions = ExecutionException.class,
      expectedExceptionsMessageRegExp = ".*TEXT_MATCH query interrupted while querying the consuming segment.*")
  public void testQueryCancellationIsSuccessful()
      throws InterruptedException, ExecutionException {
    // Avoid early finalization by not using Executors.newSingleThreadExecutor (java <= 20, JDK-8145304)
    ExecutorService executor = Executors.newFixedThreadPool(1);
    Future<MutableRoaringBitmap> res = executor.submit(() -> _realtimeLuceneTextIndex.getDocIds("/.*read.*/"));
    executor.shutdownNow();
    res.get();
  }
}
