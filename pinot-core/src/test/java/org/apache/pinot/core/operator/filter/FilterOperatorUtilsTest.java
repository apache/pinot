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
package org.apache.pinot.core.operator.filter;

import com.google.common.collect.Lists;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.OptionalInt;
import java.nio.ByteOrder;
import java.nio.file.Files;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.common.request.context.predicate.TextMatchPredicate;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.FilterBlock;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.local.segment.creator.impl.inv.RawValueBitmapInvertedIndexCreator;
import org.apache.pinot.segment.local.segment.index.readers.RawValueBitmapInvertedIndexReader;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.index.reader.InvertedIndexReader;
import org.apache.pinot.segment.spi.index.reader.TextIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class FilterOperatorUtilsTest {
  private static final QueryContext QUERY_CONTEXT = mock(QueryContext.class);
  private static final int NUM_DOCS = 10;
  private static final BaseFilterOperator EMPTY_FILTER_OPERATOR = EmptyFilterOperator.getInstance();
  private static final BaseFilterOperator MATCH_ALL_FILTER_OPERATOR = new MatchAllFilterOperator(NUM_DOCS);
  private static final BaseFilterOperator REGULAR_FILTER_OPERATOR =
      new TestFilterOperator(new int[]{1, 4, 7}, NUM_DOCS);

  @Test
  public void testGetAndFilterOperator() {
    BaseFilterOperator filterOperator =
        FilterOperatorUtils.getAndFilterOperator(QUERY_CONTEXT, Collections.emptyList(), NUM_DOCS);
    assertTrue(filterOperator instanceof MatchAllFilterOperator);

    filterOperator =
        FilterOperatorUtils.getAndFilterOperator(QUERY_CONTEXT, Collections.singletonList(EMPTY_FILTER_OPERATOR),
            NUM_DOCS);
    assertTrue(filterOperator instanceof EmptyFilterOperator);

    filterOperator =
        FilterOperatorUtils.getAndFilterOperator(QUERY_CONTEXT, Collections.singletonList(MATCH_ALL_FILTER_OPERATOR),
            NUM_DOCS);
    assertTrue(filterOperator instanceof MatchAllFilterOperator);

    filterOperator =
        FilterOperatorUtils.getAndFilterOperator(QUERY_CONTEXT, Collections.singletonList(REGULAR_FILTER_OPERATOR),
            NUM_DOCS);
    assertTrue(filterOperator instanceof TestFilterOperator);

    filterOperator = FilterOperatorUtils.getAndFilterOperator(QUERY_CONTEXT,
        Arrays.asList(EMPTY_FILTER_OPERATOR, MATCH_ALL_FILTER_OPERATOR), NUM_DOCS);
    assertTrue(filterOperator instanceof EmptyFilterOperator);

    filterOperator = FilterOperatorUtils.getAndFilterOperator(QUERY_CONTEXT,
        Arrays.asList(EMPTY_FILTER_OPERATOR, REGULAR_FILTER_OPERATOR), NUM_DOCS);
    assertTrue(filterOperator instanceof EmptyFilterOperator);

    filterOperator = FilterOperatorUtils.getAndFilterOperator(QUERY_CONTEXT,
        Arrays.asList(MATCH_ALL_FILTER_OPERATOR, REGULAR_FILTER_OPERATOR), NUM_DOCS);
    assertTrue(filterOperator instanceof TestFilterOperator);
  }

  @Test
  public void testGetOrFilterOperator() {
    BaseFilterOperator filterOperator =
        FilterOperatorUtils.getOrFilterOperator(QUERY_CONTEXT, Collections.emptyList(), NUM_DOCS);
    assertTrue(filterOperator instanceof EmptyFilterOperator);

    filterOperator =
        FilterOperatorUtils.getOrFilterOperator(QUERY_CONTEXT, Collections.singletonList(EMPTY_FILTER_OPERATOR),
            NUM_DOCS);
    assertTrue(filterOperator instanceof EmptyFilterOperator);

    filterOperator =
        FilterOperatorUtils.getOrFilterOperator(QUERY_CONTEXT, Collections.singletonList(MATCH_ALL_FILTER_OPERATOR),
            NUM_DOCS);
    assertTrue(filterOperator instanceof MatchAllFilterOperator);

    filterOperator =
        FilterOperatorUtils.getOrFilterOperator(QUERY_CONTEXT, Collections.singletonList(REGULAR_FILTER_OPERATOR),
            NUM_DOCS);
    assertTrue(filterOperator instanceof TestFilterOperator);

    filterOperator = FilterOperatorUtils.getOrFilterOperator(QUERY_CONTEXT,
        Arrays.asList(EMPTY_FILTER_OPERATOR, MATCH_ALL_FILTER_OPERATOR), NUM_DOCS);
    assertTrue(filterOperator instanceof MatchAllFilterOperator);

    filterOperator = FilterOperatorUtils.getOrFilterOperator(QUERY_CONTEXT,
        Arrays.asList(EMPTY_FILTER_OPERATOR, REGULAR_FILTER_OPERATOR), NUM_DOCS);
    assertTrue(filterOperator instanceof TestFilterOperator);

    filterOperator = FilterOperatorUtils.getOrFilterOperator(QUERY_CONTEXT,
        Arrays.asList(MATCH_ALL_FILTER_OPERATOR, REGULAR_FILTER_OPERATOR), NUM_DOCS);
    assertTrue(filterOperator instanceof MatchAllFilterOperator);
  }

  @Test
  public void testLeafFilterOperatorUsesRawValueBitmapInvertedIndexFallback()
      throws Exception {
    QueryContext queryContext = mock(QueryContext.class);
    when(queryContext.isNullHandlingEnabled()).thenReturn(false);
    when(queryContext.isIndexUseAllowed(any(DataSource.class), eq(FieldConfig.IndexType.INVERTED))).thenReturn(true);

    DataSource dataSource = mock(DataSource.class);
    DataSourceMetadata dataSourceMetadata = mock(DataSourceMetadata.class);
    when(dataSourceMetadata.isSorted()).thenReturn(false);
    when(dataSourceMetadata.getDataType()).thenReturn(DataType.INT);
    when(dataSourceMetadata.isSingleValue()).thenReturn(true);
    when(dataSource.getDataSourceMetadata()).thenReturn(dataSourceMetadata);

    PredicateEvaluator predicateEvaluator = mock(PredicateEvaluator.class);
    when(predicateEvaluator.isAlwaysFalse()).thenReturn(false);
    when(predicateEvaluator.isAlwaysTrue()).thenReturn(false);
    when(predicateEvaluator.getPredicateType()).thenReturn(Predicate.Type.EQ);
    when(predicateEvaluator.isDictionaryBased()).thenReturn(false);
    when(predicateEvaluator.isExclusive()).thenReturn(false);

    try (RawValueInvertedIndexFixture fixture = createRawValueBitmapInvertedIndexReader()) {
      when(dataSource.getInvertedIndex()).thenReturn((InvertedIndexReader) fixture._reader);
      BaseFilterOperator filterOperator =
          FilterOperatorUtils.getLeafFilterOperator(queryContext, predicateEvaluator, dataSource, NUM_DOCS);
      assertTrue(filterOperator instanceof RawValueInvertedIndexFilterOperator);
    }
  }

  @Test
  public void testLeafFilterOperatorUsesStandardInvertedIndexWhenDictionaryBased()
      throws Exception {
    QueryContext queryContext = mock(QueryContext.class);
    when(queryContext.isNullHandlingEnabled()).thenReturn(false);
    when(queryContext.isIndexUseAllowed(any(DataSource.class), eq(FieldConfig.IndexType.INVERTED))).thenReturn(true);

    DataSource dataSource = mock(DataSource.class);
    DataSourceMetadata dataSourceMetadata = mock(DataSourceMetadata.class);
    when(dataSourceMetadata.isSorted()).thenReturn(false);
    when(dataSourceMetadata.getDataType()).thenReturn(DataType.INT);
    when(dataSourceMetadata.isSingleValue()).thenReturn(true);
    when(dataSource.getDataSourceMetadata()).thenReturn(dataSourceMetadata);
    when(dataSource.getInvertedIndex()).thenReturn(mock(InvertedIndexReader.class));

    PredicateEvaluator predicateEvaluator = mock(PredicateEvaluator.class);
    when(predicateEvaluator.isAlwaysFalse()).thenReturn(false);
    when(predicateEvaluator.isAlwaysTrue()).thenReturn(false);
    when(predicateEvaluator.getPredicateType()).thenReturn(Predicate.Type.EQ);
    when(predicateEvaluator.isDictionaryBased()).thenReturn(true);
    when(predicateEvaluator.isExclusive()).thenReturn(false);

    BaseFilterOperator filterOperator =
        FilterOperatorUtils.getLeafFilterOperator(queryContext, predicateEvaluator, dataSource, NUM_DOCS);
    assertTrue(filterOperator instanceof InvertedIndexFilterOperator);
  }

  @DataProvider
  public static Object[][] priorities() {
    SortedIndexBasedFilterOperator sorted = mock(SortedIndexBasedFilterOperator.class);
    BitmapBasedFilterOperator bitmap = mock(BitmapBasedFilterOperator.class);
    RangeIndexBasedFilterOperator range = mock(RangeIndexBasedFilterOperator.class);
    TextContainsFilterOperator textContains = mock(TextContainsFilterOperator.class);
    TextMatchFilterOperator textMatch = mock(TextMatchFilterOperator.class);
    JsonMatchFilterOperator jsonMatch = mock(JsonMatchFilterOperator.class);
    H3IndexFilterOperator h3 = mock(H3IndexFilterOperator.class);
    H3InclusionIndexFilterOperator h3Inclusion = mock(H3InclusionIndexFilterOperator.class);
    AndFilterOperator andFilterOperator = mock(AndFilterOperator.class);
    OrFilterOperator orFilterOperator = mock(OrFilterOperator.class);
    NotFilterOperator notWithHighPriority = new NotFilterOperator(sorted, NUM_DOCS, false);
    NotFilterOperator notWithLowPriority = new NotFilterOperator(orFilterOperator, NUM_DOCS, false);

    ExpressionFilterOperator expression = mock(ExpressionFilterOperator.class);
    BaseFilterOperator unknown = mock(BaseFilterOperator.class);

    MockedPrioritizedFilterOperator prioritizedBetweenSortedAndBitmap = mock(MockedPrioritizedFilterOperator.class);
    OptionalInt betweenSortedAndBitmapPriority =
        OptionalInt.of((PrioritizedFilterOperator.HIGH_PRIORITY + PrioritizedFilterOperator.MEDIUM_PRIORITY) / 2);
    when(prioritizedBetweenSortedAndBitmap.getPriority()).thenReturn(betweenSortedAndBitmapPriority);

    MockedPrioritizedFilterOperator notPrioritized = mock(MockedPrioritizedFilterOperator.class);
    when(prioritizedBetweenSortedAndBitmap.getPriority())
        .thenReturn(OptionalInt.empty());

    List<? extends List<? extends BaseFilterOperator>> expectedOrder = Lists.newArrayList(
        Lists.newArrayList(sorted, notWithHighPriority),
        Lists.newArrayList(bitmap),
        Lists.newArrayList(range, textContains, textMatch, jsonMatch, h3, h3Inclusion),
        Lists.newArrayList(andFilterOperator),
        Lists.newArrayList(orFilterOperator, notWithLowPriority),
        Lists.newArrayList(expression),
        Lists.newArrayList(unknown, notPrioritized)
    );

    List<Object[]> cases = new ArrayList<>();
    for (int i = 0; i < expectedOrder.size(); i++) {
      List<? extends BaseFilterOperator> currentOps = expectedOrder.get(i);
      for (BaseFilterOperator highPriorityOp : currentOps) {
        for (int j = i + 1; j < expectedOrder.size(); j++) {
          List<? extends BaseFilterOperator> lowerPriorityOps = expectedOrder.get(j);
          for (BaseFilterOperator lowerPriorityOp : lowerPriorityOps) {
            cases.add(new Object[] {highPriorityOp, lowerPriorityOp});
          }
        }
      }
    }
    return cases.toArray(new Object[][]{});
  }

  @Test(dataProvider = "priorities")
  public void testPriority(BaseFilterOperator highPriorty, BaseFilterOperator lowerPriorty) {
    ArrayList<BaseFilterOperator> unsorted = Lists.newArrayList(lowerPriorty, highPriorty);
    BaseFilterOperator filterOperator =
        FilterOperatorUtils.getAndFilterOperator(QUERY_CONTEXT, unsorted, NUM_DOCS);
    assertTrue(filterOperator instanceof AndFilterOperator);
    List<Operator> actualChildOperators = ((AndFilterOperator) filterOperator).getChildOperators();
    assertEquals(actualChildOperators, Lists.newArrayList(highPriorty, lowerPriorty), "Filter " + highPriorty
        + " should have more priority than filter " + lowerPriorty);
  }

  private void assertOrder(BaseFilterOperator first, BaseFilterOperator second) {
    BaseFilterOperator filterOperator =
        FilterOperatorUtils.getAndFilterOperator(QUERY_CONTEXT, Lists.newArrayList(second, first), NUM_DOCS);
    assertTrue(filterOperator instanceof AndFilterOperator);
    List<Operator> actualChildOperators = ((AndFilterOperator) filterOperator).getChildOperators();
    assertEquals(actualChildOperators, Lists.newArrayList(first, second), "Filter " + first + " should have "
        + "more priority than filter " + second);
  }

  private RawValueInvertedIndexFixture createRawValueBitmapInvertedIndexReader()
      throws Exception {
    File indexDir = Files.createTempDirectory("filter-raw-inverted-index").toFile();
    File indexFile = new File(indexDir,
        "col" + org.apache.pinot.segment.spi.V1Constants.Indexes.BITMAP_INVERTED_INDEX_FILE_EXTENSION);
    try (RawValueBitmapInvertedIndexCreator creator = new RawValueBitmapInvertedIndexCreator(DataType.INT, "col",
        indexDir)) {
      creator.add(11);
      creator.add(22);
      creator.add(11);
    }
    PinotDataBuffer dataBuffer = PinotDataBuffer.mapFile(indexFile, true, 0, indexFile.length(), ByteOrder.BIG_ENDIAN,
        "filter-raw-inverted-index");
    return new RawValueInvertedIndexFixture(indexDir, dataBuffer, new RawValueBitmapInvertedIndexReader(dataBuffer,
        DataType.INT));
  }

  private static final class RawValueInvertedIndexFixture implements AutoCloseable {
    private final File _indexDir;
    private final PinotDataBuffer _dataBuffer;
    private final RawValueBitmapInvertedIndexReader _reader;

    private RawValueInvertedIndexFixture(File indexDir, PinotDataBuffer dataBuffer,
        RawValueBitmapInvertedIndexReader reader) {
      _indexDir = indexDir;
      _dataBuffer = dataBuffer;
      _reader = reader;
    }

    @Override
    public void close()
        throws Exception {
      _reader.close();
      _dataBuffer.close();
      FileUtils.deleteQuietly(_indexDir);
    }
  }

  // --- TextMatchFilterOperator searchable doc fence tests ---

  private static final int TOTAL_DOCS = 1000;
  private static final int SEARCHABLE_DOCS = 950;
  private static final int[] LUCENE_MATCH_DOC_IDS = {10, 42, 300};

  @Test
  public void testTextMatchNotFenceGetFalsesExcludesUnindexedDocs() {
    NotFilterOperator notOp = new NotFilterOperator(textMatchOp(TOTAL_DOCS, SEARCHABLE_DOCS), TOTAL_DOCS, false);
    List<Integer> result = TestUtils.getDocIds(notOp.getTrues());

    assertEquals(result.size(), SEARCHABLE_DOCS - LUCENE_MATCH_DOC_IDS.length);
    for (int docId = SEARCHABLE_DOCS; docId < TOTAL_DOCS; docId++) {
      assertFalse(result.contains(docId), "Unindexed doc " + docId + " should not appear in NOT result");
    }
    for (int matchDocId : LUCENE_MATCH_DOC_IDS) {
      assertFalse(result.contains(matchDocId), "Matched doc " + matchDocId + " should not appear in NOT result");
    }
  }

  @Test
  public void testTextMatchNotFenceGetBitmapsExcludesUnindexedDocs() {
    NotFilterOperator notOp = new NotFilterOperator(textMatchOp(TOTAL_DOCS, SEARCHABLE_DOCS), TOTAL_DOCS, false);
    ImmutableRoaringBitmap bitmap = notOp.getBitmaps().reduce();

    assertEquals(bitmap.getCardinality(), SEARCHABLE_DOCS - LUCENE_MATCH_DOC_IDS.length);
    for (int docId = SEARCHABLE_DOCS; docId < TOTAL_DOCS; docId++) {
      assertFalse(bitmap.contains(docId), "Unindexed doc " + docId + " should not be set in inverted bitmap");
    }
  }

  @Test
  public void testTextMatchNotFenceGetTruesUnaffected() {
    // SEARCHABLE_DOCS is less than TOTAL_DOCS, so the result should be the same as the LUCENE_MATCH_DOC_IDS
    List<Integer> result = TestUtils.getDocIds(textMatchOp(TOTAL_DOCS, SEARCHABLE_DOCS).getTrues());
    assertEquals(result.size(), LUCENE_MATCH_DOC_IDS.length);
    for (int docId : LUCENE_MATCH_DOC_IDS) {
      assertTrue(result.contains(docId));
    }
  }

  @Test
  public void testTextMatchNotFenceOfflineSegmentAllDocsSearchable() {
    // searchableDocCount = -1 means all docs are searchable (offline segment)
    NotFilterOperator notOp = new NotFilterOperator(textMatchOp(TOTAL_DOCS, -1), TOTAL_DOCS, false);
    List<Integer> result = TestUtils.getDocIds(notOp.getTrues());
    assertEquals(result.size(), TOTAL_DOCS - LUCENE_MATCH_DOC_IDS.length);
  }

  @Test
  public void testTextMatchNotFenceFullyCaughtUpNoFence() {
    // searchableDocCount = numDocs means Lucene is fully caught up
    NotFilterOperator notOp = new NotFilterOperator(textMatchOp(TOTAL_DOCS, TOTAL_DOCS), TOTAL_DOCS, false);
    List<Integer> result = TestUtils.getDocIds(notOp.getTrues());
    assertEquals(result.size(), TOTAL_DOCS - LUCENE_MATCH_DOC_IDS.length);
  }

  @Test
  public void testTextMatchNotFenceZeroSearchableDocsReturnsEmpty() {
    // searchableDocCount = 0 means Lucene searcher has not yet refreshed; NOT result must be empty
    NotFilterOperator notOp = new NotFilterOperator(textMatchOp(TOTAL_DOCS, 0), TOTAL_DOCS, false);
    List<Integer> result = TestUtils.getDocIds(notOp.getTrues());
    assertEquals(result.size(), 0, "NOT result must be empty when no docs are visible to Lucene yet");
  }

  @Test
  public void testTextMatchNotFenceNoMatchesReturnsAllSearchable() {
    TextMatchPredicate predicate =
        new TextMatchPredicate(ExpressionContext.forIdentifier("__mergedTextIndex"), "nothing");
    TextIndexReader emptyReader = new TextIndexReader() {
      @Override
      public ImmutableRoaringBitmap getDictIds(String searchQuery) {
        throw new UnsupportedOperationException();
      }

      @Override
      public MutableRoaringBitmap getDocIds(String searchQuery) {
        return new MutableRoaringBitmap();
      }

      @Override
      public int getSearchableDocCount() {
        return SEARCHABLE_DOCS;
      }

      @Override
      public void close() {
      }
    };
    NotFilterOperator notOp =
        new NotFilterOperator(new TextMatchFilterOperator(emptyReader, predicate, TOTAL_DOCS), TOTAL_DOCS, false);
    List<Integer> result = TestUtils.getDocIds(notOp.getTrues());

    assertEquals(result.size(), SEARCHABLE_DOCS);
    for (int docId = SEARCHABLE_DOCS; docId < TOTAL_DOCS; docId++) {
      assertFalse(result.contains(docId), "Unindexed doc " + docId + " should not appear with zero matches");
    }
  }

  private static TextIndexReader mockTextIndexReader(int searchableDocCount) {
    return new TextIndexReader() {
      @Override
      public ImmutableRoaringBitmap getDictIds(String searchQuery) {
        throw new UnsupportedOperationException();
      }

      @Override
      public MutableRoaringBitmap getDocIds(String searchQuery) {
        MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
        for (int docId : LUCENE_MATCH_DOC_IDS) {
          bitmap.add(docId);
        }
        return bitmap;
      }

      @Override
      public int getSearchableDocCount() {
        return searchableDocCount;
      }

      @Override
      public void close() {
      }
    };
  }

  private static TextMatchFilterOperator textMatchOp(int numDocs, int searchableDocCount) {
    TextMatchPredicate predicate =
        new TextMatchPredicate(ExpressionContext.forIdentifier("__mergedTextIndex"), "error");
    return new TextMatchFilterOperator(mockTextIndexReader(searchableDocCount), predicate, numDocs);
  }

  private static abstract class MockedPrioritizedFilterOperator extends BaseFilterOperator
      implements PrioritizedFilterOperator<FilterBlock> {
    public MockedPrioritizedFilterOperator() {
      // This filter operator does not support AND/OR/NOT operations.
      super(0, false);
    }
  }
}
