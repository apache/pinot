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

import java.util.Arrays;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.predicate.EqPredicate;
import org.apache.pinot.common.request.context.predicate.InPredicate;
import org.apache.pinot.common.request.context.predicate.NotEqPredicate;
import org.apache.pinot.common.request.context.predicate.NotInPredicate;
import org.apache.pinot.common.request.context.predicate.RangePredicate;
import org.apache.pinot.core.operator.blocks.FilterBlock;
import org.apache.pinot.core.operator.filter.predicate.EqualsPredicateEvaluatorFactory;
import org.apache.pinot.core.operator.filter.predicate.InPredicateEvaluatorFactory;
import org.apache.pinot.core.operator.filter.predicate.NotEqualsPredicateEvaluatorFactory;
import org.apache.pinot.core.operator.filter.predicate.NotInPredicateEvaluatorFactory;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;
import org.apache.pinot.core.operator.filter.predicate.RangePredicateEvaluatorFactory;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.Constants;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class RawSortedIndexBasedFilterOperatorTest {

  // Sorted int data: [0, 0, 1, 1, 2, 3, 4, 5, 5, 5, 6, 7, 8, 9, 10, 10, 10, 15, 20, 100]
  private static final int[] SORTED_INT_DATA =
      {0, 0, 1, 1, 2, 3, 4, 5, 5, 5, 6, 7, 8, 9, 10, 10, 10, 15, 20, 100};
  private static final int NUM_DOCS = SORTED_INT_DATA.length;
  private static final ExpressionContext COL_EXPR = ExpressionContext.forIdentifier("testCol");

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static DataSource createIntDataSource(int[] data, int numDocsPerChunk) {
    ForwardIndexReader reader = mock(ForwardIndexReader.class);
    when(reader.isDictionaryEncoded()).thenReturn(false);
    when(reader.isSingleValue()).thenReturn(true);
    when(reader.getStoredType()).thenReturn(DataType.INT);
    when(reader.getNumDocsPerChunk()).thenReturn(numDocsPerChunk);
    when(reader.createContext()).thenReturn(null);
    for (int i = 0; i < data.length; i++) {
      when(reader.getInt(i, null)).thenReturn(data[i]);
    }

    DataSourceMetadata metadata = mock(DataSourceMetadata.class);
    when(metadata.isSorted()).thenReturn(true);
    when(metadata.isSingleValue()).thenReturn(true);
    when(metadata.getDataType()).thenReturn(DataType.INT);

    DataSource dataSource = mock(DataSource.class);
    when(dataSource.getForwardIndex()).thenReturn(reader);
    when(dataSource.getDataSourceMetadata()).thenReturn(metadata);
    when(dataSource.getDictionary()).thenReturn(null);
    when(dataSource.getNullValueVector()).thenReturn(null);

    return dataSource;
  }

  private static QueryContext createQueryContext() {
    QueryContext queryContext = mock(QueryContext.class);
    when(queryContext.isNullHandlingEnabled()).thenReturn(false);
    return queryContext;
  }

  private static int[] getMatchingDocIds(RawSortedIndexBasedFilterOperator operator) {
    FilterBlock filterBlock = operator.nextBlock();
    MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
    var iterator = filterBlock.getBlockDocIdSet().iterator();
    int docId;
    while ((docId = iterator.next()) != Constants.EOF) {
      bitmap.add(docId);
    }
    int[] result = new int[bitmap.getCardinality()];
    IntIterator it = bitmap.getIntIterator();
    int idx = 0;
    while (it.hasNext()) {
      result[idx++] = it.next();
    }
    return result;
  }

  @Test
  public void testEqPredicateValueExists() {
    DataSource dataSource = createIntDataSource(SORTED_INT_DATA, 0);
    QueryContext queryContext = createQueryContext();
    EqPredicate predicate = new EqPredicate(COL_EXPR, "5");
    PredicateEvaluator evaluator =
        EqualsPredicateEvaluatorFactory.newRawValueBasedEvaluator(predicate, DataType.INT);

    RawSortedIndexBasedFilterOperator operator =
        new RawSortedIndexBasedFilterOperator(queryContext, evaluator, dataSource, NUM_DOCS);

    int[] matchingDocIds = getMatchingDocIds(operator);
    // Value 5 is at indices 7, 8, 9
    assertEquals(matchingDocIds, new int[]{7, 8, 9});
  }

  @Test
  public void testEqPredicateValueNotExists() {
    DataSource dataSource = createIntDataSource(SORTED_INT_DATA, 0);
    QueryContext queryContext = createQueryContext();
    EqPredicate predicate = new EqPredicate(COL_EXPR, "50");
    PredicateEvaluator evaluator =
        EqualsPredicateEvaluatorFactory.newRawValueBasedEvaluator(predicate, DataType.INT);

    RawSortedIndexBasedFilterOperator operator =
        new RawSortedIndexBasedFilterOperator(queryContext, evaluator, dataSource, NUM_DOCS);

    int[] matchingDocIds = getMatchingDocIds(operator);
    assertEquals(matchingDocIds.length, 0);
  }

  @Test
  public void testEqPredicateFirstValue() {
    DataSource dataSource = createIntDataSource(SORTED_INT_DATA, 0);
    QueryContext queryContext = createQueryContext();
    EqPredicate predicate = new EqPredicate(COL_EXPR, "0");
    PredicateEvaluator evaluator =
        EqualsPredicateEvaluatorFactory.newRawValueBasedEvaluator(predicate, DataType.INT);

    RawSortedIndexBasedFilterOperator operator =
        new RawSortedIndexBasedFilterOperator(queryContext, evaluator, dataSource, NUM_DOCS);

    int[] matchingDocIds = getMatchingDocIds(operator);
    assertEquals(matchingDocIds, new int[]{0, 1});
  }

  @Test
  public void testEqPredicateLastValue() {
    DataSource dataSource = createIntDataSource(SORTED_INT_DATA, 0);
    QueryContext queryContext = createQueryContext();
    EqPredicate predicate = new EqPredicate(COL_EXPR, "100");
    PredicateEvaluator evaluator =
        EqualsPredicateEvaluatorFactory.newRawValueBasedEvaluator(predicate, DataType.INT);

    RawSortedIndexBasedFilterOperator operator =
        new RawSortedIndexBasedFilterOperator(queryContext, evaluator, dataSource, NUM_DOCS);

    int[] matchingDocIds = getMatchingDocIds(operator);
    assertEquals(matchingDocIds, new int[]{19});
  }

  @Test
  public void testNotEqPredicate() {
    DataSource dataSource = createIntDataSource(SORTED_INT_DATA, 0);
    QueryContext queryContext = createQueryContext();
    NotEqPredicate predicate = new NotEqPredicate(COL_EXPR, "5");
    PredicateEvaluator evaluator =
        NotEqualsPredicateEvaluatorFactory.newRawValueBasedEvaluator(predicate, DataType.INT);

    RawSortedIndexBasedFilterOperator operator =
        new RawSortedIndexBasedFilterOperator(queryContext, evaluator, dataSource, NUM_DOCS);

    int[] matchingDocIds = getMatchingDocIds(operator);
    // All except indices 7, 8, 9
    assertEquals(matchingDocIds, new int[]{0, 1, 2, 3, 4, 5, 6, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19});
  }

  @Test
  public void testRangePredicateInclusive() {
    DataSource dataSource = createIntDataSource(SORTED_INT_DATA, 0);
    QueryContext queryContext = createQueryContext();
    // Range [3, 7] inclusive
    RangePredicate predicate = new RangePredicate(COL_EXPR, true, "3", true, "7", DataType.INT);
    PredicateEvaluator evaluator =
        RangePredicateEvaluatorFactory.newRawValueBasedEvaluator(predicate, DataType.INT);

    RawSortedIndexBasedFilterOperator operator =
        new RawSortedIndexBasedFilterOperator(queryContext, evaluator, dataSource, NUM_DOCS);

    int[] matchingDocIds = getMatchingDocIds(operator);
    // Values 3,4,5,5,5,6,7 at indices 5,6,7,8,9,10,11
    assertEquals(matchingDocIds, new int[]{5, 6, 7, 8, 9, 10, 11});
  }

  @Test
  public void testRangePredicateExclusive() {
    DataSource dataSource = createIntDataSource(SORTED_INT_DATA, 0);
    QueryContext queryContext = createQueryContext();
    // Range (3, 7) exclusive
    RangePredicate predicate = new RangePredicate(COL_EXPR, false, "3", false, "7", DataType.INT);
    PredicateEvaluator evaluator =
        RangePredicateEvaluatorFactory.newRawValueBasedEvaluator(predicate, DataType.INT);

    RawSortedIndexBasedFilterOperator operator =
        new RawSortedIndexBasedFilterOperator(queryContext, evaluator, dataSource, NUM_DOCS);

    int[] matchingDocIds = getMatchingDocIds(operator);
    // Values 4,5,5,5,6 at indices 6,7,8,9,10
    assertEquals(matchingDocIds, new int[]{6, 7, 8, 9, 10});
  }

  @Test
  public void testRangePredicateUnboundedLower() {
    DataSource dataSource = createIntDataSource(SORTED_INT_DATA, 0);
    QueryContext queryContext = createQueryContext();
    RangePredicate predicate =
        new RangePredicate(COL_EXPR, true, RangePredicate.UNBOUNDED, true, "2", DataType.INT);
    PredicateEvaluator evaluator =
        RangePredicateEvaluatorFactory.newRawValueBasedEvaluator(predicate, DataType.INT);

    RawSortedIndexBasedFilterOperator operator =
        new RawSortedIndexBasedFilterOperator(queryContext, evaluator, dataSource, NUM_DOCS);

    int[] matchingDocIds = getMatchingDocIds(operator);
    // Values 0,0,1,1,2 at indices 0,1,2,3,4
    assertEquals(matchingDocIds, new int[]{0, 1, 2, 3, 4});
  }

  @Test
  public void testRangePredicateUnboundedUpper() {
    DataSource dataSource = createIntDataSource(SORTED_INT_DATA, 0);
    QueryContext queryContext = createQueryContext();
    RangePredicate predicate =
        new RangePredicate(COL_EXPR, true, "15", true, RangePredicate.UNBOUNDED, DataType.INT);
    PredicateEvaluator evaluator =
        RangePredicateEvaluatorFactory.newRawValueBasedEvaluator(predicate, DataType.INT);

    RawSortedIndexBasedFilterOperator operator =
        new RawSortedIndexBasedFilterOperator(queryContext, evaluator, dataSource, NUM_DOCS);

    int[] matchingDocIds = getMatchingDocIds(operator);
    // Values 15,20,100 at indices 17,18,19
    assertEquals(matchingDocIds, new int[]{17, 18, 19});
  }

  @Test
  public void testRangePredicateNoMatch() {
    DataSource dataSource = createIntDataSource(SORTED_INT_DATA, 0);
    QueryContext queryContext = createQueryContext();
    RangePredicate predicate = new RangePredicate(COL_EXPR, true, "50", true, "99", DataType.INT);
    PredicateEvaluator evaluator =
        RangePredicateEvaluatorFactory.newRawValueBasedEvaluator(predicate, DataType.INT);

    RawSortedIndexBasedFilterOperator operator =
        new RawSortedIndexBasedFilterOperator(queryContext, evaluator, dataSource, NUM_DOCS);

    int[] matchingDocIds = getMatchingDocIds(operator);
    assertEquals(matchingDocIds.length, 0);
  }

  @Test
  public void testInPredicate() {
    DataSource dataSource = createIntDataSource(SORTED_INT_DATA, 0);
    QueryContext queryContext = createQueryContext();
    InPredicate predicate = new InPredicate(COL_EXPR, Arrays.asList("1", "5", "10"));
    PredicateEvaluator evaluator =
        InPredicateEvaluatorFactory.newRawValueBasedEvaluator(predicate, DataType.INT);

    RawSortedIndexBasedFilterOperator operator =
        new RawSortedIndexBasedFilterOperator(queryContext, evaluator, dataSource, NUM_DOCS);

    int[] matchingDocIds = getMatchingDocIds(operator);
    // Value 1 at 2,3; value 5 at 7,8,9; value 10 at 14,15,16
    assertEquals(matchingDocIds, new int[]{2, 3, 7, 8, 9, 14, 15, 16});
  }

  @Test
  public void testNotInPredicate() {
    DataSource dataSource = createIntDataSource(SORTED_INT_DATA, 0);
    QueryContext queryContext = createQueryContext();
    NotInPredicate predicate = new NotInPredicate(COL_EXPR, Arrays.asList("0", "100"));
    PredicateEvaluator evaluator =
        NotInPredicateEvaluatorFactory.newRawValueBasedEvaluator(predicate, DataType.INT);

    RawSortedIndexBasedFilterOperator operator =
        new RawSortedIndexBasedFilterOperator(queryContext, evaluator, dataSource, NUM_DOCS);

    int[] matchingDocIds = getMatchingDocIds(operator);
    // All except 0,1 (value 0) and 19 (value 100)
    assertEquals(matchingDocIds, new int[]{2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18});
  }

  @Test
  public void testCanOptimizeCount() {
    DataSource dataSource = createIntDataSource(SORTED_INT_DATA, 0);
    QueryContext queryContext = createQueryContext();
    EqPredicate predicate = new EqPredicate(COL_EXPR, "5");
    PredicateEvaluator evaluator =
        EqualsPredicateEvaluatorFactory.newRawValueBasedEvaluator(predicate, DataType.INT);

    RawSortedIndexBasedFilterOperator operator =
        new RawSortedIndexBasedFilterOperator(queryContext, evaluator, dataSource, NUM_DOCS);

    assertTrue(operator.canOptimizeCount());
    assertEquals(operator.getNumMatchingDocs(), 3);
  }

  @Test
  public void testCanProduceBitmaps() {
    DataSource dataSource = createIntDataSource(SORTED_INT_DATA, 0);
    QueryContext queryContext = createQueryContext();
    EqPredicate predicate = new EqPredicate(COL_EXPR, "10");
    PredicateEvaluator evaluator =
        EqualsPredicateEvaluatorFactory.newRawValueBasedEvaluator(predicate, DataType.INT);

    RawSortedIndexBasedFilterOperator operator =
        new RawSortedIndexBasedFilterOperator(queryContext, evaluator, dataSource, NUM_DOCS);

    assertTrue(operator.canProduceBitmaps());
    BitmapCollection bitmaps = operator.getBitmaps();
    ImmutableRoaringBitmap bitmap = bitmaps.reduce();
    assertEquals(bitmap.getCardinality(), 3);
    assertTrue(bitmap.contains(14));
    assertTrue(bitmap.contains(15));
    assertTrue(bitmap.contains(16));
  }

  @Test
  public void testChunkAwareBinarySearch() {
    // Test with numDocsPerChunk = 4 to verify two-level search
    DataSource dataSource = createIntDataSource(SORTED_INT_DATA, 4);
    QueryContext queryContext = createQueryContext();
    EqPredicate predicate = new EqPredicate(COL_EXPR, "5");
    PredicateEvaluator evaluator =
        EqualsPredicateEvaluatorFactory.newRawValueBasedEvaluator(predicate, DataType.INT);

    RawSortedIndexBasedFilterOperator operator =
        new RawSortedIndexBasedFilterOperator(queryContext, evaluator, dataSource, NUM_DOCS);

    int[] matchingDocIds = getMatchingDocIds(operator);
    assertEquals(matchingDocIds, new int[]{7, 8, 9});
  }

  @Test
  public void testChunkAwareRangePredicate() {
    // Test with numDocsPerChunk = 5 to verify two-level range search
    DataSource dataSource = createIntDataSource(SORTED_INT_DATA, 5);
    QueryContext queryContext = createQueryContext();
    RangePredicate predicate = new RangePredicate(COL_EXPR, true, "3", true, "7", DataType.INT);
    PredicateEvaluator evaluator =
        RangePredicateEvaluatorFactory.newRawValueBasedEvaluator(predicate, DataType.INT);

    RawSortedIndexBasedFilterOperator operator =
        new RawSortedIndexBasedFilterOperator(queryContext, evaluator, dataSource, NUM_DOCS);

    int[] matchingDocIds = getMatchingDocIds(operator);
    assertEquals(matchingDocIds, new int[]{5, 6, 7, 8, 9, 10, 11});
  }

  @Test
  public void testExplainString() {
    DataSource dataSource = createIntDataSource(SORTED_INT_DATA, 0);
    QueryContext queryContext = createQueryContext();
    EqPredicate predicate = new EqPredicate(COL_EXPR, "5");
    PredicateEvaluator evaluator =
        EqualsPredicateEvaluatorFactory.newRawValueBasedEvaluator(predicate, DataType.INT);

    RawSortedIndexBasedFilterOperator operator =
        new RawSortedIndexBasedFilterOperator(queryContext, evaluator, dataSource, NUM_DOCS);

    String explainString = operator.toExplainString();
    assertTrue(explainString.contains("FILTER_RAW_SORTED_INDEX"));
    assertTrue(explainString.contains("raw_sorted_index"));
  }

  @Test
  public void testAllDocsMatch() {
    DataSource dataSource = createIntDataSource(SORTED_INT_DATA, 0);
    QueryContext queryContext = createQueryContext();
    RangePredicate predicate = new RangePredicate(COL_EXPR, true, "0", true, "100", DataType.INT);
    PredicateEvaluator evaluator =
        RangePredicateEvaluatorFactory.newRawValueBasedEvaluator(predicate, DataType.INT);

    RawSortedIndexBasedFilterOperator operator =
        new RawSortedIndexBasedFilterOperator(queryContext, evaluator, dataSource, NUM_DOCS);

    assertEquals(operator.getNumMatchingDocs(), NUM_DOCS);
  }

  @Test
  public void testSingleElementData() {
    int[] data = {42};
    DataSource dataSource = createIntDataSource(data, 0);
    QueryContext queryContext = createQueryContext();
    EqPredicate predicate = new EqPredicate(COL_EXPR, "42");
    PredicateEvaluator evaluator =
        EqualsPredicateEvaluatorFactory.newRawValueBasedEvaluator(predicate, DataType.INT);

    RawSortedIndexBasedFilterOperator operator =
        new RawSortedIndexBasedFilterOperator(queryContext, evaluator, dataSource, 1);

    int[] matchingDocIds = getMatchingDocIds(operator);
    assertEquals(matchingDocIds, new int[]{0});
  }

  // --- Long data type test ---

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static DataSource createLongDataSource(long[] data) {
    ForwardIndexReader reader = mock(ForwardIndexReader.class);
    when(reader.isDictionaryEncoded()).thenReturn(false);
    when(reader.isSingleValue()).thenReturn(true);
    when(reader.getStoredType()).thenReturn(DataType.LONG);
    when(reader.getNumDocsPerChunk()).thenReturn(0);
    when(reader.createContext()).thenReturn(null);
    for (int i = 0; i < data.length; i++) {
      when(reader.getLong(i, null)).thenReturn(data[i]);
    }

    DataSourceMetadata metadata = mock(DataSourceMetadata.class);
    when(metadata.isSorted()).thenReturn(true);
    when(metadata.isSingleValue()).thenReturn(true);
    when(metadata.getDataType()).thenReturn(DataType.LONG);

    DataSource dataSource = mock(DataSource.class);
    when(dataSource.getForwardIndex()).thenReturn(reader);
    when(dataSource.getDataSourceMetadata()).thenReturn(metadata);
    when(dataSource.getDictionary()).thenReturn(null);
    when(dataSource.getNullValueVector()).thenReturn(null);

    return dataSource;
  }

  @Test
  public void testLongEqPredicate() {
    long[] data = {100L, 200L, 300L, 300L, 400L, 500L};
    DataSource dataSource = createLongDataSource(data);
    QueryContext queryContext = createQueryContext();
    EqPredicate predicate = new EqPredicate(COL_EXPR, "300");
    PredicateEvaluator evaluator =
        EqualsPredicateEvaluatorFactory.newRawValueBasedEvaluator(predicate, DataType.LONG);

    RawSortedIndexBasedFilterOperator operator =
        new RawSortedIndexBasedFilterOperator(queryContext, evaluator, dataSource, data.length);

    int[] matchingDocIds = getMatchingDocIds(operator);
    assertEquals(matchingDocIds, new int[]{2, 3});
  }

  // --- String data type test ---

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static DataSource createStringDataSource(String[] data) {
    ForwardIndexReader reader = mock(ForwardIndexReader.class);
    when(reader.isDictionaryEncoded()).thenReturn(false);
    when(reader.isSingleValue()).thenReturn(true);
    when(reader.getStoredType()).thenReturn(DataType.STRING);
    when(reader.getNumDocsPerChunk()).thenReturn(0);
    when(reader.createContext()).thenReturn(null);
    for (int i = 0; i < data.length; i++) {
      when(reader.getString(i, null)).thenReturn(data[i]);
    }

    DataSourceMetadata metadata = mock(DataSourceMetadata.class);
    when(metadata.isSorted()).thenReturn(true);
    when(metadata.isSingleValue()).thenReturn(true);
    when(metadata.getDataType()).thenReturn(DataType.STRING);

    DataSource dataSource = mock(DataSource.class);
    when(dataSource.getForwardIndex()).thenReturn(reader);
    when(dataSource.getDataSourceMetadata()).thenReturn(metadata);
    when(dataSource.getDictionary()).thenReturn(null);
    when(dataSource.getNullValueVector()).thenReturn(null);

    return dataSource;
  }

  @Test
  public void testStringRangePredicate() {
    String[] data = {"apple", "banana", "cherry", "date", "elderberry", "fig"};
    DataSource dataSource = createStringDataSource(data);
    QueryContext queryContext = createQueryContext();
    RangePredicate predicate =
        new RangePredicate(COL_EXPR, true, "banana", true, "elderberry", DataType.STRING);
    PredicateEvaluator evaluator =
        RangePredicateEvaluatorFactory.newRawValueBasedEvaluator(predicate, DataType.STRING);

    RawSortedIndexBasedFilterOperator operator =
        new RawSortedIndexBasedFilterOperator(queryContext, evaluator, dataSource, data.length);

    int[] matchingDocIds = getMatchingDocIds(operator);
    // banana(1), cherry(2), date(3), elderberry(4)
    assertEquals(matchingDocIds, new int[]{1, 2, 3, 4});
  }
}
