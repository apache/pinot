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
package org.apache.pinot.core.operator.filter.predicate;

import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.predicate.EqPredicate;
import org.apache.pinot.common.request.context.predicate.RangePredicate;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.InvertedIndexReader;
import org.apache.pinot.segment.spi.index.reader.RangeIndexReader;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class PredicateEvaluatorProviderTest {

  /// RAW forward index with dictionary but no inverted/range/sorted index — the planner must drop the dictionary so
  /// that the scan-based filter operator receives a raw-value evaluator. Otherwise, the scan iterator would call
  /// `applySV(rawValue)` on a dict-based evaluator and throw [UnsupportedOperationException].
  @Test
  public void rawForwardWithDictNoSecondaryIndexUsesRawValueEvaluator() {
    DataSource dataSource = mockDataSource("col", DataType.STRING, /*sorted=*/false,
        /*dict=*/true, /*forwardDictEncoded=*/false, /*inverted=*/false, /*range=*/false, /*rangeExact=*/false);
    PredicateEvaluator evaluator =
        PredicateEvaluatorProvider.getPredicateEvaluator(eqPredicate("col", "value-1"), dataSource,
            mockQueryContext(/*allowAllIndexes=*/true));
    assertFalse(evaluator.isDictionaryBased(),
        "RAW forward + dict + no secondary index must produce raw-value evaluator");
  }

  /// RAW forward index with dictionary + inverted index — keep the dictionary so the inverted-index path can translate
  /// dict IDs to docs.
  @Test
  public void rawForwardWithDictAndInvertedKeepsDictEvaluator() {
    DataSource dataSource = mockDataSource("col", DataType.STRING, /*sorted=*/false,
        /*dict=*/true, /*forwardDictEncoded=*/false, /*inverted=*/true, /*range=*/false, /*rangeExact=*/false);
    PredicateEvaluator evaluator =
        PredicateEvaluatorProvider.getPredicateEvaluator(eqPredicate("col", "value-1"), dataSource,
            mockQueryContext(/*allowAllIndexes=*/true));
    assertTrue(evaluator.isDictionaryBased(),
        "RAW forward + dict + inverted index must keep dict-based evaluator for inverted-index lookup");
  }

  /// RAW forward index with dictionary + inverted index, but the user disabled the inverted index for this query via
  /// `OPTION(skipIndexes=...)`. The planner must drop the dictionary because the scan path will be used.
  @Test
  public void rawForwardWithDictAndDisabledInvertedDropsDictEvaluator() {
    DataSource dataSource = mockDataSource("col", DataType.STRING, /*sorted=*/false,
        /*dict=*/true, /*forwardDictEncoded=*/false, /*inverted=*/true, /*range=*/false, /*rangeExact=*/false);
    QueryContext queryContext = mockQueryContext(/*allowAllIndexes=*/false);
    PredicateEvaluator evaluator =
        PredicateEvaluatorProvider.getPredicateEvaluator(eqPredicate("col", "value-1"), dataSource, queryContext);
    assertFalse(evaluator.isDictionaryBased(),
        "Inverted index disallowed via skipIndexes must trigger raw-value evaluator on raw forward");
  }

  /// Standard dict-encoded forward index — keep the dictionary regardless of secondary indexes.
  @Test
  public void dictEncodedForwardKeepsDictEvaluator() {
    DataSource dataSource = mockDataSource("col", DataType.STRING, /*sorted=*/false,
        /*dict=*/true, /*forwardDictEncoded=*/true, /*inverted=*/false, /*range=*/false, /*rangeExact=*/false);
    PredicateEvaluator evaluator =
        PredicateEvaluatorProvider.getPredicateEvaluator(eqPredicate("col", "value-1"), dataSource,
            mockQueryContext(/*allowAllIndexes=*/true));
    assertTrue(evaluator.isDictionaryBased(), "Dict-encoded forward index must always keep dict-based evaluator");
  }

  /// Forward index disabled (e.g. forward-index-disabled column) — keep dictionary in place since scan is impossible.
  @Test
  public void forwardIndexDisabledKeepsDictEvaluator() {
    DataSource dataSource = mockDataSource("col", DataType.STRING, /*sorted=*/false,
        /*dict=*/true, /*forwardDictEncoded=*/false, /*inverted=*/true, /*range=*/false, /*rangeExact=*/false);
    Mockito.when(dataSource.getForwardIndex()).thenReturn(null);
    PredicateEvaluator evaluator =
        PredicateEvaluatorProvider.getPredicateEvaluator(eqPredicate("col", "value-1"), dataSource,
            mockQueryContext(/*allowAllIndexes=*/true));
    assertTrue(evaluator.isDictionaryBased(),
        "Disabled forward index must keep dict-based evaluator (scan is impossible)");
  }

  /// RAW forward + dictionary + inverted index but a RANGE predicate is being filtered. The inverted index does not
  /// service RANGE filtering, so the operator selection falls through to scan. The planner must drop the dictionary
  /// for this predicate even though an inverted index physically exists on the column.
  @Test
  public void rawForwardWithDictAndInvertedDropsDictForRangePredicate() {
    DataSource dataSource = mockDataSource("col", DataType.INT, /*sorted=*/false,
        /*dict=*/true, /*forwardDictEncoded=*/false, /*inverted=*/true, /*range=*/false, /*rangeExact=*/false);
    Dictionary result = PredicateEvaluatorProvider.getDictionaryUsableForFiltering(dataSource,
        mockQueryContext(/*allowAllIndexes=*/true), rangePredicate("col", "(5\t\t15)"));
    assertNull(result,
        "RANGE predicate on inverted-but-not-range column must drop dict so raw-value evaluator is built");
  }

  /// RAW forward + dictionary + range index + RANGE predicate. When a dictionary exists, the range index is built over
  /// dict IDs (see `RangeIndexType#createIndexCreator`), so the dictionary must be preserved and the dict-based range
  /// evaluator used.
  @Test
  public void rawForwardWithDictAndRangeIndexKeepsDictForRangePredicate() {
    DataSource dataSource = mockDataSource("col", DataType.INT, /*sorted=*/false,
        /*dict=*/true, /*forwardDictEncoded=*/false, /*inverted=*/false, /*range=*/true, /*rangeExact=*/true);
    Dictionary result = PredicateEvaluatorProvider.getDictionaryUsableForFiltering(dataSource,
        mockQueryContext(/*allowAllIndexes=*/true), rangePredicate("col", "(5\t\t15)"));
    assertNotNull(result,
        "RANGE predicate on dict-id-based range index must keep dict so range reader receives dict-id evaluator");
  }

  /// RAW forward + dictionary + non-exact (legacy) range index + RANGE predicate. Non-exact range readers fall back to
  /// ScanBasedFilterOperator for partial matches; that scan applies the predicate evaluator on raw forward values, so
  /// a dict-based evaluator would be misapplied. The dictionary must be dropped.
  @Test
  public void rawForwardWithDictAndNonExactRangeIndexDropsDictForRangePredicate() {
    DataSource dataSource = mockDataSource("col", DataType.INT, /*sorted=*/false,
        /*dict=*/true, /*forwardDictEncoded=*/false, /*inverted=*/false, /*range=*/true, /*rangeExact=*/false);
    Dictionary result = PredicateEvaluatorProvider.getDictionaryUsableForFiltering(dataSource,
        mockQueryContext(/*allowAllIndexes=*/true), rangePredicate("col", "(5\t\t15)"));
    assertNull(result,
        "Non-exact range index falls back to scan; dict must be dropped so scan uses raw-value evaluator");
  }

  /// RAW forward + dictionary + range index but the range index is disabled via skipIndexes. With no other
  /// dict-consuming operator available, RANGE falls to scan, which needs the raw-value evaluator.
  @Test
  public void rawForwardWithDictAndDisabledRangeIndexDropsDictForRangePredicate() {
    DataSource dataSource = mockDataSource("col", DataType.INT, /*sorted=*/false,
        /*dict=*/true, /*forwardDictEncoded=*/false, /*inverted=*/false, /*range=*/true, /*rangeExact=*/true);
    Dictionary result = PredicateEvaluatorProvider.getDictionaryUsableForFiltering(dataSource,
        mockQueryContext(/*allowAllIndexes=*/false), rangePredicate("col", "(5\t\t15)"));
    assertNull(result, "Range index disallowed via skipIndexes must trigger raw-value evaluator on raw forward");
  }

  /// RAW forward + dictionary + EXACT range index + EQ predicate. The exact range index can serve EQ (see
  /// `RangeIndexBasedFilterOperator#canEvaluate`) and uses dict IDs when dict exists, so keep the dict.
  @Test
  public void rawForwardWithDictAndExactRangeIndexKeepsDictForEqPredicate() {
    DataSource dataSource = mockDataSource("col", DataType.INT, /*sorted=*/false,
        /*dict=*/true, /*forwardDictEncoded=*/false, /*inverted=*/false, /*range=*/true, /*rangeExact=*/true);
    Dictionary result = PredicateEvaluatorProvider.getDictionaryUsableForFiltering(dataSource,
        mockQueryContext(/*allowAllIndexes=*/true), eqPredicate("col", "7"));
    assertNotNull(result, "Exact range index serves EQ predicate; dict must be preserved for the dict-based evaluator");
  }

  /// RAW forward + dictionary + NON-EXACT range index + EQ predicate. Non-exact range indexes don't serve EQ; EQ falls
  /// through to scan. With no inverted/sorted, the dictionary must be dropped.
  @Test
  public void rawForwardWithDictAndNonExactRangeIndexDropsDictForEqPredicate() {
    DataSource dataSource = mockDataSource("col", DataType.INT, /*sorted=*/false,
        /*dict=*/true, /*forwardDictEncoded=*/false, /*inverted=*/false, /*range=*/true, /*rangeExact=*/false);
    Dictionary result = PredicateEvaluatorProvider.getDictionaryUsableForFiltering(dataSource,
        mockQueryContext(/*allowAllIndexes=*/true), eqPredicate("col", "7"));
    assertNull(result,
        "Non-exact range index does not serve EQ; dict must be dropped so scan uses raw-value evaluator");
  }

  /// RAW forward + dictionary + range index + NOT_EQ predicate. NOT_EQ never uses the range index, so dict must be
  /// dropped (no inverted/sorted in this test).
  @Test
  public void rawForwardWithDictAndRangeIndexDropsDictForNotEqPredicate() {
    DataSource dataSource = mockDataSource("col", DataType.INT, /*sorted=*/false,
        /*dict=*/true, /*forwardDictEncoded=*/false, /*inverted=*/false, /*range=*/true, /*rangeExact=*/true);
    Dictionary result = PredicateEvaluatorProvider.getDictionaryUsableForFiltering(dataSource,
        mockQueryContext(/*allowAllIndexes=*/true),
        new org.apache.pinot.common.request.context.predicate.NotEqPredicate(ExpressionContext.forIdentifier("col"),
            "7"));
    assertNull(result, "NOT_EQ never consults the range index; dict must be dropped so scan uses raw-value evaluator");
  }

  /// RAW forward + dictionary + inverted + EQ predicate. EQ uses inverted index, so dict must be preserved.
  @Test
  public void rawForwardWithDictAndInvertedKeepsDictForEqPredicate() {
    DataSource dataSource = mockDataSource("col", DataType.STRING, /*sorted=*/false,
        /*dict=*/true, /*forwardDictEncoded=*/false, /*inverted=*/true, /*range=*/false, /*rangeExact=*/false);
    Dictionary result = PredicateEvaluatorProvider.getDictionaryUsableForFiltering(dataSource,
        mockQueryContext(/*allowAllIndexes=*/true), eqPredicate("col", "value-1"));
    assertNotNull(result, "EQ predicate must keep dict so InvertedIndexFilterOperator can do dict-id lookup");
  }

  private static EqPredicate eqPredicate(String column, String value) {
    return new EqPredicate(ExpressionContext.forIdentifier(column), value);
  }

  private static RangePredicate rangePredicate(String column, String range) {
    return new RangePredicate(ExpressionContext.forIdentifier(column), range);
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static DataSource mockDataSource(String columnName, DataType dataType, boolean sorted, boolean hasDictionary,
      boolean forwardDictEncoded, boolean hasInverted, boolean hasRange, boolean rangeExact) {
    DataSource dataSource = Mockito.mock(DataSource.class);
    DataSourceMetadata metadata = Mockito.mock(DataSourceMetadata.class);
    FieldSpec fieldSpec = new org.apache.pinot.spi.data.DimensionFieldSpec(columnName, dataType, true);
    Mockito.when(metadata.getDataType()).thenReturn(dataType);
    Mockito.when(metadata.isSorted()).thenReturn(sorted);
    Mockito.when(metadata.getFieldSpec()).thenReturn(fieldSpec);
    Mockito.when(dataSource.getDataSourceMetadata()).thenReturn(metadata);
    Mockito.when(dataSource.getColumnName()).thenReturn(columnName);

    ForwardIndexReader forwardIndex = Mockito.mock(ForwardIndexReader.class);
    Mockito.when(forwardIndex.isDictionaryEncoded()).thenReturn(forwardDictEncoded);
    Mockito.when(dataSource.getForwardIndex()).thenReturn(forwardIndex);

    if (hasDictionary) {
      Dictionary dictionary = Mockito.mock(Dictionary.class);
      Mockito.when(dictionary.length()).thenReturn(0);
      Mockito.when(dataSource.getDictionary()).thenReturn(dictionary);
    } else {
      Mockito.when(dataSource.getDictionary()).thenReturn(null);
    }
    if (hasInverted) {
      Mockito.when(dataSource.getInvertedIndex()).thenReturn(Mockito.mock(InvertedIndexReader.class));
    } else {
      Mockito.when(dataSource.getInvertedIndex()).thenReturn(null);
    }
    if (hasRange) {
      RangeIndexReader rangeIndex = Mockito.mock(RangeIndexReader.class);
      Mockito.when(rangeIndex.isExact()).thenReturn(rangeExact);
      Mockito.when(dataSource.getRangeIndex()).thenReturn(rangeIndex);
    } else {
      Mockito.when(dataSource.getRangeIndex()).thenReturn(null);
    }
    return dataSource;
  }

  private static QueryContext mockQueryContext(boolean allowAllIndexes) {
    QueryContext queryContext = Mockito.mock(QueryContext.class);
    Mockito.when(
            queryContext.isIndexUseAllowed(Mockito.any(DataSource.class), Mockito.any(FieldConfig.IndexType.class)))
        .thenReturn(allowAllIndexes);
    Mockito.when(queryContext.isIndexUseAllowed(Mockito.anyString(), Mockito.any(FieldConfig.IndexType.class)))
        .thenReturn(allowAllIndexes);
    return queryContext;
  }
}
