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
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.request.context.predicate.EqPredicate;
import org.apache.pinot.common.request.context.predicate.InPredicate;
import org.apache.pinot.common.request.context.predicate.IsNotNullPredicate;
import org.apache.pinot.common.request.context.predicate.IsNullPredicate;
import org.apache.pinot.common.request.context.predicate.NotEqPredicate;
import org.apache.pinot.common.request.context.predicate.NotInPredicate;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.common.request.context.predicate.RangePredicate;
import org.apache.pinot.common.request.context.predicate.RegexpLikePredicate;
import org.apache.pinot.core.common.BlockDocIdIterator;
import org.apache.pinot.core.operator.transform.function.ItemTransformFunction;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.Constants;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.datasource.OpenStructDataSource;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.InvertedIndexReader;
import org.apache.pinot.segment.spi.index.reader.NullValueVectorReader;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class MapFilterOperatorOpenStructTest {
  private static final int NUM_DOCS = 100;
  private static final String COLUMN = "metrics";
  private static final String KEY = "status";

  private static ExpressionContext itemExpr(String column, String key) {
    ExpressionContext colArg = ExpressionContext.forIdentifier(column);
    ExpressionContext keyArg = ExpressionContext.forLiteral(FieldSpec.DataType.STRING, key);
    FunctionContext fn = new FunctionContext(FunctionContext.Type.TRANSFORM,
        ItemTransformFunction.FUNCTION_NAME, Arrays.asList(colArg, keyArg));
    return ExpressionContext.forFunction(fn);
  }

  private static Predicate makeEqPredicate(String column, String key, String value) {
    return new EqPredicate(itemExpr(column, key), value);
  }

  private static Predicate makeNotEqPredicate(String column, String key, String value) {
    return new NotEqPredicate(itemExpr(column, key), value);
  }

  private static Predicate makeInPredicate(String column, String key, List<String> values) {
    return new InPredicate(itemExpr(column, key), values);
  }

  private static Predicate makeNotInPredicate(String column, String key, List<String> values) {
    return new NotInPredicate(itemExpr(column, key), values);
  }

  private static Predicate makeIsNullPredicate(String column, String key) {
    return new IsNullPredicate(itemExpr(column, key));
  }

  private static Predicate makeIsNotNullPredicate(String column, String key) {
    return new IsNotNullPredicate(itemExpr(column, key));
  }

  private QueryContext mockQueryContext() {
    return mockQueryContext(false);
  }

  private QueryContext mockQueryContext(boolean nullHandlingEnabled) {
    QueryContext qc = mock(QueryContext.class);
    when(qc.isNullHandlingEnabled()).thenReturn(nullHandlingEnabled);
    when(qc.isIndexUseAllowed(any(DataSource.class), any())).thenReturn(true);
    when(qc.isIndexUseAllowed(anyString(), any())).thenReturn(true);
    return qc;
  }

  /**
   * OPEN_STRUCT source that is fully materialized but does not hold {@code key}. Stubs the field
   * spec and doc count that {@code OpenStructNullDataSource.forAbsentKey} reads.
   */
  private static OpenStructDataSource mockFullyMaterializedAbsentKey(String key) {
    return mockFullyMaterializedAbsentKey(key, Map.of());
  }

  /**
   * OPEN_STRUCT source that is fully materialized but does not hold {@code key}. Stubs the field
   * spec and doc count that {@code OpenStructNullDataSource.forAbsentKey} reads. {@code children}
   * carries the declared child specs — pass an empty map for an undeclared key.
   */
  private static OpenStructDataSource mockFullyMaterializedAbsentKey(String key, Map<String, FieldSpec> children) {
    OpenStructDataSource osDs = mock(OpenStructDataSource.class);
    when(osDs.isMaterialized(key)).thenReturn(false);
    when(osDs.isFullyMaterialized()).thenReturn(true);
    when(osDs.getFieldSpec()).thenReturn(
        new ComplexFieldSpec(COLUMN, FieldSpec.DataType.OPEN_STRUCT, true, children));
    DataSourceMetadata osMeta = mock(DataSourceMetadata.class);
    when(osMeta.getNumDocs()).thenReturn(NUM_DOCS);
    when(osDs.getDataSourceMetadata()).thenReturn(osMeta);
    return osDs;
  }

  private static IndexSegment mockSegment(OpenStructDataSource osDs) {
    IndexSegment segment = mock(IndexSegment.class);
    when(segment.getDataSourceNullable(COLUMN)).thenReturn(osDs);
    return segment;
  }

  /// Counts matching docs by iterating. Scan-based operators do not implement getNumMatchingDocs().
  private static int countMatches(MapFilterOperator op) {
    BlockDocIdIterator iterator = op.getTrues().iterator();
    int count = 0;
    while (iterator.next() != Constants.EOF) {
      count++;
    }
    return count;
  }

  /**
   * Materialized key with EQ predicate dispatches to PER_KEY_INDEX.
   */
  @Test
  public void testPerKeyIndexEq() {
    OpenStructDataSource osDs = mock(OpenStructDataSource.class);
    when(osDs.isMaterialized(KEY)).thenReturn(true);

    DataSource keyDs = mock(DataSource.class);
    when(osDs.getDataSource(KEY)).thenReturn(keyDs);

    DataSourceMetadata meta = mock(DataSourceMetadata.class);
    when(meta.getDataType()).thenReturn(FieldSpec.DataType.STRING);
    when(meta.isSorted()).thenReturn(false);
    when(meta.isSingleValue()).thenReturn(true);
    when(keyDs.getDataSourceMetadata()).thenReturn(meta);
    when(keyDs.getColumnName()).thenReturn(KEY);

    // Return null forward index so dictionary is always kept by getDictionaryUsableForFiltering
    when(keyDs.getForwardIndex()).thenReturn(null);

    Dictionary dict = mock(Dictionary.class);
    when(dict.indexOf("active")).thenReturn(0);
    when(keyDs.getDictionary()).thenReturn(dict);

    InvertedIndexReader<?> invertedIndex = mock(InvertedIndexReader.class);
    doReturn(invertedIndex).when(keyDs).getInvertedIndex();

    IndexSegment segment = mock(IndexSegment.class);
    when(segment.getDataSourceNullable(COLUMN)).thenReturn(osDs);

    QueryContext qc = mockQueryContext();
    Predicate predicate = makeEqPredicate(COLUMN, KEY, "active");

    MapFilterOperator op = new MapFilterOperator(segment, predicate, qc, NUM_DOCS);
    assertTrue(op.toExplainString().contains("delegateTo:per_key_index"));
  }

  /**
   * Absent key on a fully materialized segment with EQ → no doc matches (getTrues returns EOF).
   */
  @Test
  public void testAbsentKeyFullyMaterializedEq() {
    OpenStructDataSource osDs = mockFullyMaterializedAbsentKey("missing_key");
    IndexSegment segment = mockSegment(osDs);

    Predicate predicate = makeEqPredicate(COLUMN, "missing_key", "whatever");
    MapFilterOperator op = new MapFilterOperator(segment, predicate, mockQueryContext(), NUM_DOCS);

    assertTrue(op.toExplainString().contains("delegateTo:per_key_index"));
    assertEquals(op.getTrues().iterator().next(), Constants.EOF);
  }

  /**
   * Absent key on a fully materialized segment with IS_NULL → every doc matches.
   */
  @Test
  public void testAbsentKeyFullyMaterializedIsNull() {
    OpenStructDataSource osDs = mockFullyMaterializedAbsentKey("missing_key");
    IndexSegment segment = mockSegment(osDs);

    Predicate predicate = makeIsNullPredicate(COLUMN, "missing_key");
    MapFilterOperator op = new MapFilterOperator(segment, predicate, mockQueryContext(), NUM_DOCS);

    assertTrue(op.toExplainString().contains("delegateTo:per_key_index"));
    assertTrue(op.canOptimizeCount());
    assertEquals(op.getNumMatchingDocs(), NUM_DOCS);
  }

  /**
   * With null handling off, an absent key reads as its type default, so NOT_EQ against any other
   * value must match every doc. Regression test — this previously returned EmptyFilterOperator.
   */
  @Test
  public void testAbsentKeyNotEqMatchesAllWhenNullHandlingOff() {
    OpenStructDataSource osDs = mockFullyMaterializedAbsentKey("missing_key");
    IndexSegment segment = mockSegment(osDs);

    Predicate predicate = makeNotEqPredicate(COLUMN, "missing_key", "whatever");
    MapFilterOperator op = new MapFilterOperator(segment, predicate, mockQueryContext(), NUM_DOCS);

    assertTrue(op.toExplainString().contains("delegateTo:per_key_index"));
    assertTrue(op.canOptimizeCount());
    assertEquals(countMatches(op), NUM_DOCS);
  }

  /**
   * Same as above for NOT_IN.
   */
  @Test
  public void testAbsentKeyNotInMatchesAllWhenNullHandlingOff() {
    OpenStructDataSource osDs = mockFullyMaterializedAbsentKey("missing_key");
    IndexSegment segment = mockSegment(osDs);

    Predicate predicate = makeNotInPredicate(COLUMN, "missing_key", List.of("a", "b"));
    MapFilterOperator op = new MapFilterOperator(segment, predicate, mockQueryContext(), NUM_DOCS);

    assertTrue(op.toExplainString().contains("delegateTo:per_key_index"));
    assertEquals(countMatches(op), NUM_DOCS);
  }

  /**
   * IN against an absent key never matches, regardless of null handling.
   */
  @Test
  public void testAbsentKeyInMatchesNothingWhenNullHandlingOff() {
    OpenStructDataSource osDs = mockFullyMaterializedAbsentKey("missing_key");
    IndexSegment segment = mockSegment(osDs);

    Predicate predicate = makeInPredicate(COLUMN, "missing_key", List.of("a", "b"));
    MapFilterOperator op = new MapFilterOperator(segment, predicate, mockQueryContext(), NUM_DOCS);

    assertEquals(countMatches(op), 0);
  }

  /**
   * A numeric RANGE over an absent key that the schema does not declare. There is no type to
   * recover, so the key resolves through the same STRING fallback {@code item()} uses and the
   * comparison is lexicographic: the default "null" sorts above "100", so every doc matches.
   * Pinned deliberately — filter and projection must not disagree, even when the answer is only
   * meaningful as a string comparison.
   */
  @Test
  public void testAbsentUndeclaredKeyRangeUsesStringFallback() {
    OpenStructDataSource osDs = mockFullyMaterializedAbsentKey("missing_key");
    IndexSegment segment = mockSegment(osDs);

    Predicate predicate = new RangePredicate(itemExpr(COLUMN, "missing_key"), false, "100", false,
        RangePredicate.UNBOUNDED, FieldSpec.DataType.LONG);
    MapFilterOperator op = new MapFilterOperator(segment, predicate, mockQueryContext(), NUM_DOCS);

    assertEquals(countMatches(op), NUM_DOCS);
  }

  /**
   * Same RANGE against a key the schema does declare — the declared type drives the comparison, so
   * LONG's default (Long.MIN_VALUE) is correctly below 100 and nothing matches. The absent key is
   * folded to a constant, so the operator stays countable instead of scanning an all-null column.
   */
  @Test
  public void testAbsentDeclaredKeyRangeMatchesNothing() {
    OpenStructDataSource osDs = mockFullyMaterializedAbsentKey("missing_key",
        Map.of("missing_key", new DimensionFieldSpec("missing_key", FieldSpec.DataType.LONG, true)));
    IndexSegment segment = mockSegment(osDs);

    Predicate predicate = new RangePredicate(itemExpr(COLUMN, "missing_key"), false, "100", false,
        RangePredicate.UNBOUNDED, FieldSpec.DataType.LONG);
    MapFilterOperator op = new MapFilterOperator(segment, predicate, mockQueryContext(), NUM_DOCS);

    assertTrue(op.canOptimizeCount());
    assertEquals(op.getNumMatchingDocs(), 0);
  }

  /**
   * With null handling on, three-valued logic makes every value predicate — including the
   * negations — unmatched for an all-null key.
   */
  @Test
  public void testAbsentKeyNotEqEmptyWhenNullHandlingOn() {
    OpenStructDataSource osDs = mockFullyMaterializedAbsentKey("missing_key");
    IndexSegment segment = mockSegment(osDs);

    Predicate predicate = makeNotEqPredicate(COLUMN, "missing_key", "whatever");
    MapFilterOperator op = new MapFilterOperator(segment, predicate, mockQueryContext(true), NUM_DOCS);

    assertEquals(countMatches(op), 0);
  }

  @Test
  public void testAbsentKeyEqEmptyWhenNullHandlingOn() {
    OpenStructDataSource osDs = mockFullyMaterializedAbsentKey("missing_key");
    IndexSegment segment = mockSegment(osDs);

    Predicate predicate = makeEqPredicate(COLUMN, "missing_key", "whatever");
    MapFilterOperator op = new MapFilterOperator(segment, predicate, mockQueryContext(true), NUM_DOCS);

    assertEquals(countMatches(op), 0);
  }

  @Test
  public void testAbsentKeyIsNullMatchesAllWhenNullHandlingOn() {
    OpenStructDataSource osDs = mockFullyMaterializedAbsentKey("missing_key");
    IndexSegment segment = mockSegment(osDs);

    Predicate predicate = makeIsNullPredicate(COLUMN, "missing_key");
    MapFilterOperator op = new MapFilterOperator(segment, predicate, mockQueryContext(true), NUM_DOCS);

    assertTrue(op.canOptimizeCount());
    assertEquals(op.getNumMatchingDocs(), NUM_DOCS);
  }

  @Test
  public void testAbsentKeyIsNotNullMatchesNothing() {
    OpenStructDataSource osDs = mockFullyMaterializedAbsentKey("missing_key");
    IndexSegment segment = mockSegment(osDs);

    Predicate predicate = makeIsNotNullPredicate(COLUMN, "missing_key");
    MapFilterOperator op = new MapFilterOperator(segment, predicate, mockQueryContext(), NUM_DOCS);

    assertEquals(op.getNumMatchingDocs(), 0);
  }

  /**
   * A predicate the per-key path cannot rewrite (REGEXP_LIKE) must decline rather than fold the
   * absent key to a match-all/match-none it never evaluated. Structured like
   * {@link #testSparseKeyFallsToExpressionFilter} because ExpressionFilterOperator cannot be built
   * against a mock segment.
   */
  @Test
  public void testAbsentKeyUnsupportedPredicateFallsThrough() {
    OpenStructDataSource osDs = mockFullyMaterializedAbsentKey("missing_key");
    when(osDs.getJsonIndex()).thenReturn(null);
    IndexSegment segment = mockSegment(osDs);
    when(segment.getDataSource(COLUMN)).thenReturn(osDs);
    when(osDs.getColumnName()).thenReturn(COLUMN);

    Predicate predicate = new RegexpLikePredicate(itemExpr(COLUMN, "missing_key"), "a.*");
    try {
      MapFilterOperator op = new MapFilterOperator(segment, predicate, mockQueryContext(), NUM_DOCS);
      assertFalse(op.toExplainString().contains("delegateTo:per_key_index"));
    } catch (Exception e) {
      // The per-key path still had to decline before the expression fallback was attempted.
      verify(osDs).isFullyMaterialized();
    }
  }

  /**
   * Non-materialized key on a segment that is NOT fully materialized → falls to EXPRESSION_FILTER.
   */
  @Test
  public void testSparseKeyFallsToExpressionFilter() {
    OpenStructDataSource osDs = mock(OpenStructDataSource.class);
    when(osDs.isMaterialized("sparse_key")).thenReturn(false);
    when(osDs.isFullyMaterialized()).thenReturn(false);
    // No JSON index
    when(osDs.getJsonIndex()).thenReturn(null);

    IndexSegment segment = mock(IndexSegment.class);
    when(segment.getDataSourceNullable(COLUMN)).thenReturn(osDs);
    // ExpressionFilterOperator constructor calls segment.getDataSource(column) for columns in the
    // predicate expression. Return the osDs for the column itself.
    when(segment.getDataSource(COLUMN)).thenReturn(osDs);

    // ExpressionFilterOperator needs column metadata from the DataSource
    DataSourceMetadata meta = mock(DataSourceMetadata.class);
    when(meta.getDataType()).thenReturn(FieldSpec.DataType.STRING);
    when(meta.isSingleValue()).thenReturn(true);
    when(osDs.getDataSourceMetadata()).thenReturn(meta);
    when(osDs.getColumnName()).thenReturn(COLUMN);

    QueryContext qc = mockQueryContext();
    Predicate predicate = makeEqPredicate(COLUMN, "sparse_key", "value");

    // ExpressionFilterOperator's constructor creates a TransformFunction via the factory, which
    // may fail on a mock segment. We verify the dispatch path via isMaterialized/isFullyMaterialized
    // interaction: the per-key path should NOT be entered (getDataSource(key) never called).
    try {
      MapFilterOperator op = new MapFilterOperator(segment, predicate, qc, NUM_DOCS);
      assertTrue(op.toExplainString().contains("delegateTo:expression_filter"));
    } catch (Exception e) {
      // If ExpressionFilterOperator constructor fails on mock internals, that's OK —
      // verify the per-key path was not taken.
      verify(osDs, never()).getDataSource("sparse_key");
      verify(osDs).isMaterialized("sparse_key");
      verify(osDs).isFullyMaterialized();
    }
  }

  /**
   * Materialized key with IS_NOT_NULL and a null bitmap → PER_KEY_INDEX (BitmapBasedFilterOperator).
   */
  @Test
  public void testIsNotNullWithNullBitmap() {
    OpenStructDataSource osDs = mock(OpenStructDataSource.class);
    when(osDs.isMaterialized(KEY)).thenReturn(true);

    DataSource keyDs = mock(DataSource.class);
    when(osDs.getDataSource(KEY)).thenReturn(keyDs);

    // Set up a null bitmap with doc 5 and 10 as null
    MutableRoaringBitmap nullBitmap = new MutableRoaringBitmap();
    nullBitmap.add(5);
    nullBitmap.add(10);

    NullValueVectorReader nullReader = mock(NullValueVectorReader.class);
    when(nullReader.getNullBitmap()).thenReturn(nullBitmap);
    when(keyDs.getNullValueVector()).thenReturn(nullReader);

    IndexSegment segment = mock(IndexSegment.class);
    when(segment.getDataSourceNullable(COLUMN)).thenReturn(osDs);

    QueryContext qc = mockQueryContext();
    Predicate predicate = makeIsNotNullPredicate(COLUMN, KEY);

    MapFilterOperator op = new MapFilterOperator(segment, predicate, qc, NUM_DOCS);
    assertTrue(op.toExplainString().contains("delegateTo:per_key_index"));

    // BitmapBasedFilterOperator with exclusive=true: matches numDocs - nullCount
    assertTrue(op.canOptimizeCount());
    assertEquals(op.getNumMatchingDocs(), NUM_DOCS - 2);
  }
}
