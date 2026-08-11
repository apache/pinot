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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
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
import org.apache.pinot.core.common.BlockDocIdSet;
import org.apache.pinot.core.operator.transform.function.ItemTransformFunction;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.local.segment.index.openstruct.FakeStringForwardIndex;
import org.apache.pinot.segment.local.segment.index.openstruct.OpenStructSparseBlobReader;
import org.apache.pinot.segment.local.segment.index.openstruct.SparseKeyDataSource;
import org.apache.pinot.segment.spi.Constants;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.datasource.OpenStructDataSource;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.InvertedIndexReader;
import org.apache.pinot.segment.spi.index.reader.JsonIndexReader;
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

  /// OPEN_STRUCT source that is fully materialized but does not hold {@code key}. Stubs the field
  /// spec and doc count that {@code OpenStructNullDataSource.forAbsentKey} reads.
  private static OpenStructDataSource mockFullyMaterializedAbsentKey(String key) {
    return mockFullyMaterializedAbsentKey(key, Map.of());
  }

  /// OPEN_STRUCT source that is fully materialized but does not hold {@code key}. Stubs the field
  /// spec and doc count that {@code OpenStructNullDataSource.forAbsentKey} reads. {@code children}
  /// carries the declared child specs — pass an empty map for an undeclared key.
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

  /// Even docIds have region ("us" when docId%4==0 else "eu"); odd docIds have empty blobs.
  private static OpenStructDataSource mockSparseSegmentSource(@Nullable List<String> manifest,
      Map<String, FieldSpec> children) {
    String[] blobs = new String[NUM_DOCS];
    for (int i = 0; i < NUM_DOCS; i++) {
      blobs[i] = i % 2 == 0 ? "{\"region\":\"" + (i % 4 == 0 ? "us" : "eu") + "\"}" : null;
    }
    return mockSparseSegmentSource(manifest, children, blobs);
  }

  private static OpenStructDataSource mockSparseSegmentSource(@Nullable List<String> manifest,
      Map<String, FieldSpec> children, String[] blobs) {
    OpenStructSparseBlobReader blob = new OpenStructSparseBlobReader(
        new FakeStringForwardIndex(blobs), FakeStringForwardIndex.nullVector(blobs), NUM_DOCS);
    OpenStructDataSource osDs = mock(OpenStructDataSource.class);
    when(osDs.isMaterialized(anyString())).thenReturn(false);
    when(osDs.isFullyMaterialized()).thenReturn(false);
    when(osDs.getFieldSpec()).thenReturn(
        new ComplexFieldSpec(COLUMN, FieldSpec.DataType.OPEN_STRUCT, true, children));
    DataSourceMetadata osMeta = mock(DataSourceMetadata.class);
    when(osMeta.getNumDocs()).thenReturn(NUM_DOCS);
    when(osDs.getDataSourceMetadata()).thenReturn(osMeta);
    when(osDs.getDataSource(anyString())).thenAnswer(inv -> {
      String key = inv.getArgument(0);
      if (manifest != null && !manifest.contains(key)) {
        return null;
      }
      FieldSpec childSpec = children.get(key);
      if (childSpec == null) {
        childSpec = new DimensionFieldSpec(key, FieldSpec.DataType.STRING, true);
      }
      return new SparseKeyDataSource(childSpec, blob);
    });
    return osDs;
  }

  private static int countMatches(MapFilterOperator op) {
    BlockDocIdIterator iterator = op.getTrues().iterator();
    int count = 0;
    while (iterator.next() != Constants.EOF) {
      count++;
    }
    return count;
  }

  /// Materialized key with EQ predicate dispatches to PER_KEY_INDEX.
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

  /// Absent key on a fully materialized segment with EQ → no doc matches (getTrues returns EOF).
  @Test
  public void testAbsentKeyFullyMaterializedEq() {
    OpenStructDataSource osDs = mockFullyMaterializedAbsentKey("missing_key");
    IndexSegment segment = mockSegment(osDs);

    Predicate predicate = makeEqPredicate(COLUMN, "missing_key", "whatever");
    MapFilterOperator op = new MapFilterOperator(segment, predicate, mockQueryContext(), NUM_DOCS);

    assertTrue(op.toExplainString().contains("delegateTo:per_key_index"));
    assertEquals(op.getTrues().iterator().next(), Constants.EOF);
  }

  /// Absent key on a fully materialized segment with IS_NULL → every doc matches.
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

  /// With null handling off, an absent key reads as its type default, so NOT_EQ against any other
  /// value must match every doc. Regression test — this previously returned EmptyFilterOperator.
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

  /// IN against an absent key never matches, regardless of null handling.
  @Test
  public void testAbsentKeyInMatchesNothingWhenNullHandlingOff() {
    OpenStructDataSource osDs = mockFullyMaterializedAbsentKey("missing_key");
    IndexSegment segment = mockSegment(osDs);

    Predicate predicate = makeInPredicate(COLUMN, "missing_key", List.of("a", "b"));
    MapFilterOperator op = new MapFilterOperator(segment, predicate, mockQueryContext(), NUM_DOCS);

    assertEquals(countMatches(op), 0);
  }

  /// A numeric RANGE over an absent key that the schema does not declare. There is no type to
  /// recover, so the key resolves through the same STRING fallback {@code item()} uses and the
  /// comparison is lexicographic: the default "null" sorts above "100", so every doc matches.
  /// Pinned deliberately — filter and projection must not disagree, even when the answer is only
  /// meaningful as a string comparison.
  @Test
  public void testAbsentUndeclaredKeyRangeUsesStringFallback() {
    OpenStructDataSource osDs = mockFullyMaterializedAbsentKey("missing_key");
    IndexSegment segment = mockSegment(osDs);

    Predicate predicate = new RangePredicate(itemExpr(COLUMN, "missing_key"), false, "100", false,
        RangePredicate.UNBOUNDED, FieldSpec.DataType.LONG);
    MapFilterOperator op = new MapFilterOperator(segment, predicate, mockQueryContext(), NUM_DOCS);

    assertEquals(countMatches(op), NUM_DOCS);
  }

  /// Same RANGE against a key the schema does declare — the declared type drives the comparison, so
  /// LONG's default (Long.MIN_VALUE) is correctly below 100 and nothing matches. The absent key is
  /// folded to a constant, so the operator stays countable instead of scanning an all-null column.
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

  /// With null handling on, three-valued logic makes every value predicate — including the
  /// negations — unmatched for an all-null key.
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

  /// A predicate the per-key path cannot rewrite (REGEXP_LIKE) must decline rather than fold the
  /// absent key to a match-all/match-none it never evaluated. Structured like
  /// {@link #testSparseKeyUnsupportedPredicateStillFallsThrough} because ExpressionFilterOperator
  /// cannot be built against a mock segment.
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

  @Test
  public void testSparseKeyScansVirtualReader() {
    OpenStructDataSource osDs = mockSparseSegmentSource(List.of("region"), Map.of());
    IndexSegment segment = mockSegment(osDs);

    Predicate eqUs = makeEqPredicate(COLUMN, "region", "us");
    MapFilterOperator eqOp = new MapFilterOperator(segment, eqUs, mockQueryContext(), NUM_DOCS);
    assertTrue(eqOp.toExplainString().contains("delegateTo:per_key_index"));
    assertEquals(countMatches(eqOp), 25);

    Predicate neqUs = makeNotEqPredicate(COLUMN, "region", "us");
    MapFilterOperator neqOp = new MapFilterOperator(segment, neqUs, mockQueryContext(), NUM_DOCS);
    assertTrue(neqOp.toExplainString().contains("delegateTo:per_key_index"));
    assertEquals(countMatches(neqOp), 75);
  }

  @Test
  public void testSparseKeyNotEqWithNullHandlingOn() {
    OpenStructDataSource osDs = mockSparseSegmentSource(List.of("region"), Map.of());
    IndexSegment segment = mockSegment(osDs);

    Predicate neqUs = makeNotEqPredicate(COLUMN, "region", "us");
    MapFilterOperator op = new MapFilterOperator(segment, neqUs, mockQueryContext(true), NUM_DOCS);
    assertTrue(op.toExplainString().contains("delegateTo:per_key_index"));
    assertEquals(countMatches(op), 25);
  }

  @Test
  public void testSparseKeyInPredicateScansVirtualReader() {
    OpenStructDataSource osDs = mockSparseSegmentSource(List.of("region"), Map.of());
    IndexSegment segment = mockSegment(osDs);

    Predicate inPred = makeInPredicate(COLUMN, "region", List.of("us", "eu"));
    MapFilterOperator inOp = new MapFilterOperator(segment, inPred, mockQueryContext(), NUM_DOCS);
    assertTrue(inOp.toExplainString().contains("delegateTo:per_key_index"));
    assertEquals(countMatches(inOp), 50);

    Predicate notInPred = makeNotInPredicate(COLUMN, "region", List.of("us", "eu"));
    MapFilterOperator notInOp = new MapFilterOperator(segment, notInPred, mockQueryContext(), NUM_DOCS);
    assertTrue(notInOp.toExplainString().contains("delegateTo:per_key_index"));
    assertEquals(countMatches(notInOp), 50);
  }

  @Test
  public void testManifestMissingKeyShortCircuits() {
    OpenStructDataSource osDs = mockSparseSegmentSource(List.of("region"), Map.of());
    IndexSegment segment = mockSegment(osDs);

    Predicate eqPred = makeEqPredicate(COLUMN, "not_there", "x");
    MapFilterOperator eqOp = new MapFilterOperator(segment, eqPred, mockQueryContext(), NUM_DOCS);
    assertTrue(eqOp.toExplainString().contains("delegateTo:per_key_index"));
    assertEquals(countMatches(eqOp), 0);

    Predicate isNull = makeIsNullPredicate(COLUMN, "not_there");
    MapFilterOperator nullOp = new MapFilterOperator(segment, isNull, mockQueryContext(), NUM_DOCS);
    assertTrue(nullOp.toExplainString().contains("delegateTo:per_key_index"));
    assertEquals(nullOp.getNumMatchingDocs(), NUM_DOCS);
  }

  @Test
  public void testNoManifestFallsBackToVirtualScan() {
    OpenStructDataSource osDs = mockSparseSegmentSource(null, Map.of());
    IndexSegment segment = mockSegment(osDs);

    Predicate eqPred = makeEqPredicate(COLUMN, "ghost_key", "x");
    MapFilterOperator op = new MapFilterOperator(segment, eqPred, mockQueryContext(), NUM_DOCS);
    assertTrue(op.toExplainString().contains("delegateTo:per_key_index"));
    assertEquals(countMatches(op), 0);
  }

  @Test
  public void testSparseKeyIsNullUsesPresenceBitmap() {
    OpenStructDataSource osDs = mockSparseSegmentSource(List.of("region"), Map.of());
    IndexSegment segment = mockSegment(osDs);

    Predicate isNull = makeIsNullPredicate(COLUMN, "region");
    MapFilterOperator nullOp = new MapFilterOperator(segment, isNull, mockQueryContext(true), NUM_DOCS);
    assertTrue(nullOp.toExplainString().contains("delegateTo:per_key_index"));
    assertEquals(nullOp.getNumMatchingDocs(), 50);

    Predicate isNotNull = makeIsNotNullPredicate(COLUMN, "region");
    MapFilterOperator notNullOp = new MapFilterOperator(segment, isNotNull, mockQueryContext(true), NUM_DOCS);
    assertTrue(notNullOp.toExplainString().contains("delegateTo:per_key_index"));
    assertEquals(notNullOp.getNumMatchingDocs(), 50);
  }

  @Test
  public void testSparseKeyRangeOnDeclaredLongScans() {
    String[] blobs = new String[NUM_DOCS];
    for (int i = 0; i < NUM_DOCS; i++) {
      blobs[i] = i % 2 == 0 ? "{\"latencyMs\":" + i + "}" : null;
    }
    Map<String, FieldSpec> children =
        Map.of("latencyMs", new DimensionFieldSpec("latencyMs", FieldSpec.DataType.LONG, true));
    OpenStructDataSource osDs = mockSparseSegmentSource(List.of("latencyMs"), children, blobs);
    IndexSegment segment = mockSegment(osDs);

    Predicate range = new RangePredicate(itemExpr(COLUMN, "latencyMs"), false, "50", false,
        RangePredicate.UNBOUNDED, FieldSpec.DataType.LONG);
    MapFilterOperator op = new MapFilterOperator(segment, range, mockQueryContext(), NUM_DOCS);
    assertTrue(op.toExplainString().contains("delegateTo:per_key_index"));
    assertEquals(countMatches(op), 24);
  }

  @Test
  public void testSparseKeyUnsupportedPredicateStillFallsThrough() {
    OpenStructDataSource osDs = mockSparseSegmentSource(List.of("region"), Map.of());
    when(osDs.getJsonIndex()).thenReturn(null);
    IndexSegment segment = mockSegment(osDs);
    when(segment.getDataSource(COLUMN)).thenReturn(osDs);
    when(osDs.getColumnName()).thenReturn(COLUMN);

    Predicate predicate = new RegexpLikePredicate(itemExpr(COLUMN, "region"), "u.*");
    try {
      MapFilterOperator op = new MapFilterOperator(segment, predicate, mockQueryContext(), NUM_DOCS);
      assertFalse(op.toExplainString().contains("delegateTo:per_key_index"));
    } catch (Exception e) {
      // Per-key path declined; expression fallback attempted but may fail on mock internals.
      verify(osDs).isFullyMaterialized();
    }
  }

  /// Materialized key with IS_NOT_NULL and a null bitmap → PER_KEY_INDEX (BitmapBasedFilterOperator).
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

  /// With null handling on, docs missing the key are UNKNOWN, not FALSE. The operator delegates
  /// getNulls()/getFalses(), so `NOT (col['key'] = v)` — which reads getFalses() — must not match
  /// them. Without the delegation getFalses() is NOT(trues) and wrongly sweeps the null docs in.
  @Test
  public void testNullDocsAreUnknownNotFalseUnderNullHandling() {
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
    when(keyDs.getForwardIndex()).thenReturn(null);

    Dictionary dict = mock(Dictionary.class);
    when(dict.indexOf("active")).thenReturn(0);
    when(keyDs.getDictionary()).thenReturn(dict);

    // Docs 0-2 carry "active"; docs 5 and 10 never set the key at all.
    InvertedIndexReader<?> invertedIndex = mock(InvertedIndexReader.class);
    doReturn(MutableRoaringBitmap.bitmapOf(0, 1, 2)).when(invertedIndex).getDocIds(0);
    doReturn(invertedIndex).when(keyDs).getInvertedIndex();

    NullValueVectorReader nullReader = mock(NullValueVectorReader.class);
    when(nullReader.getNullBitmap()).thenReturn(MutableRoaringBitmap.bitmapOf(5, 10));
    when(keyDs.getNullValueVector()).thenReturn(nullReader);

    IndexSegment segment = mock(IndexSegment.class);
    when(segment.getDataSourceNullable(COLUMN)).thenReturn(osDs);

    MapFilterOperator op = new MapFilterOperator(segment, makeEqPredicate(COLUMN, KEY, "active"),
        mockQueryContext(true), NUM_DOCS);
    assertTrue(op.toExplainString().contains("delegateTo:per_key_index"));

    assertEquals(collect(op.getTrues()), List.of(0, 1, 2));
    assertEquals(collect(op.getNulls()), List.of(5, 10));

    List<Integer> falses = collect(op.getFalses());
    assertFalse(falses.contains(5), "doc 5 is missing the key: UNKNOWN, not FALSE");
    assertFalse(falses.contains(10), "doc 10 is missing the key: UNKNOWN, not FALSE");
    assertTrue(falses.contains(3), "doc 3 has the key with a non-matching value: FALSE");
  }

  private static List<Integer> collect(BlockDocIdSet docIdSet) {
    List<Integer> docIds = new ArrayList<>();
    BlockDocIdIterator iterator = docIdSet.iterator();
    for (int docId = iterator.next(); docId != Constants.EOF; docId = iterator.next()) {
      docIds.add(docId);
    }
    return docIds;
  }

  private static OpenStructDataSource withSparseJsonIndex(OpenStructDataSource osDs,
      JsonIndexReader jsonIndex) {
    when(osDs.getSparseJsonIndex()).thenReturn(jsonIndex);
    return osDs;
  }

  @Test
  public void testSparseJsonIndexEqUsesPostings() {
    JsonIndexReader jsonIndex = mock(JsonIndexReader.class);
    MutableRoaringBitmap postings = new MutableRoaringBitmap();
    postings.add(0);
    postings.add(4);
    when(jsonIndex.getMatchingDocIds(any(FilterContext.class))).thenReturn(postings);

    OpenStructDataSource osDs = withSparseJsonIndex(mockSparseSegmentSource(List.of("region"), Map.of()), jsonIndex);
    MapFilterOperator op = new MapFilterOperator(mockSegment(osDs),
        makeEqPredicate(COLUMN, "region", "us"), mockQueryContext(), NUM_DOCS);

    assertTrue(op.toExplainString().contains("delegateTo:json_match"));
    assertEquals(countMatches(op), 2);
  }

  @Test
  public void testSparseJsonIndexNotEqComplementsPostings() {
    JsonIndexReader jsonIndex = mock(JsonIndexReader.class);
    MutableRoaringBitmap postings = new MutableRoaringBitmap();
    postings.add(0);
    when(jsonIndex.getMatchingDocIds(any(FilterContext.class))).thenReturn(postings);

    OpenStructDataSource osDs = withSparseJsonIndex(mockSparseSegmentSource(List.of("region"), Map.of()), jsonIndex);
    MapFilterOperator op = new MapFilterOperator(mockSegment(osDs),
        makeNotEqPredicate(COLUMN, "region", "us"), mockQueryContext(), NUM_DOCS);

    assertTrue(op.toExplainString().contains("delegateTo:json_match"));
    assertEquals(countMatches(op), NUM_DOCS - 1);
  }

  @Test
  public void testSparseJsonIndexRefusals() {
    JsonIndexReader jsonIndex = mock(JsonIndexReader.class);

    // (a) EQ against the STRING default "null"
    OpenStructDataSource a = withSparseJsonIndex(mockSparseSegmentSource(List.of("region"), Map.of()), jsonIndex);
    MapFilterOperator opA = new MapFilterOperator(mockSegment(a),
        makeEqPredicate(COLUMN, "region", "null"), mockQueryContext(), NUM_DOCS);
    assertTrue(opA.toExplainString().contains("delegateTo:per_key_index"));
    assertEquals(countMatches(opA), NUM_DOCS / 2);

    // (b) NOT_EQ with null handling on
    OpenStructDataSource b = withSparseJsonIndex(mockSparseSegmentSource(List.of("region"), Map.of()), jsonIndex);
    MapFilterOperator opB = new MapFilterOperator(mockSegment(b),
        makeNotEqPredicate(COLUMN, "region", "us"), mockQueryContext(true), NUM_DOCS);
    assertTrue(opB.toExplainString().contains("delegateTo:per_key_index"));

    // (c) numeric declared type
    String[] longBlobs = new String[NUM_DOCS];
    for (int i = 0; i < NUM_DOCS; i++) {
      longBlobs[i] = i % 2 == 0 ? "{\"latencyMs\":" + i + "}" : null;
    }
    Map<String, FieldSpec> children =
        Map.of("latencyMs", new DimensionFieldSpec("latencyMs", FieldSpec.DataType.LONG, true));
    OpenStructDataSource c = withSparseJsonIndex(
        mockSparseSegmentSource(List.of("latencyMs"), children, longBlobs), jsonIndex);
    MapFilterOperator opC = new MapFilterOperator(mockSegment(c),
        makeEqPredicate(COLUMN, "latencyMs", "42"), mockQueryContext(), NUM_DOCS);
    assertTrue(opC.toExplainString().contains("delegateTo:per_key_index"));

    verify(jsonIndex, never()).getMatchingDocIds(any(FilterContext.class));
  }
}
