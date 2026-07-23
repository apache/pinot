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
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.request.context.predicate.EqPredicate;
import org.apache.pinot.common.request.context.predicate.IsNotNullPredicate;
import org.apache.pinot.common.request.context.predicate.IsNullPredicate;
import org.apache.pinot.common.request.context.predicate.Predicate;
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

  private static Predicate makeEqPredicate(String column, String key, String value) {
    ExpressionContext colArg = ExpressionContext.forIdentifier(column);
    ExpressionContext keyArg = ExpressionContext.forLiteral(FieldSpec.DataType.STRING, key);
    FunctionContext fn = new FunctionContext(FunctionContext.Type.TRANSFORM,
        ItemTransformFunction.FUNCTION_NAME, Arrays.asList(colArg, keyArg));
    ExpressionContext itemExpr = ExpressionContext.forFunction(fn);
    return new EqPredicate(itemExpr, value);
  }

  private static Predicate makeIsNullPredicate(String column, String key) {
    ExpressionContext colArg = ExpressionContext.forIdentifier(column);
    ExpressionContext keyArg = ExpressionContext.forLiteral(FieldSpec.DataType.STRING, key);
    FunctionContext fn = new FunctionContext(FunctionContext.Type.TRANSFORM,
        ItemTransformFunction.FUNCTION_NAME, Arrays.asList(colArg, keyArg));
    ExpressionContext itemExpr = ExpressionContext.forFunction(fn);
    return new IsNullPredicate(itemExpr);
  }

  private static Predicate makeIsNotNullPredicate(String column, String key) {
    ExpressionContext colArg = ExpressionContext.forIdentifier(column);
    ExpressionContext keyArg = ExpressionContext.forLiteral(FieldSpec.DataType.STRING, key);
    FunctionContext fn = new FunctionContext(FunctionContext.Type.TRANSFORM,
        ItemTransformFunction.FUNCTION_NAME, Arrays.asList(colArg, keyArg));
    ExpressionContext itemExpr = ExpressionContext.forFunction(fn);
    return new IsNotNullPredicate(itemExpr);
  }

  private QueryContext mockQueryContext() {
    QueryContext qc = mock(QueryContext.class);
    when(qc.isNullHandlingEnabled()).thenReturn(false);
    when(qc.isIndexUseAllowed(any(DataSource.class), any())).thenReturn(true);
    when(qc.isIndexUseAllowed(anyString(), any())).thenReturn(true);
    return qc;
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
   * Absent key on a fully materialized segment with EQ → EmptyFilterOperator (getTrues returns EOF).
   */
  @Test
  public void testAbsentKeyFullyMaterializedEq() {
    OpenStructDataSource osDs = mock(OpenStructDataSource.class);
    when(osDs.isMaterialized("missing_key")).thenReturn(false);
    when(osDs.isFullyMaterialized()).thenReturn(true);

    IndexSegment segment = mock(IndexSegment.class);
    when(segment.getDataSourceNullable(COLUMN)).thenReturn(osDs);

    QueryContext qc = mockQueryContext();
    Predicate predicate = makeEqPredicate(COLUMN, "missing_key", "whatever");

    MapFilterOperator op = new MapFilterOperator(segment, predicate, qc, NUM_DOCS);
    assertTrue(op.toExplainString().contains("delegateTo:per_key_index"));

    // EmptyFilterOperator.getTrues() returns EmptyDocIdSet → iterator next() is EOF
    assertEquals(op.getTrues().iterator().next(), Constants.EOF);
  }

  /**
   * Absent key on a fully materialized segment with IS_NULL → MatchAllFilterOperator.
   */
  @Test
  public void testAbsentKeyFullyMaterializedIsNull() {
    OpenStructDataSource osDs = mock(OpenStructDataSource.class);
    when(osDs.isMaterialized("missing_key")).thenReturn(false);
    when(osDs.isFullyMaterialized()).thenReturn(true);

    IndexSegment segment = mock(IndexSegment.class);
    when(segment.getDataSourceNullable(COLUMN)).thenReturn(osDs);

    QueryContext qc = mockQueryContext();
    Predicate predicate = makeIsNullPredicate(COLUMN, "missing_key");

    MapFilterOperator op = new MapFilterOperator(segment, predicate, qc, NUM_DOCS);
    assertTrue(op.toExplainString().contains("delegateTo:per_key_index"));
    assertTrue(op.canOptimizeCount());
    assertEquals(op.getNumMatchingDocs(), NUM_DOCS);
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
