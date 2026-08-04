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
package org.apache.pinot.core.operator.transform.function;

import java.util.List;
import java.util.Map;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.datasource.MapDataSource;
import org.apache.pinot.segment.spi.datasource.OpenStructDataSource;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.roaringbitmap.RoaringBitmap;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;


/// Covers {@link ItemTransformFunction#getNullBitmap} for both backing column types.
///
/// <p>OPEN_STRUCT keeps a per-key presence bitmap, so {@code getNullBitmap} reads the per-key value set and must return
/// block-local indices (projected by
/// {@link org.apache.pinot.core.operator.docvalsets.ProjectionBlockValSet}) rather than raw segment-level doc IDs.
///
/// <p>MAP has no per-key null information, so {@code getNullBitmap} must fall back to
/// {@link BaseTransformFunction#getNullBitmap} — the OR of the argument bitmaps, which yields the MAP column's own
/// null bitmap. Reading the per-key value set for a MAP column would report "no nulls" for a key that is absent from
/// every doc, because an absent MAP key resolves to a
/// {@link org.apache.pinot.segment.local.segment.index.map.NullDataSource} carrying only a forward index.
public class ItemTransformFunctionNullBitmapTest {
  private static final String COLUMN = "myMap";
  private static final String KEY = "foo";
  private static final int NUM_DOCS = 5;

  private AutoCloseable _mocks;

  @Mock
  private ProjectionBlock _projectionBlock;
  @Mock
  private BlockValSet _perKeyBlockValSet;
  @Mock
  private ColumnContext _columnContext;
  @Mock
  private MapDataSource _mapDataSource;
  @Mock
  private OpenStructDataSource _openStructDataSource;
  @Mock
  private DataSource _keyDataSource;
  @Mock
  private DataSourceMetadata _keyMetadata;
  @Mock
  private DataSourceMetadata _parentMetadata;
  @Mock
  private IdentifierTransformFunction _identifierArg;
  @Mock
  private LiteralTransformFunction _literalArg;

  @BeforeMethod
  public void setUp() {
    _mocks = MockitoAnnotations.openMocks(this);

    ForwardIndexReader<?> forwardIndex = mock(ForwardIndexReader.class);
    when(forwardIndex.isDictionaryEncoded()).thenReturn(false);
    doReturn(forwardIndex).when(_keyDataSource).getForwardIndex();
    when(_keyDataSource.getDataSourceMetadata()).thenReturn(_keyMetadata);
    when(_keyMetadata.getDataType()).thenReturn(FieldSpec.DataType.STRING);
    when(_keyMetadata.isSingleValue()).thenReturn(true);

    when(_identifierArg.getColumnName()).thenReturn(COLUMN);
    when(_literalArg.getStringLiteral()).thenReturn(KEY);
    when(_projectionBlock.getBlockValueSet(any(String[].class))).thenReturn(_perKeyBlockValSet);
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    _mocks.close();
  }

  // ---------------------------------------------------------------------------------------------
  // OPEN_STRUCT — per-key null bitmap is authoritative
  // ---------------------------------------------------------------------------------------------

  /// The underlying {@link NullValueVectorReader} holds segment-level doc IDs {100, 200, 300}, but the block contains
  /// only 5 docs and the projected bitmap is {0, 2}. getNullBitmap must return {0, 2}. Returning segment-level IDs
  /// causes ArrayIndexOutOfBoundsException in callers that index into block-sized arrays.
  @Test
  public void testOpenStructReturnsBlockLocalPerKeyIndices() {
    when(_openStructDataSource.getDataSource(KEY)).thenReturn(_keyDataSource);

    RoaringBitmap projectedBitmap = RoaringBitmap.bitmapOf(0, 2);
    when(_perKeyBlockValSet.getNullBitmap()).thenReturn(projectedBitmap);

    RoaringBitmap result = initFunction(_openStructDataSource).getNullBitmap(_projectionBlock);

    assertNotNull(result);
    assertEquals(result, projectedBitmap);
  }

  /// The per-key value set is authoritative for OPEN_STRUCT: the parent column's bitmap must not be consulted, and
  /// must not widen the result.
  @Test
  public void testOpenStructIgnoresParentColumnBitmap() {
    when(_openStructDataSource.getDataSource(KEY)).thenReturn(_keyDataSource);
    when(_perKeyBlockValSet.getNullBitmap()).thenReturn(RoaringBitmap.bitmapOf(0, 2));

    RoaringBitmap result = initFunction(_openStructDataSource).getNullBitmap(_projectionBlock);

    assertEquals(result, RoaringBitmap.bitmapOf(0, 2));
    verify(_identifierArg, never()).getNullBitmap(any());
  }

  @Test
  public void testOpenStructReturnsNullWhenNoNulls() {
    when(_openStructDataSource.getDataSource(KEY)).thenReturn(_keyDataSource);
    when(_perKeyBlockValSet.getNullBitmap()).thenReturn(null);

    assertNull(initFunction(_openStructDataSource).getNullBitmap(_projectionBlock));
  }

  /// A key absent from the OPEN_STRUCT segment resolves to an all-null
  /// {@link org.apache.pinot.segment.local.segment.index.openstruct.OpenStructNullDataSource}, which still exposes
  /// per-key nulls — so the per-key value set stays authoritative.
  @Test
  public void testOpenStructAbsentKeyStillUsesPerKeyBitmap() {
    when(_openStructDataSource.getDataSource(KEY)).thenReturn(null);
    when(_openStructDataSource.getFieldSpec()).thenReturn(
        new ComplexFieldSpec(COLUMN, FieldSpec.DataType.OPEN_STRUCT, true,
            Map.of(KEY, new DimensionFieldSpec(KEY, FieldSpec.DataType.STRING, true))));
    when(_openStructDataSource.getDataSourceMetadata()).thenReturn(_parentMetadata);
    when(_parentMetadata.getNumDocs()).thenReturn(NUM_DOCS);

    RoaringBitmap allNull = RoaringBitmap.bitmapOf(0, 1, 2, 3, 4);
    when(_perKeyBlockValSet.getNullBitmap()).thenReturn(allNull);

    RoaringBitmap result = initFunction(_openStructDataSource).getNullBitmap(_projectionBlock);

    assertEquals(result, allNull);
    verify(_identifierArg, never()).getNullBitmap(any());
  }

  // ---------------------------------------------------------------------------------------------
  // MAP — no per-key null info, fall back to the MAP column's own (conservative) bitmap
  // ---------------------------------------------------------------------------------------------

  /// The per-key value set reports "no nulls" (MAP keeps no per-key null vector), but the MAP column itself is null
  /// for docs {1, 3}. getNullBitmap must report {1, 3}. Reading the per-key value set here would return null and
  /// silently treat every doc as non-null.
  @Test
  public void testMapFallsBackToParentColumnBitmap() {
    when(_mapDataSource.getDataSource(KEY)).thenReturn(_keyDataSource);
    when(_perKeyBlockValSet.getNullBitmap()).thenReturn(null);
    when(_identifierArg.getNullBitmap(_projectionBlock)).thenReturn(RoaringBitmap.bitmapOf(1, 3));

    RoaringBitmap result = initFunction(_mapDataSource).getNullBitmap(_projectionBlock);

    assertNotNull(result, "MAP must fall back to the map column's null bitmap");
    assertEquals(result, RoaringBitmap.bitmapOf(1, 3));
  }

  /// A MAP key absent from the segment resolves to a
  /// {@link org.apache.pinot.segment.local.segment.index.map.NullDataSource}, which carries no null value vector.
  /// The fallback must still surface the MAP column's own nulls rather than reporting none.
  @Test
  public void testMapAbsentKeyFallsBackToParentColumnBitmap() {
    when(_mapDataSource.getDataSource(KEY)).thenReturn(null);
    when(_perKeyBlockValSet.getNullBitmap()).thenReturn(null);
    when(_identifierArg.getNullBitmap(_projectionBlock)).thenReturn(RoaringBitmap.bitmapOf(2));

    RoaringBitmap result = initFunction(_mapDataSource).getNullBitmap(_projectionBlock);

    assertNotNull(result);
    assertEquals(result, RoaringBitmap.bitmapOf(2));
  }

  @Test
  public void testMapReturnsNullWhenMapColumnHasNoNulls() {
    when(_mapDataSource.getDataSource(KEY)).thenReturn(_keyDataSource);
    when(_identifierArg.getNullBitmap(_projectionBlock)).thenReturn(null);

    assertNull(initFunction(_mapDataSource).getNullBitmap(_projectionBlock));
  }

  private ItemTransformFunction initFunction(DataSource columnDataSource) {
    when(_columnContext.getDataSource()).thenReturn(columnDataSource);
    ItemTransformFunction fn = new ItemTransformFunction();
    fn.init(List.of(_identifierArg, _literalArg), Map.of(COLUMN, _columnContext));
    return fn;
  }
}
