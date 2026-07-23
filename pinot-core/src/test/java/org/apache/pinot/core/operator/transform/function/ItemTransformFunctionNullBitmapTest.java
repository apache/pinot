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
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.NullValueVectorReader;
import org.apache.pinot.spi.data.FieldSpec;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;


/**
 * Validates that {@link ItemTransformFunction#getNullBitmap} returns block-local indices
 * (projected by {@link org.apache.pinot.core.operator.docvalsets.ProjectionBlockValSet})
 * rather than raw segment-level doc IDs from the underlying {@link NullValueVectorReader}.
 */
public class ItemTransformFunctionNullBitmapTest {
  private static final String COLUMN = "myMap";
  private static final String KEY = "foo";

  private AutoCloseable _mocks;

  @Mock private ProjectionBlock _projectionBlock;
  @Mock private BlockValSet _blockValSet;
  @Mock private ColumnContext _columnContext;
  @Mock private MapDataSource _mapDataSource;
  @Mock private DataSource _keyDataSource;
  @Mock private DataSourceMetadata _keyMetadata;
  @Mock private NullValueVectorReader _nullValueVector;

  @BeforeMethod
  @SuppressWarnings("unchecked")
  public void setUp() {
    _mocks = MockitoAnnotations.openMocks(this);

    ForwardIndexReader<?> forwardIndex = mock(ForwardIndexReader.class);
    when(forwardIndex.isDictionaryEncoded()).thenReturn(false);

    when(_columnContext.getDataSource()).thenReturn(_mapDataSource);
    when(_mapDataSource.getDataSource(KEY)).thenReturn(_keyDataSource);
    when(_keyDataSource.getDataSourceMetadata()).thenReturn(_keyMetadata);
    doReturn(forwardIndex).when(_keyDataSource).getForwardIndex();
    when(_keyMetadata.getDataType()).thenReturn(FieldSpec.DataType.STRING);
    when(_keyMetadata.isSingleValue()).thenReturn(true);
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    _mocks.close();
  }

  /// Segment-level null bitmap has doc IDs {100, 200, 300}. The block contains only 5 docs and
  /// the projected (block-local) bitmap is {0, 2}. getNullBitmap must return {0, 2}, not
  /// {100, 200, 300}. Returning segment-level IDs causes ArrayIndexOutOfBoundsException in
  /// callers that index into block-sized arrays.
  @Test
  public void testGetNullBitmapReturnsBlockLocalIndices() {
    // Segment-level bitmap — these are NOT valid block-local indices
    MutableRoaringBitmap segmentBitmap = new MutableRoaringBitmap();
    segmentBitmap.add(100);
    segmentBitmap.add(200);
    segmentBitmap.add(300);
    when(_nullValueVector.getNullBitmap()).thenReturn(segmentBitmap);
    when(_keyDataSource.getNullValueVector()).thenReturn(_nullValueVector);

    // Block-local projected bitmap — what callers actually expect
    RoaringBitmap projectedBitmap = new RoaringBitmap();
    projectedBitmap.add(0);
    projectedBitmap.add(2);
    when(_projectionBlock.getBlockValueSet(any(String[].class))).thenReturn(_blockValSet);
    when(_blockValSet.getNullBitmap()).thenReturn(projectedBitmap);

    ItemTransformFunction fn = initFunction();
    RoaringBitmap result = fn.getNullBitmap(_projectionBlock);

    assertNotNull(result);
    assertEquals(result, projectedBitmap);
    assertTrue(result.contains(0));
    assertTrue(result.contains(2));
    assertFalse(result.contains(100), "Must not contain segment-level doc ID");
  }

  @Test
  public void testGetNullBitmapReturnsNullWhenNoNulls() {
    when(_keyDataSource.getNullValueVector()).thenReturn(_nullValueVector);
    when(_projectionBlock.getBlockValueSet(any(String[].class))).thenReturn(_blockValSet);
    when(_blockValSet.getNullBitmap()).thenReturn(null);

    ItemTransformFunction fn = initFunction();
    assertNull(fn.getNullBitmap(_projectionBlock));
  }

  private ItemTransformFunction initFunction() {
    IdentifierTransformFunction identifierTf = mock(IdentifierTransformFunction.class);
    when(identifierTf.getColumnName()).thenReturn(COLUMN);
    LiteralTransformFunction literalTf = mock(LiteralTransformFunction.class);
    when(literalTf.getStringLiteral()).thenReturn(KEY);

    ItemTransformFunction fn = new ItemTransformFunction();
    fn.init(List.of(identifierTf, literalTf), Map.of(COLUMN, _columnContext));
    return fn;
  }
}
