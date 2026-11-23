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

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.datasource.OpenStructDataSource;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.roaringbitmap.RoaringBitmap;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;


/// A single-value OPEN_STRUCT key is read through whichever `transformTo*ValuesSV` matches its own type. The base
/// class can only *convert* into a type, so a key whose type is already the one being asked for has to be read
/// straight off its value set. Without that, `BaseTransformFunction` reaches its conversion switch, finds no case
/// for the key's own type and fails the query -- `Cannot read SV BIG_DECIMAL as BIG_DECIMAL` -- so a decimal key
/// is unreadable however it was written.
public class ItemTransformFunctionScalarTypeTest {

  private static final String COLUMN = "props";
  private static final String KEY = "amount";

  private AutoCloseable _mocks;

  @Mock
  private ProjectionBlock _projectionBlock;
  @Mock
  private BlockValSet _perKeyBlockValSet;
  @Mock
  private ColumnContext _columnContext;
  @Mock
  private OpenStructDataSource _openStructDataSource;
  @Mock
  private DataSource _keyDataSource;
  @Mock
  private DataSourceMetadata _keyMetadata;
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
    when(_keyMetadata.isSingleValue()).thenReturn(true);

    when(_openStructDataSource.getDataSource(KEY)).thenReturn(_keyDataSource);
    when(_identifierArg.getColumnName()).thenReturn(COLUMN);
    when(_literalArg.getStringLiteral()).thenReturn(KEY);
    when(_projectionBlock.getBlockValueSet(any(String[].class))).thenReturn(_perKeyBlockValSet);
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    _mocks.close();
  }

  private ItemTransformFunction initFunction(DataType keyType) {
    when(_keyMetadata.getDataType()).thenReturn(keyType);
    when(_columnContext.getDataSource()).thenReturn(_openStructDataSource);
    ItemTransformFunction fn = new ItemTransformFunction();
    fn.init(List.of(_identifierArg, _literalArg), Map.of(COLUMN, _columnContext));
    return fn;
  }

  @Test
  public void testPlannedLogicalTypeOverStringSegment() {
    when(_keyMetadata.getDataType()).thenReturn(DataType.STRING);
    when(_columnContext.getDataSource()).thenReturn(_openStructDataSource);
    when(_perKeyBlockValSet.getValueType()).thenReturn(DataType.STRING);
    when(_perKeyBlockValSet.isSingleValue()).thenReturn(true);
    when(_projectionBlock.getNumDocs()).thenReturn(1);
    LiteralTransformFunction target = mock(LiteralTransformFunction.class);
    when(target.getStringLiteral()).thenReturn("BOOLEAN", "TIMESTAMP");

    ItemTransformFunction flag = new ItemTransformFunction();
    flag.init(List.of(_identifierArg, _literalArg, target), Map.of(COLUMN, _columnContext));
    when(_perKeyBlockValSet.getStringValuesSV()).thenReturn(new String[]{"true"});
    assertEquals(flag.transformToIntValuesSV(_projectionBlock), new int[]{1});

    ItemTransformFunction timestamp = new ItemTransformFunction();
    timestamp.init(List.of(_identifierArg, _literalArg, target), Map.of(COLUMN, _columnContext));
    String text = "2024-01-01 00:00:00";
    when(_perKeyBlockValSet.getStringValuesSV()).thenReturn(new String[]{text});
    assertEquals(timestamp.transformToLongValuesSV(_projectionBlock), new long[]{Timestamp.valueOf(text).getTime()});
  }

  @Test
  public void testMissingKeyUsesPlannedFieldDefaultWithNullHandlingDisabled() {
    when(_keyMetadata.getDataType()).thenReturn(DataType.STRING);
    when(_columnContext.getDataSource()).thenReturn(_openStructDataSource);
    when(_perKeyBlockValSet.getValueType()).thenReturn(DataType.STRING);
    when(_perKeyBlockValSet.isSingleValue()).thenReturn(true);
    when(_perKeyBlockValSet.getStringValuesSV()).thenReturn(new String[]{"17", "null"});
    when(_perKeyBlockValSet.getNullBitmap()).thenReturn(RoaringBitmap.bitmapOf(1));
    when(_projectionBlock.getNumDocs()).thenReturn(2);
    LiteralTransformFunction target = mock(LiteralTransformFunction.class);
    when(target.getStringLiteral()).thenReturn("LONG_ARRAY");
    Schema schema = new Schema();
    schema.addField(new ComplexFieldSpec(COLUMN, DataType.OPEN_STRUCT, true,
        Map.of(KEY, new DimensionFieldSpec(KEY, DataType.LONG, false, -7L))));
    QueryContext queryContext = mock(QueryContext.class);
    when(queryContext.getSchema()).thenReturn(schema);

    ItemTransformFunction fn = new ItemTransformFunction();
    fn.init(List.of(_identifierArg, _literalArg, target), Map.of(COLUMN, _columnContext), queryContext);
    assertEquals(fn.transformToLongValuesMV(_projectionBlock), new long[][]{{17}, {-7}});
  }

  @Test
  public void testBigDecimalValuesComeFromTheKeysOwnValueSet() {
    BigDecimal[] values = {new BigDecimal("12.5"), new BigDecimal("-0.001")};
    when(_perKeyBlockValSet.getBigDecimalValuesSV()).thenReturn(values);

    assertEquals(initFunction(DataType.BIG_DECIMAL).transformToBigDecimalValuesSV((ValueBlock) _projectionBlock),
        values);
  }

  @Test
  public void testFloatValuesComeFromTheKeysOwnValueSet() {
    float[] values = {1.5f, -2.25f};
    when(_perKeyBlockValSet.getFloatValuesSV()).thenReturn(values);

    assertEquals(initFunction(DataType.FLOAT).transformToFloatValuesSV((ValueBlock) _projectionBlock), values);
  }

  @Test
  public void testBytesValuesComeFromTheKeysOwnValueSet() {
    byte[][] values = {{1, 2, 3}, {4}};
    when(_perKeyBlockValSet.getBytesValuesSV()).thenReturn(values);

    assertEquals(initFunction(DataType.BYTES).transformToBytesValuesSV((ValueBlock) _projectionBlock), values);
  }

  /// A decimal key is still readable as the other numeric types, which is what an expression wrapping the key in
  /// arithmetic asks for. Those reads go to the key's own value set too, so the fix cannot be to narrow the type the
  /// key reports -- that would send the identity read through a lossy conversion to make it match a case.
  @Test
  public void testDecimalKeyStillReadsAsDouble() {
    when(_perKeyBlockValSet.getDoubleValuesSV()).thenReturn(new double[]{12.5d, -0.001d});

    assertEquals(initFunction(DataType.BIG_DECIMAL).transformToDoubleValuesSV((ValueBlock) _projectionBlock),
        new double[]{12.5d, -0.001d});
  }
}
