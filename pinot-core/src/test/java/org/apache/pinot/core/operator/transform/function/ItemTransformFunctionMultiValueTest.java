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
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.datasource.OpenStructDataSource;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;


/// An OPEN_STRUCT key can hold a list, in which case its value source is multi-value. `ITEM` has to carry that
/// through in two places: the result metadata, which is what the query engine reads the shape off, and the
/// multi-value reads that shape then implies. Without the second, a multi-value key is storable and describable
/// but not readable — the engine asks for values the way the metadata told it to and falls through to the base
/// class, which knows nothing about the key.
public class ItemTransformFunctionMultiValueTest {

  private static final String COLUMN = "props";
  private static final String KEY = "tags";

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
    when(_keyMetadata.getDataType()).thenReturn(DataType.STRING);
    when(_keyMetadata.isSingleValue()).thenReturn(false);

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

  private ItemTransformFunction initFunction() {
    when(_columnContext.getDataSource()).thenReturn(_openStructDataSource);
    ItemTransformFunction fn = new ItemTransformFunction();
    fn.init(List.of(_identifierArg, _literalArg), Map.of(COLUMN, _columnContext));
    return fn;
  }

  @Test
  public void testResultMetadataReportsTheKeyShapeNotTheColumnShape() {
    assertFalse(initFunction().getResultMetadata().isSingleValue(),
        "a key holding lists must report multi-value, whatever the parent column is");
    assertEquals(initFunction().getResultMetadata().getDataType(), DataType.STRING);
  }

  @Test
  public void testStringValuesComeFromTheKeysOwnValueSet() {
    String[][] values = {{"a", "b"}, {"c"}};
    when(_perKeyBlockValSet.getStringValuesMV()).thenReturn(values);

    assertEquals(initFunction().transformToStringValuesMV((ValueBlock) _projectionBlock), values);
  }

  @Test
  public void testIntValuesComeFromTheKeysOwnValueSet() {
    int[][] values = {{1, 2}, {3}};
    when(_perKeyBlockValSet.getIntValuesMV()).thenReturn(values);

    assertEquals(initFunction().transformToIntValuesMV((ValueBlock) _projectionBlock), values);
  }

  @Test
  public void testLongValuesComeFromTheKeysOwnValueSet() {
    long[][] values = {{1L, 2L}, {3L}};
    when(_perKeyBlockValSet.getLongValuesMV()).thenReturn(values);

    assertEquals(initFunction().transformToLongValuesMV((ValueBlock) _projectionBlock), values);
  }

  @Test
  public void testDictIdsComeFromTheKeysOwnValueSet() {
    int[][] dictIds = {{0, 1}, {2}};
    when(_perKeyBlockValSet.getDictionaryIdsMV()).thenReturn(dictIds);

    assertEquals(initFunction().transformToDictIdsMV((ValueBlock) _projectionBlock), dictIds);
  }
}
