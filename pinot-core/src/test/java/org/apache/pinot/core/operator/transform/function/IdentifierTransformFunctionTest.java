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

import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.roaringbitmap.RoaringBitmap;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.when;


public class IdentifierTransformFunctionTest {
  private static final int NUM_DOCS = 100;

  private static final int[] INT_VALUES;

  private static final RoaringBitmap NULL_BITMAP;

  static {
    INT_VALUES = new int[100];
    NULL_BITMAP = new RoaringBitmap();
    for (int i = 0; i < NUM_DOCS; i++) {
      INT_VALUES[i] = i;
      if (i % 2 == 0) {
        NULL_BITMAP.add(i);
      }
    }
  }

  private static final String TEST_COLUMN_NAME = "testColumn";

  private AutoCloseable _mocks;

  @Mock
  private ColumnContext _columnContext;

  @Mock
  private ProjectionBlock _projectionBlock;

  @Mock
  private BlockValSet _blockValSet;

  @Mock
  private DataSource _dataSource;

  @Mock
  private Dictionary _dictionary;

  @Mock
  private DataSourceMetadata _metadata;

  @BeforeMethod
  public void setUp() {
    _mocks = MockitoAnnotations.openMocks(this);
    when(_projectionBlock.getNumDocs()).thenReturn(NUM_DOCS);
    when(_blockValSet.getIntValuesSV()).thenReturn(INT_VALUES);
    when(_blockValSet.getNullBitmap()).thenReturn(NULL_BITMAP);
    when(_projectionBlock.getBlockValueSet(TEST_COLUMN_NAME)).thenReturn(_blockValSet);
    when(_columnContext.getDataSource()).thenReturn(_dataSource);
    when(_dataSource.getDictionary()).thenReturn(_dictionary);
    when(_dataSource.getDataSourceMetadata()).thenReturn(_metadata);
    when(_metadata.getDataType()).thenReturn(FieldSpec.DataType.INT);
    when(_metadata.isSingleValue()).thenReturn(true);
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    _mocks.close();
  }

  @Test
  public void testNullBitmap() {
    IdentifierTransformFunction identifierTransformFunction =
        new IdentifierTransformFunction(TEST_COLUMN_NAME, _columnContext);
    RoaringBitmap bitmap = identifierTransformFunction.getNullBitmap(_projectionBlock);
    Assert.assertEquals(bitmap, NULL_BITMAP);
    Assert.assertEquals(identifierTransformFunction.transformToIntValuesSV(_projectionBlock), INT_VALUES);
  }
}
