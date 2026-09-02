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
package org.apache.pinot.segment.local.segment.index.map;

import java.util.Map;
import org.apache.pinot.segment.local.segment.index.datasource.NullDataSource;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.index.column.ColumnIndexContainer;
import org.apache.pinot.segment.spi.index.reader.MapIndexReader;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.data.ComplexFieldSpec.KEY_FIELD;
import static org.apache.pinot.spi.data.ComplexFieldSpec.VALUE_FIELD;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// Covers [BaseMapDataSource#getDataSource] on the branch taken when the map index reports the key absent: the
/// key must resolve to an all-null [NullDataSource] typed as the map's value field and spanning the doc count.
public class BaseMapDataSourceTest {
  private static final int NUM_DOCS = 100;

  @Test
  public void testAbsentKeyResolvesToAllNullSourceOfValueType() {
    ComplexFieldSpec mapFieldSpec = new ComplexFieldSpec("m", DataType.MAP, true, Map.of(
        KEY_FIELD, new DimensionFieldSpec(KEY_FIELD, DataType.STRING, true),
        VALUE_FIELD, new DimensionFieldSpec(VALUE_FIELD, DataType.LONG, true)
    ));
    DataSourceMetadata mapMetadata = mock(DataSourceMetadata.class);
    when(mapMetadata.getFieldSpec()).thenReturn(mapFieldSpec);
    when(mapMetadata.getNumDocs()).thenReturn(NUM_DOCS);
    MapIndexReader<?> mapIndexReader = mock(MapIndexReader.class);
    when(mapIndexReader.getIndexes("absent")).thenReturn(null);
    BaseMapDataSource mapDataSource = new BaseMapDataSource(mapMetadata, ColumnIndexContainer.Empty.INSTANCE) {
      @Override
      public MapIndexReader<?> getMapIndexReader() {
        return mapIndexReader;
      }
    };

    DataSource keyDataSource = mapDataSource.getDataSource("absent");
    assertTrue(keyDataSource instanceof NullDataSource);
    DataSourceMetadata metadata = keyDataSource.getDataSourceMetadata();
    assertEquals(metadata.getDataType(), DataType.LONG);
    assertEquals(metadata.getNumDocs(), NUM_DOCS);
    assertEquals(keyDataSource.getDictionary().get(0), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_LONG);
  }
}
