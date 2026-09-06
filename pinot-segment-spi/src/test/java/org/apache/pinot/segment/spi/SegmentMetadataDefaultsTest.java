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
package org.apache.pinot.segment.spi;

import java.util.List;
import java.util.SortedSet;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.testng.annotations.Test;

import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;
import static org.testng.Assert.assertEquals;


/// Covers the [SegmentMetadata] default methods against an implementation that holds no column metadata, which
/// [SegmentMetadata#getColumnMetadataMap()] is documented to answer with `null`. The only implementation in the tree
/// overrides [SegmentMetadata#getAllColumnMetadata()], so the defaults are exercised here rather than through it.
public class SegmentMetadataDefaultsTest {

  @Test
  public void testAllColumnMetadataIsEmptyWithoutAColumnMetadataMap() {
    SegmentMetadata segmentMetadata = metadataHoldingNoColumns();
    assertEquals(segmentMetadata.getAllColumnMetadata(), List.of());
  }

  @Test
  public void testPhysicalColumnNamesFallBackToTheSchemaWithoutColumnMetadata() {
    SegmentMetadata segmentMetadata = metadataHoldingNoColumns();
    assertEquals(segmentMetadata.getPhysicalColumnNames(), schema().getPhysicalColumnNames());
  }

  @Test
  public void testPhysicalColumnNamesAreSortedAndSkipVirtualColumns() {
    ColumnMetadata zebra = columnMetadata("zebra", false);
    ColumnMetadata apple = columnMetadata("apple", false);
    ColumnMetadata virtual = columnMetadata("$docId", true);
    SegmentMetadata segmentMetadata = mock(SegmentMetadata.class, CALLS_REAL_METHODS);
    when(segmentMetadata.getAllColumnMetadata()).thenReturn(List.of(zebra, apple, virtual));

    SortedSet<String> physicalColumnNames = segmentMetadata.getPhysicalColumnNames();
    assertEquals(List.copyOf(physicalColumnNames), List.of("apple", "zebra"));
  }

  private static SegmentMetadata metadataHoldingNoColumns() {
    SegmentMetadata segmentMetadata = mock(SegmentMetadata.class, withSettings().defaultAnswer(CALLS_REAL_METHODS));
    when(segmentMetadata.getColumnMetadataMap()).thenReturn(null);
    when(segmentMetadata.getSchema()).thenReturn(schema());
    return segmentMetadata;
  }

  private static Schema schema() {
    return new Schema.SchemaBuilder()
        .addSingleValueDimension("zebra", FieldSpec.DataType.STRING)
        .addSingleValueDimension("apple", FieldSpec.DataType.STRING)
        .build();
  }

  private static ColumnMetadata columnMetadata(String column, boolean virtual) {
    DimensionFieldSpec fieldSpec = new DimensionFieldSpec(column, FieldSpec.DataType.STRING, true);
    if (virtual) {
      fieldSpec.setVirtualColumnProvider("org.apache.pinot.segment.spi.virtualcolumn.DocIdVirtualColumnProvider");
    }
    ColumnMetadata columnMetadata = mock(ColumnMetadata.class);
    when(columnMetadata.getFieldSpec()).thenReturn(fieldSpec);
    return columnMetadata;
  }
}
