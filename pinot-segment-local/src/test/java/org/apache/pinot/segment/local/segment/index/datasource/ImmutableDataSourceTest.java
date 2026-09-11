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
package org.apache.pinot.segment.local.segment.index.datasource;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import java.util.Set;
import org.apache.pinot.segment.local.segment.index.map.SimpleColumnMetadata;
import org.apache.pinot.segment.local.segment.virtualcolumn.DocIdVirtualColumnProvider;
import org.apache.pinot.segment.local.segment.virtualcolumn.VirtualColumnContext;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.index.column.ColumnIndexContainer;
import org.apache.pinot.segment.spi.index.metadata.ColumnMetadataImpl;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.segment.spi.partition.PartitionFunctionFactory;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


/// Covers the [DataSourceMetadata] view an [ImmutableDataSource] exposes over the segment's [ColumnMetadata]. The
/// view delegates to the column metadata instead of copying it, so every accessor must report the value (and, for
/// reference types, the instance) the column metadata holds, while keeping the two contracts that differ between the
/// interfaces: `getMaxNumValuesPerMVEntry()` is `-1` for single-value columns and `getMaxRowLengthInBytes()` stays
/// at the [DataSourceMetadata] default of `-1`.
public class ImmutableDataSourceTest {
  private static final int NUM_DOCS = 1000;

  @Test
  public void testSingleValueColumn() {
    FieldSpec fieldSpec = new DimensionFieldSpec("sv", DataType.INT, true);
    PartitionFunction partitionFunction = PartitionFunctionFactory.getPartitionFunction("Modulo", 4, null);
    Set<Integer> partitions = new IntOpenHashSet(new int[]{1, 3});
    ColumnMetadata columnMetadata = new ColumnMetadataImpl.Builder().setFieldSpec(fieldSpec)
        .setTotalDocs(NUM_DOCS)
        .setCardinality(37)
        .setHasDictionary(true)
        .setSorted(true)
        .setMinValue(-5)
        .setMaxValue(123456)
        .setPartitionFunction(partitionFunction)
        .setPartitions(partitions)
        .build();

    DataSourceMetadata metadata = dataSourceMetadata(columnMetadata);
    assertSame(metadata.getFieldSpec(), fieldSpec);
    assertEquals(metadata.getDataType(), DataType.INT);
    assertTrue(metadata.isSingleValue());
    assertTrue(metadata.isSorted());
    assertEquals(metadata.getNumDocs(), NUM_DOCS);
    assertEquals(metadata.getNumValues(), NUM_DOCS);
    assertEquals(metadata.getNumValues(), columnMetadata.getTotalNumberOfEntries());
    assertEquals(metadata.getCardinality(), 37);
    assertSame(metadata.getMinValue(), columnMetadata.getMinValue());
    assertEquals(metadata.getMinValue(), -5);
    assertSame(metadata.getMaxValue(), columnMetadata.getMaxValue());
    assertEquals(metadata.getMaxValue(), 123456);
    assertSame(metadata.getPartitionFunction(), partitionFunction);
    assertSame(metadata.getPartitions(), partitions);

    // Single-value columns report -1 through the data source view although the column metadata canonicalises to 0
    assertEquals(columnMetadata.getMaxNumberOfMultiValues(), 0);
    assertEquals(metadata.getMaxNumValuesPerMVEntry(), -1);

    // The row length is not delegated: the column metadata computes it, the data source view keeps its default
    assertEquals(columnMetadata.getMaxRowLengthInBytes(), Integer.BYTES);
    assertEquals(metadata.getMaxRowLengthInBytes(), -1);
  }

  @Test
  public void testMultiValueColumn() {
    FieldSpec fieldSpec = new DimensionFieldSpec("mv", DataType.STRING, false);
    ColumnMetadata columnMetadata = new ColumnMetadataImpl.Builder().setFieldSpec(fieldSpec)
        .setTotalDocs(NUM_DOCS)
        .setCardinality(11)
        .setHasDictionary(true)
        .setSorted(false)
        .setMinValue("apple")
        .setMaxValue("zebra")
        .setTotalNumberOfEntries(4 * NUM_DOCS)
        .setMaxNumberOfMultiValues(7)
        .setMaxRowLengthInBytes(42)
        .setLengthOfLongestElement(6)
        .build();

    DataSourceMetadata metadata = dataSourceMetadata(columnMetadata);
    assertSame(metadata.getFieldSpec(), fieldSpec);
    assertEquals(metadata.getDataType(), DataType.STRING);
    assertFalse(metadata.isSingleValue());
    assertFalse(metadata.isSorted());
    assertEquals(metadata.getNumDocs(), NUM_DOCS);
    assertEquals(metadata.getNumValues(), 4 * NUM_DOCS);
    assertEquals(metadata.getMaxNumValuesPerMVEntry(), 7);
    assertEquals(metadata.getMaxNumValuesPerMVEntry(), columnMetadata.getMaxNumberOfMultiValues());
    assertEquals(metadata.getCardinality(), 11);
    assertSame(metadata.getMinValue(), columnMetadata.getMinValue());
    assertEquals(metadata.getMinValue(), "apple");
    assertSame(metadata.getMaxValue(), columnMetadata.getMaxValue());
    assertEquals(metadata.getMaxValue(), "zebra");
    assertNull(metadata.getPartitionFunction());
    assertNull(metadata.getPartitions());

    assertEquals(columnMetadata.getMaxRowLengthInBytes(), 42);
    assertEquals(metadata.getMaxRowLengthInBytes(), -1);
  }

  @Test
  public void testVirtualColumn() {
    FieldSpec fieldSpec = new DimensionFieldSpec("$docId", DataType.INT, true);
    DataSource dataSource =
        new DocIdVirtualColumnProvider().buildDataSource(new VirtualColumnContext(fieldSpec, NUM_DOCS));
    assertTrue(dataSource instanceof ImmutableDataSource);

    DataSourceMetadata metadata = dataSource.getDataSourceMetadata();
    assertSame(metadata.getFieldSpec(), fieldSpec);
    assertTrue(metadata.isSingleValue());
    assertTrue(metadata.isSorted());
    assertEquals(metadata.getNumDocs(), NUM_DOCS);
    assertEquals(metadata.getNumValues(), NUM_DOCS);
    assertEquals(metadata.getMaxNumValuesPerMVEntry(), -1);
    assertEquals(metadata.getCardinality(), NUM_DOCS);
    assertNull(metadata.getMinValue());
    assertNull(metadata.getMaxValue());
    assertNull(metadata.getPartitionFunction());
    assertNull(metadata.getPartitions());
    assertEquals(metadata.getMaxRowLengthInBytes(), -1);
  }

  /// A MAP key's data source is built over a [SimpleColumnMetadata] whose stats are all unavailable; the view must
  /// pass them through unchanged rather than re-deriving them.
  @Test
  public void testUnavailableStatsPassThrough() {
    FieldSpec fieldSpec = new DimensionFieldSpec("key", DataType.LONG, false);
    ColumnMetadata columnMetadata = new SimpleColumnMetadata(fieldSpec, NUM_DOCS);

    DataSourceMetadata metadata = dataSourceMetadata(columnMetadata);
    assertSame(metadata.getFieldSpec(), fieldSpec);
    assertFalse(metadata.isSorted());
    assertEquals(metadata.getNumDocs(), NUM_DOCS);
    assertEquals(metadata.getNumValues(), ColumnMetadata.UNAVAILABLE);
    assertEquals(metadata.getMaxNumValuesPerMVEntry(), ColumnMetadata.UNAVAILABLE);
    assertEquals(metadata.getCardinality(), ColumnMetadata.UNAVAILABLE);
    assertNull(metadata.getMinValue());
    assertNull(metadata.getMaxValue());
    assertNull(metadata.getPartitionFunction());
    assertNull(metadata.getPartitions());
    assertEquals(metadata.getMaxRowLengthInBytes(), -1);
  }

  private static DataSourceMetadata dataSourceMetadata(ColumnMetadata columnMetadata) {
    return new ImmutableDataSource(columnMetadata, ColumnIndexContainer.Empty.INSTANCE).getDataSourceMetadata();
  }
}
