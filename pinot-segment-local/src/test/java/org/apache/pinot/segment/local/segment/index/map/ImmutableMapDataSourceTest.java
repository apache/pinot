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

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.index.IndexReader;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.column.ColumnIndexContainer;
import org.apache.pinot.segment.spi.index.metadata.ColumnMetadataImpl;
import org.apache.pinot.segment.spi.index.reader.MapIndexReader;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.segment.spi.partition.PartitionFunctionFactory;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.data.ComplexFieldSpec.KEY_FIELD;
import static org.apache.pinot.spi.data.ComplexFieldSpec.VALUE_FIELD;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/// Covers the [DataSourceMetadata] view an [ImmutableMapDataSource] exposes over the MAP column's [ColumnMetadata].
/// The view delegates to the column metadata instead of copying it, except for the two MAP-specific contracts: the
/// column is never reported as sorted, and `getMaxRowLengthInBytes()` is unsupported.
public class ImmutableMapDataSourceTest {
  private static final int NUM_DOCS = 1000;

  @Test
  public void testDelegatesToColumnMetadata() {
    ComplexFieldSpec fieldSpec = new ComplexFieldSpec("m", DataType.MAP, true, Map.of(
        KEY_FIELD, new DimensionFieldSpec(KEY_FIELD, DataType.STRING, true),
        VALUE_FIELD, new DimensionFieldSpec(VALUE_FIELD, DataType.LONG, true)
    ));
    PartitionFunction partitionFunction = PartitionFunctionFactory.getPartitionFunction("Modulo", 2, null);
    Set<Integer> partitions = new IntOpenHashSet(new int[]{0});
    // Sorted is set on purpose: the MAP view must ignore it
    ColumnMetadata columnMetadata = new ColumnMetadataImpl.Builder().setFieldSpec(fieldSpec)
        .setTotalDocs(NUM_DOCS)
        .setCardinality(5)
        .setSorted(true)
        .setMinValue("a")
        .setMaxValue("z")
        .setPartitionFunction(partitionFunction)
        .setPartitions(partitions)
        .build();
    Map<IndexType, IndexReader> indexes = Map.of(StandardIndexes.forward(), mock(MapIndexReader.class));
    ColumnIndexContainer indexContainer = new ColumnIndexContainer.FromMap(indexes);

    DataSourceMetadata metadata = new ImmutableMapDataSource(columnMetadata, indexContainer).getDataSourceMetadata();
    assertSame(metadata.getFieldSpec(), fieldSpec);
    assertEquals(metadata.getDataType(), DataType.MAP);
    assertTrue(metadata.isSingleValue());
    assertTrue(columnMetadata.isSorted());
    assertFalse(metadata.isSorted());
    assertEquals(metadata.getNumDocs(), NUM_DOCS);
    assertEquals(metadata.getNumValues(), NUM_DOCS);
    assertEquals(metadata.getMaxNumValuesPerMVEntry(), -1);
    assertEquals(metadata.getCardinality(), 5);
    assertSame(metadata.getMinValue(), columnMetadata.getMinValue());
    assertEquals(metadata.getMinValue(), "a");
    assertSame(metadata.getMaxValue(), columnMetadata.getMaxValue());
    assertEquals(metadata.getMaxValue(), "z");
    assertSame(metadata.getPartitionFunction(), partitionFunction);
    assertSame(metadata.getPartitions(), partitions);
    assertThrows(UnsupportedOperationException.class, metadata::getMaxRowLengthInBytes);
  }
}
