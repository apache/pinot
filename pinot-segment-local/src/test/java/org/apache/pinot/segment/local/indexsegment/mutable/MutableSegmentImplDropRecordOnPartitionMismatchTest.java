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
package org.apache.pinot.segment.local.indexsegment.mutable;

import java.io.IOException;
import org.apache.pinot.segment.spi.partition.ModuloPartitionFunction;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/**
 * Tests for the {@link MutableSegmentImpl#index} behavior when
 * {@code dropRecordOnPartitionMismatch} is enabled in the realtime segment config.
 */
public class MutableSegmentImplDropRecordOnPartitionMismatchTest {
  private static final String PARTITION_COLUMN = "memberId";
  private static final String VALUE_COLUMN = "teamId";
  // Modulo partition function with 4 partitions; main partition is 0
  private static final int NUM_PARTITIONS = 4;
  private static final int MAIN_PARTITION_ID = 0;

  private Schema _schema;

  @BeforeMethod
  public void setUp() {
    _schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(PARTITION_COLUMN, FieldSpec.DataType.INT)
        .addSingleValueDimension(VALUE_COLUMN, FieldSpec.DataType.INT)
        .build();
  }

  @Test
  public void testRecordsMatchingPartitionAreIndexed()
      throws IOException {
    MutableSegmentImpl segment = MutableSegmentImplTestUtils.createMutableSegmentImpl(_schema, PARTITION_COLUMN,
        new ModuloPartitionFunction(NUM_PARTITIONS), MAIN_PARTITION_ID, true);

    // memberId values 0, 4, 8 all map to partition 0
    indexRow(segment, 0, 100);
    indexRow(segment, 4, 101);
    indexRow(segment, 8, 102);

    assertEquals(segment.getNumDocsIndexed(), 3);
  }

  @Test
  public void testRecordsMismatchingPartitionAreDropped()
      throws IOException {
    MutableSegmentImpl segment = MutableSegmentImplTestUtils.createMutableSegmentImpl(_schema, PARTITION_COLUMN,
        new ModuloPartitionFunction(NUM_PARTITIONS), MAIN_PARTITION_ID, true);

    // memberId values 1, 2, 3 map to partitions 1, 2, 3 — all mismatches
    indexRow(segment, 1, 200);
    indexRow(segment, 2, 201);
    indexRow(segment, 3, 202);

    assertEquals(segment.getNumDocsIndexed(), 0);
  }

  @Test
  public void testMixedPartitionsOnlyIndexMatchingRecords()
      throws IOException {
    MutableSegmentImpl segment = MutableSegmentImplTestUtils.createMutableSegmentImpl(_schema, PARTITION_COLUMN,
        new ModuloPartitionFunction(NUM_PARTITIONS), MAIN_PARTITION_ID, true);

    indexRow(segment, 0, 100);  // partition 0 — indexed
    indexRow(segment, 1, 101);  // partition 1 — dropped
    indexRow(segment, 4, 102);  // partition 0 — indexed
    indexRow(segment, 7, 103);  // partition 3 — dropped

    assertEquals(segment.getNumDocsIndexed(), 2);
  }

  @Test
  public void testConfigDisabledIndexesAllRecordsRegardlessOfPartition()
      throws IOException {
    MutableSegmentImpl segment = MutableSegmentImplTestUtils.createMutableSegmentImpl(_schema, PARTITION_COLUMN,
        new ModuloPartitionFunction(NUM_PARTITIONS), MAIN_PARTITION_ID, false);

    indexRow(segment, 0, 100);  // partition 0
    indexRow(segment, 1, 101);  // partition 1
    indexRow(segment, 2, 102);  // partition 2
    indexRow(segment, 3, 103);  // partition 3

    assertEquals(segment.getNumDocsIndexed(), 4);
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testNullPartitionColumnValueThrowsException()
      throws IOException {
    MutableSegmentImpl segment = MutableSegmentImplTestUtils.createMutableSegmentImpl(_schema, PARTITION_COLUMN,
        new ModuloPartitionFunction(NUM_PARTITIONS), MAIN_PARTITION_ID, true);

    GenericRow row = new GenericRow();
    row.putValue(PARTITION_COLUMN, null);
    row.putValue(VALUE_COLUMN, 999);
    segment.index(row, null);
  }

  private void indexRow(MutableSegmentImpl segment, int partitionColumnValue, int valueColumnValue)
      throws IOException {
    GenericRow row = new GenericRow();
    row.putValue(PARTITION_COLUMN, partitionColumnValue);
    row.putValue(VALUE_COLUMN, valueColumnValue);
    segment.index(row, null);
  }
}
