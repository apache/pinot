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
import org.apache.pinot.common.partition.function.ModuloPartitionFunction;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/// Tests for the [MutableSegmentImpl#index] behavior when `dropRecordOnPartitionMismatch` is enabled.
public class MutableSegmentImplDropRecordOnPartitionMismatchTest {
  private static final String PARTITION_COLUMN = "memberId";
  private static final String VALUE_COLUMN = "teamId";

  // Modulo partition function with 4 partitions; main partition is 0
  private static final int NUM_PARTITIONS = 4;
  private static final int MAIN_PARTITION_ID = 0;

  private static final Schema SCHEMA = new Schema.SchemaBuilder()
      .addSingleValueDimension(PARTITION_COLUMN, FieldSpec.DataType.INT)
      .addSingleValueDimension(VALUE_COLUMN, FieldSpec.DataType.INT)
      .build();

  private MutableSegmentImpl _segment;

  @AfterMethod
  public void tearDown() {
    if (_segment != null) {
      _segment.destroy();
      _segment = null;
    }
  }

  @Test
  public void testRecordsMatchingPartitionAreIndexed()
      throws IOException {
    _segment = MutableSegmentImplTestUtils.createMutableSegmentImpl(SCHEMA, PARTITION_COLUMN,
        new ModuloPartitionFunction(NUM_PARTITIONS, null), MAIN_PARTITION_ID, true);

    // memberId values 0, 4, 8 all map to partition 0
    indexRow(_segment, 0, 100);
    indexRow(_segment, 4, 101);
    indexRow(_segment, 8, 102);

    assertEquals(_segment.getNumDocsIndexed(), 3);
  }

  @Test
  public void testRecordsMismatchingPartitionAreDropped()
      throws IOException {
    _segment = MutableSegmentImplTestUtils.createMutableSegmentImpl(SCHEMA, PARTITION_COLUMN,
        new ModuloPartitionFunction(NUM_PARTITIONS, null), MAIN_PARTITION_ID, true);

    // memberId values 1, 2, 3 map to partitions 1, 2, 3 — all mismatches
    indexRow(_segment, 1, 200);
    indexRow(_segment, 2, 201);
    indexRow(_segment, 3, 202);

    assertEquals(_segment.getNumDocsIndexed(), 0);
  }

  @Test
  public void testMixedPartitionsOnlyIndexMatchingRecords()
      throws IOException {
    _segment = MutableSegmentImplTestUtils.createMutableSegmentImpl(SCHEMA, PARTITION_COLUMN,
        new ModuloPartitionFunction(NUM_PARTITIONS, null), MAIN_PARTITION_ID, true);

    indexRow(_segment, 0, 100);  // partition 0 — indexed
    indexRow(_segment, 1, 101);  // partition 1 — dropped
    indexRow(_segment, 4, 102);  // partition 0 — indexed
    indexRow(_segment, 7, 103);  // partition 3 — dropped

    assertEquals(_segment.getNumDocsIndexed(), 2);
  }

  @Test
  public void testConfigDisabledIndexesAllRecordsRegardlessOfPartition()
      throws IOException {
    _segment = MutableSegmentImplTestUtils.createMutableSegmentImpl(SCHEMA, PARTITION_COLUMN,
        new ModuloPartitionFunction(NUM_PARTITIONS, null), MAIN_PARTITION_ID, false);

    indexRow(_segment, 0, 100);  // partition 0
    indexRow(_segment, 1, 101);  // partition 1
    indexRow(_segment, 2, 102);  // partition 2
    indexRow(_segment, 3, 103);  // partition 3

    assertEquals(_segment.getNumDocsIndexed(), 4);
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testNullPartitionColumnValueThrowsException()
      throws IOException {
    _segment = MutableSegmentImplTestUtils.createMutableSegmentImpl(SCHEMA, PARTITION_COLUMN,
        new ModuloPartitionFunction(NUM_PARTITIONS, null), MAIN_PARTITION_ID, true);

    GenericRow row = new GenericRow();
    row.putValue(PARTITION_COLUMN, null);
    row.putValue(VALUE_COLUMN, 999);
    _segment.index(row, null);
  }

  private void indexRow(MutableSegmentImpl segment, int partitionColumnValue, int valueColumnValue)
      throws IOException {
    GenericRow row = new GenericRow();
    row.putValue(PARTITION_COLUMN, partitionColumnValue);
    row.putValue(VALUE_COLUMN, valueColumnValue);
    segment.index(row, null);
  }
}
