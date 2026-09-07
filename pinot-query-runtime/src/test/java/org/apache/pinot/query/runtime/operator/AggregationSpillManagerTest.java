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
package org.apache.pinot.query.runtime.operator;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.spi.utils.CommonConstants.Server;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/// Tests partitioning, serialization, and cleanup behavior of [AggregationSpillManager].
public class AggregationSpillManagerTest {
  @DataProvider(name = "equivalentValues")
  public Object[][] equivalentValues() {
    return new Object[][]{
        {null, null},
        {"key", new String("key")},
        {new Object[]{"key", new int[]{1, 2}}, new Object[]{"key", new int[]{1, 2}}},
        {new byte[]{1, 2}, new byte[]{1, 2}},
        {new short[]{1, 2}, new short[]{1, 2}},
        {new int[]{1, 2}, new int[]{1, 2}},
        {new long[]{1, 2}, new long[]{1, 2}},
        {new char[]{1, 2}, new char[]{1, 2}},
        {new float[]{1, 2}, new float[]{1, 2}},
        {new double[]{1, 2}, new double[]{1, 2}},
        {new boolean[]{true, false}, new boolean[]{true, false}}
    };
  }

  @Test(dataProvider = "equivalentValues")
  public void testDeepHashCode(Object left, Object right) {
    assertEquals(AggregationSpillManager.deepHashCode(left), AggregationSpillManager.deepHashCode(right));
  }

  @DataProvider(name = "partitionConsumers")
  public Object[][] partitionConsumers() {
    return new Object[][]{{false}, {true}};
  }

  @Test(dataProvider = "partitionConsumers")
  public void testConsumePartitionDeletesFileAndCloseIsIdempotent(boolean consumerFails) {
    DataSchema spillSchema = new DataSchema(new String[]{"key"}, new ColumnDataType[]{ColumnDataType.INT});
    AggregationSpillManager spillManager = new AggregationSpillManager(1, 1, spillSchema, new AggregationFunction[0]);
    Path spillDirectory = spillManager.getSpillDirectory();
    spillManager.spill(List.<Object[]>of(new Object[]{1}).iterator());
    assertTrue(spillManager.hasPartition(0));

    if (consumerFails) {
      assertThrows(RuntimeException.class,
          () -> spillManager.consumePartition(0, block -> {
            throw new RuntimeException("consumer failure");
          }));
    } else {
      spillManager.consumePartition(0, block -> assertEquals(block.getNumRows(), 1));
    }

    assertFalse(spillManager.hasPartition(0));
    spillManager.close();
    spillManager.close();
    assertFalse(Files.exists(spillDirectory));
  }

  @Test
  public void testSuccessfulConsumeRetriesCleanupOnClose() {
    DataSchema spillSchema = new DataSchema(new String[]{"key"}, new ColumnDataType[]{ColumnDataType.INT});
    AggregationSpillManager spillManager =
        new AggregationSpillManager(1, 1, spillSchema, new AggregationFunction[0]) {
          @Override
          void deleteSpillFile(Path spillFile) {
            throw new UncheckedIOException("delete failure", new IOException("delete failure"));
          }
        };
    Path spillDirectory = spillManager.getSpillDirectory();
    spillManager.spill(List.<Object[]>of(new Object[]{1}).iterator());

    spillManager.consumePartition(0, block -> assertEquals(block.getNumRows(), 1));

    assertTrue(spillManager.hasPartition(0));
    spillManager.close();
    assertFalse(Files.exists(spillDirectory));
  }

  @Test
  public void testSpillFlushesMultipleBoundedRecords() {
    DataSchema spillSchema = new DataSchema(new String[]{"key"}, new ColumnDataType[]{ColumnDataType.INT});
    AggregationSpillManager spillManager = new AggregationSpillManager(1, 1, spillSchema, new AggregationFunction[0]);
    List<Object[]> rows = new ArrayList<>(1025);
    for (int i = 0; i < 1025; i++) {
      rows.add(new Object[]{i});
    }
    spillManager.spill(rows.iterator());
    List<Integer> recordSizes = new ArrayList<>();

    spillManager.consumePartition(0, block -> recordSizes.add(block.getNumRows()));

    assertEquals(recordSizes, List.of(1024, 1));
    spillManager.close();
  }

  @Test
  public void testSpillWriterCacheIsBounded() {
    int numPartitions = Server.MAX_MSE_AGGREGATION_SPILL_PARTITIONS;
    DataSchema spillSchema = new DataSchema(new String[]{"key"}, new ColumnDataType[]{ColumnDataType.INT});
    AggregationSpillManager spillManager =
        new AggregationSpillManager(numPartitions, 1, spillSchema, new AggregationFunction[0]);
    List<Object[]> rows = new ArrayList<>(numPartitions);
    for (int i = 0; i < numPartitions; i++) {
      rows.add(new Object[]{i});
    }

    spillManager.spill(rows.iterator());

    assertEquals(spillManager.getNumOpenSpillWriters(), numPartitions);
    spillManager.close();
  }

  @Test
  public void testSpillBatchesRowsPerPartition() {
    int numPartitions = Server.DEFAULT_MSE_AGGREGATION_SPILL_PARTITIONS;
    DataSchema spillSchema = new DataSchema(new String[]{"key"}, new ColumnDataType[]{ColumnDataType.INT});
    AggregationSpillManager spillManager =
        new AggregationSpillManager(numPartitions, 1, spillSchema, new AggregationFunction[0]);
    List<Object[]> rows = new ArrayList<>(8192);
    for (int i = 0; i < 8192; i++) {
      rows.add(new Object[]{i});
    }
    spillManager.spill(rows.iterator());
    int[] recordsAndRows = new int[2];

    for (int partitionId = 0; partitionId < numPartitions; partitionId++) {
      spillManager.consumePartition(partitionId, block -> {
        recordsAndRows[0]++;
        recordsAndRows[1] += block.getNumRows();
      });
    }

    assertEquals(recordsAndRows, new int[]{numPartitions, rows.size()});
    spillManager.close();
  }
}
