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
package org.apache.pinot.core.segment.processing.framework;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.pinot.core.segment.processing.partitioner.ColumnValuePartitioner;
import org.apache.pinot.core.segment.processing.partitioner.NoOpPartitioner;
import org.apache.pinot.core.segment.processing.partitioner.Partitioner;
import org.apache.pinot.core.segment.processing.partitioner.PartitionerConfig;
import org.apache.pinot.core.segment.processing.partitioner.PartitionerFactory;
import org.apache.pinot.core.segment.processing.partitioner.RoundRobinPartitioner;
import org.apache.pinot.core.segment.processing.partitioner.TableConfigPartitioner;
import org.apache.pinot.core.segment.processing.partitioner.TransformFunctionPartitioner;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


/**
 * Tests for {@link Partitioner}
 */
public class PartitionerTest {

  @Test
  public void testNoOpPartitioner() {
    PartitionerConfig partitionerConfig = new PartitionerConfig.Builder().build();
    Partitioner partitioner = PartitionerFactory.getPartitioner(partitionerConfig);
    assertEquals(partitioner.getClass(), NoOpPartitioner.class);

    GenericRow row = new GenericRow();
    assertEquals(partitioner.getPartition(row), "0");
    row.putValue("dim", "aDimValue");
    row.putValue("metric", "aMetricValue");
    row.putValue("time", "aTimeValue");
    assertEquals(partitioner.getPartition(row), "0");
  }

  @Test
  public void testColumnValuePartitioner() {
    PartitionerConfig partitionerConfig =
        new PartitionerConfig.Builder().setPartitionerType(PartitionerFactory.PartitionerType.COLUMN_VALUE).build();
    try {
      PartitionerFactory.getPartitioner(partitionerConfig);
      fail("Should not create COLUMN_VALUE Partitioner without column name");
    } catch (IllegalStateException e) {
      // expected
    }
    partitionerConfig =
        new PartitionerConfig.Builder().setPartitionerType(PartitionerFactory.PartitionerType.COLUMN_VALUE)
            .setColumnName("foo").build();
    Partitioner partitioner = PartitionerFactory.getPartitioner(partitionerConfig);
    assertEquals(partitioner.getClass(), ColumnValuePartitioner.class);

    GenericRow row = new GenericRow();
    assertEquals(partitioner.getPartition(row), "null");
    row.putValue("foo", "20191120");
    assertEquals(partitioner.getPartition(row), "20191120");
  }

  @Test
  public void testRoundRobinPartitioner() {
    PartitionerConfig partitionerConfig =
        new PartitionerConfig.Builder().setPartitionerType(PartitionerFactory.PartitionerType.ROUND_ROBIN).build();
    try {
      PartitionerFactory.getPartitioner(partitionerConfig);
      fail("Should not create ROUND_ROBIN Partitioner without num partitions");
    } catch (IllegalStateException e) {
      // expected
    }
    partitionerConfig =
        new PartitionerConfig.Builder().setPartitionerType(PartitionerFactory.PartitionerType.ROUND_ROBIN)
            .setNumPartitions(0).build();
    try {
      PartitionerFactory.getPartitioner(partitionerConfig);
      fail("Should not create ROUND_ROBIN Partitioner without num partitions <=0");
    } catch (IllegalStateException e) {
      // expected
    }
    int numPartitions = 3;
    partitionerConfig =
        new PartitionerConfig.Builder().setPartitionerType(PartitionerFactory.PartitionerType.ROUND_ROBIN)
            .setNumPartitions(numPartitions).build();
    Partitioner partitioner = PartitionerFactory.getPartitioner(partitionerConfig);
    assertEquals(partitioner.getClass(), RoundRobinPartitioner.class);

    GenericRow row = new GenericRow();
    int expectedPartition = 0;
    for (int i = 0; i < 10; i++) {
      row.putValue("dim", RandomStringUtils.randomAlphabetic(3));
      int partition = Integer.parseInt(partitioner.getPartition(row));
      assertEquals(partition, expectedPartition);
      expectedPartition = (expectedPartition + 1) % numPartitions;
    }
  }

  @Test
  public void testTableColumnPartitionConfigPartitioner() {
    PartitionerConfig partitionerConfig =
        new PartitionerConfig.Builder().setPartitionerType(PartitionerFactory.PartitionerType.TABLE_PARTITION_CONFIG)
            .build();
    try {
      PartitionerFactory.getPartitioner(partitionerConfig);
      fail("Should not create TABLE_PARTITION_CONFIG Partitioner without column name");
    } catch (IllegalStateException e) {
      // expected
    }
    partitionerConfig =
        new PartitionerConfig.Builder().setPartitionerType(PartitionerFactory.PartitionerType.TABLE_PARTITION_CONFIG)
            .setColumnName("foo").build();
    try {
      PartitionerFactory.getPartitioner(partitionerConfig);
      fail("Should not create TABLE_PARTITION_CONFIG Partitioner without columnPartitionConfig");
    } catch (IllegalStateException e) {
      // expected
    }
    partitionerConfig =
        new PartitionerConfig.Builder().setPartitionerType(PartitionerFactory.PartitionerType.TABLE_PARTITION_CONFIG)
            .setColumnName("foo").setColumnPartitionConfig(new ColumnPartitionConfig("MURMUR", 3)).build();
    Partitioner partitioner = PartitionerFactory.getPartitioner(partitionerConfig);
    assertEquals(partitioner.getClass(), TableConfigPartitioner.class);

    GenericRow row = new GenericRow();
    for (int i = 0; i < 10; i++) {
      row.putValue("foo", RandomStringUtils.randomAlphabetic(3));
      int partition = Integer.parseInt(partitioner.getPartition(row));
      assertTrue(partition >= 0 && partition < 3);
    }
  }

  @Test
  public void testTransformFunctionPartitioner() {
    PartitionerConfig partitionerConfig =
        new PartitionerConfig.Builder().setPartitionerType(PartitionerFactory.PartitionerType.TRANSFORM_FUNCTION)
            .build();
    try {
      PartitionerFactory.getPartitioner(partitionerConfig);
      fail("Should not create TRANSFORM_FUNCTION Partitioner without transform function");
    } catch (IllegalStateException e) {
      // expected
    }
    partitionerConfig =
        new PartitionerConfig.Builder().setPartitionerType(PartitionerFactory.PartitionerType.TRANSFORM_FUNCTION)
            .setTransformFunction("badFunction()").build();
    try {
      PartitionerFactory.getPartitioner(partitionerConfig);
      fail("Should not create TRANSFORM_FUNCTION Partitioner for invalid transform function");
    } catch (IllegalStateException e) {
      // expected
    }

    partitionerConfig =
        new PartitionerConfig.Builder().setPartitionerType(PartitionerFactory.PartitionerType.TRANSFORM_FUNCTION)
            .setTransformFunction("toEpochDays(\"timestamp\")").build();
    Partitioner partitioner = PartitionerFactory.getPartitioner(partitionerConfig);
    assertEquals(partitioner.getClass(), TransformFunctionPartitioner.class);
    GenericRow row = new GenericRow();
    row.putValue("timestamp", 1587410614000L);
    assertEquals(partitioner.getPartition(row), "18372");

    partitionerConfig =
        new PartitionerConfig.Builder().setPartitionerType(PartitionerFactory.PartitionerType.TRANSFORM_FUNCTION)
            .setTransformFunction("Groovy({a+b},a,b)").build();
    partitioner = PartitionerFactory.getPartitioner(partitionerConfig);
    row.putValue("a", 10);
    row.putValue("b", 20);
    assertEquals(partitioner.getPartition(row), "30");

    // mv column
    partitionerConfig =
        new PartitionerConfig.Builder().setPartitionerType(PartitionerFactory.PartitionerType.TRANSFORM_FUNCTION)
            .setTransformFunction("Groovy({dMv[1]},dMv)").build();
    partitioner = PartitionerFactory.getPartitioner(partitionerConfig);
    row.putValue("dMv", new Object[]{1, 2, 3});
    assertEquals(partitioner.getPartition(row), "2");
  }
}
