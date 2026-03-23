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
package org.apache.pinot.core.data.manager.ingest;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/**
 * Tests for {@link InsertPartitionRouter} routing for append, upsert, and dedup tables.
 */
public class InsertPartitionRouterTest {

  @Test
  public void testRoundRobinForAppendTable() {
    InsertPartitionRouter router = new InsertPartitionRouter(4, null, null, false);

    GenericRow row = new GenericRow();
    row.putValue("id", 1);

    // Should cycle through partitions
    int p0 = router.getPartition(row);
    int p1 = router.getPartition(row);
    int p2 = router.getPartition(row);
    int p3 = router.getPartition(row);
    int p4 = router.getPartition(row);

    assertEquals(p0, 0);
    assertEquals(p1, 1);
    assertEquals(p2, 2);
    assertEquals(p3, 3);
    assertEquals(p4, 0); // wraps around
  }

  @Test
  public void testPartitionFunctionRouting() {
    // Simple modulo partition function
    PartitionFunction modulo = new PartitionFunction() {
      @Override
      public int getPartition(String value) {
        return Math.abs(value.hashCode() % 4);
      }

      @Override
      public String getName() {
        return "Modulo";
      }

      @Override
      public int getNumPartitions() {
        return 4;
      }
    };

    InsertPartitionRouter router = new InsertPartitionRouter(4, "userId", modulo, true);

    GenericRow row1 = new GenericRow();
    row1.putValue("userId", "user-123");

    GenericRow row2 = new GenericRow();
    row2.putValue("userId", "user-123");

    // Same key should always route to same partition
    assertEquals(router.getPartition(row1), router.getPartition(row2));
  }

  @Test
  public void testRouteRowsGroupsByPartition() {
    PartitionFunction modulo = new PartitionFunction() {
      @Override
      public int getPartition(String value) {
        return Integer.parseInt(value) % 3;
      }

      @Override
      public String getName() {
        return "Modulo";
      }

      @Override
      public int getNumPartitions() {
        return 3;
      }
    };

    InsertPartitionRouter router = new InsertPartitionRouter(3, "id", modulo, false);

    GenericRow row0 = new GenericRow();
    row0.putValue("id", "0");
    GenericRow row1 = new GenericRow();
    row1.putValue("id", "1");
    GenericRow row2 = new GenericRow();
    row2.putValue("id", "2");
    GenericRow row3 = new GenericRow();
    row3.putValue("id", "3");

    Map<Integer, List<GenericRow>> grouped = router.routeRows(Arrays.asList(row0, row1, row2, row3));

    assertEquals(grouped.get(0).size(), 2); // id=0 and id=3
    assertEquals(grouped.get(1).size(), 1); // id=1
    assertEquals(grouped.get(2).size(), 1); // id=2
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testUpsertTableWithoutPartitionConfigThrows() {
    UpsertConfig upsertConfig = new UpsertConfig(UpsertConfig.Mode.FULL);
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME)
        .setTableName("upsertTable")
        .setUpsertConfig(upsertConfig)
        .build();

    // Should throw because no partition config is set
    new InsertPartitionRouter(tableConfig);
  }

  @Test
  public void testAppendTableWithoutPartitionConfig() {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("appendTable")
        .build();

    InsertPartitionRouter router = new InsertPartitionRouter(tableConfig);

    assertEquals(router.getNumPartitions(), 1);
    assertFalse(router.isUpsertOrDedup());
    assertNull(router.getPartitionColumn());
  }

  @Test
  public void testTableWithPartitionConfig() {
    Map<String, ColumnPartitionConfig> partitionMap = new HashMap<>();
    partitionMap.put("userId", new ColumnPartitionConfig("HashCode", 4));
    SegmentPartitionConfig segPartConfig = new SegmentPartitionConfig(partitionMap);

    IndexingConfig indexingConfig = new IndexingConfig();
    indexingConfig.setSegmentPartitionConfig(segPartConfig);

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("partitionedTable")
        .build();
    tableConfig.setIndexingConfig(indexingConfig);

    InsertPartitionRouter router = new InsertPartitionRouter(tableConfig);

    assertEquals(router.getNumPartitions(), 4);
    assertEquals(router.getPartitionColumn(), "userId");

    GenericRow row = new GenericRow();
    row.putValue("userId", "test-user");

    int partition = router.getPartition(row);
    assertTrue(partition >= 0 && partition < 4);
  }

  @Test
  public void testSinglePartitionRouting() {
    InsertPartitionRouter router = new InsertPartitionRouter(1, null, null, false);

    GenericRow row = new GenericRow();
    row.putValue("id", 1);

    assertEquals(router.getPartition(row), 0);
    assertEquals(router.getPartition(row), 0);
  }

  @Test
  public void testRouteEmptyList() {
    InsertPartitionRouter router = new InsertPartitionRouter(4, null, null, false);
    Map<Integer, List<GenericRow>> grouped = router.routeRows(Collections.emptyList());
    assertTrue(grouped.isEmpty());
  }
}
