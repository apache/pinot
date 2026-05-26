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
package org.apache.pinot.materializedview.metadata;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.materializedview.metadata.MaterializedViewDefinitionMetadata.MaterializedViewSplitSpec;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class MaterializedViewMetadataTest {

  @Test
  public void testDefinitionRoundTrip() {
    String viewTableName = "mv_daily_order_amount_OFFLINE";
    Map<String, String> partitionExprMaps = new HashMap<>();
    partitionExprMaps.put("DaysSinceEpoch", "DaysSinceEpoch");

    MaterializedViewSplitSpec splitSpec = new MaterializedViewSplitSpec(
        "ts", "1:MILLISECONDS:EPOCH", "DaysSinceEpoch", "1:DAYS:EPOCH", 86400000L);

    MaterializedViewDefinitionMetadata original = new MaterializedViewDefinitionMetadata(
        viewTableName,
        Arrays.asList("orders_OFFLINE", "products_OFFLINE"),
        "SELECT DaysSinceEpoch, city, count(*) as cnt FROM orders GROUP BY DaysSinceEpoch, city",
        partitionExprMaps,
        splitSpec);

    ZNRecord znRecord = original.toZNRecord();
    assertEquals(znRecord.getId(), viewTableName);

    MaterializedViewDefinitionMetadata restored = MaterializedViewDefinitionMetadata.fromZNRecord(znRecord);
    assertEquals(restored.getMaterializedViewTableNameWithType(), viewTableName);
    assertEquals(restored.getBaseTables(), Arrays.asList("orders_OFFLINE", "products_OFFLINE"));
    assertNotNull(restored.getDefinedSql());
    assertEquals(restored.getPartitionExprMaps().size(), 1);
    assertEquals(restored.getPartitionExprMaps().get("DaysSinceEpoch"), "DaysSinceEpoch");

    MaterializedViewSplitSpec restoredSpec = restored.getSplitSpec();
    assertNotNull(restoredSpec);
    assertEquals(restoredSpec.getSourceTimeColumn(), "ts");
    assertEquals(restoredSpec.getSourceTimeFormat(), "1:MILLISECONDS:EPOCH");
    assertEquals(restoredSpec.getMaterializedViewTimeColumn(), "DaysSinceEpoch");
    assertEquals(restoredSpec.getMaterializedViewTimeFormat(), "1:DAYS:EPOCH");
    assertEquals(restoredSpec.getBucketMs(), 86400000L);
  }

  @Test
  public void testDefinitionWithNoSplitSpec() {
    MaterializedViewDefinitionMetadata metadata = new MaterializedViewDefinitionMetadata(
        "mv_OFFLINE",
        Collections.singletonList("src_OFFLINE"),
        null,
        Collections.emptyMap(),
        null);

    ZNRecord znRecord = metadata.toZNRecord();
    MaterializedViewDefinitionMetadata restored = MaterializedViewDefinitionMetadata.fromZNRecord(znRecord);

    assertEquals(restored.getBaseTables(), Collections.singletonList("src_OFFLINE"));
    assertNull(restored.getDefinedSql());
    assertTrue(restored.getPartitionExprMaps().isEmpty());
    assertNull(restored.getSplitSpec());
  }

  @Test
  public void testRuntimeRoundTrip() {
    Map<Long, PartitionInfo> partitions = new HashMap<>();
    partitions.put(86400000L, new PartitionInfo(
        PartitionState.VALID, new PartitionFingerprint(10, 5000L), 1700010000000L));
    partitions.put(172800000L, new PartitionInfo(
        PartitionState.STALE, new PartitionFingerprint(8, 3200L), 1700090000000L));

    MaterializedViewRuntimeMetadata original = new MaterializedViewRuntimeMetadata(
        "mv_test_OFFLINE", 259200000L, partitions);

    ZNRecord znRecord = original.toZNRecord();
    MaterializedViewRuntimeMetadata restored = MaterializedViewRuntimeMetadata.fromZNRecord(znRecord);

    assertEquals(restored.getMaterializedViewTableNameWithType(), "mv_test_OFFLINE");
    assertEquals(restored.getWatermarkMs(), 259200000L);
    assertEquals(restored.getPartitions().size(), 2);

    PartitionInfo info1 = restored.getPartitions().get(86400000L);
    assertEquals(info1.getState(), PartitionState.VALID);
    assertEquals(info1.getFingerprint(), new PartitionFingerprint(10, 5000L));
    assertEquals(info1.getLastRefreshTime(), 1700010000000L);

    assertEquals(restored.getPartitions().get(172800000L).getState(), PartitionState.STALE);
  }

  @Test
  public void testRuntimeEmptyPartitions() {
    MaterializedViewRuntimeMetadata metadata = new MaterializedViewRuntimeMetadata(
        "mv_OFFLINE", 0L, new HashMap<>());

    ZNRecord znRecord = metadata.toZNRecord();
    MaterializedViewRuntimeMetadata restored = MaterializedViewRuntimeMetadata.fromZNRecord(znRecord);

    assertEquals(restored.getWatermarkMs(), 0L);
    assertTrue(restored.getPartitions().isEmpty());
  }

  @Test
  public void testColdStartWatermarkOnlyEmptyPartitions() {
    MaterializedViewRuntimeMetadata metadata = new MaterializedViewRuntimeMetadata(
        "mv_OFFLINE", 86400000L, new HashMap<>());

    ZNRecord znRecord = metadata.toZNRecord();
    MaterializedViewRuntimeMetadata restored = MaterializedViewRuntimeMetadata.fromZNRecord(znRecord);

    assertEquals(restored.getWatermarkMs(), 86400000L);
    assertTrue(restored.getPartitions().isEmpty());
  }

  @Test
  public void testValidateForPersistAcceptsAnyValidState() {
    MaterializedViewRuntimeMetadata legitimate = new MaterializedViewRuntimeMetadata(
        "mv_OFFLINE", 200L, new HashMap<>());
    legitimate.validateForPersist(); // must not throw — no cross-field invariants under Design C
  }

  @Test
  public void testWatermarkAlwaysWritten() {
    // Even at cold-start (watermark=0), the watermark key must be written so the reader
    // round-trips zero correctly without depending on field-presence heuristics.
    MaterializedViewRuntimeMetadata metadata = new MaterializedViewRuntimeMetadata(
        "mv_OFFLINE", 0L, new HashMap<>());
    ZNRecord znRecord = metadata.toZNRecord();
    assertTrue(znRecord.getSimpleFields().containsKey("watermarkMs"),
        "watermarkMs key must always be written");
  }
}
