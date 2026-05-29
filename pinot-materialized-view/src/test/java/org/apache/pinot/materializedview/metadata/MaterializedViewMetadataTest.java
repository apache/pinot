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
import static org.testng.Assert.assertNotEquals;
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

  /// Definition equality is the controller's idempotency check at CREATE MV time: a second
  /// CREATE with the same definedSQL must hash/compare equal to the first so a stale orphan
  /// znode from a prior failed CREATE doesn't get classified as "different content".
  ///
  /// Verifies that every direct field of [MaterializedViewDefinitionMetadata] independently
  /// flips equality (so an equals() that silently drops a comparison is caught here). The
  /// nested SplitSpec's own field-by-field coverage is pinned separately in
  /// [#testSplitSpecEqualsAndHashCode]; that test also documents the one persisted-but-not-
  /// compared field (`materializedViewTimeFormat`), which is intentional and tracked as a
  /// separate follow-up. A regression that adds a new direct field on
  /// [MaterializedViewDefinitionMetadata] without wiring it through equals/hashCode must add
  /// a matching `assertNotEquals` line below — otherwise the controller's idempotency check
  /// would silently treat two distinct definitions as the same.
  @Test
  public void testDefinitionEqualsAndHashCodeAllFields() {
    Map<String, String> partitionExprMaps = new HashMap<>();
    partitionExprMaps.put("ts", "ts");
    MaterializedViewSplitSpec splitSpec =
        new MaterializedViewSplitSpec("ts", "1:MILLISECONDS:TIMESTAMP", "ts",
            "1:MILLISECONDS:TIMESTAMP", 86400000L);
    MaterializedViewDefinitionMetadata a = new MaterializedViewDefinitionMetadata(
        "mv_OFFLINE", Collections.singletonList("orders"),
        "SELECT ts FROM orders", partitionExprMaps, splitSpec, 0L, true);
    MaterializedViewDefinitionMetadata b = new MaterializedViewDefinitionMetadata(
        "mv_OFFLINE", Collections.singletonList("orders"),
        "SELECT ts FROM orders", new HashMap<>(partitionExprMaps),
        new MaterializedViewSplitSpec("ts", "1:MILLISECONDS:TIMESTAMP", "ts",
            "1:MILLISECONDS:TIMESTAMP", 86400000L),
        0L, true);
    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());

    // Each field independently flips equality — guards against an equals() that silently
    // omits a field. Use a fresh `a` baseline for each delta so order in the chain doesn't
    // matter.
    MaterializedViewDefinitionMetadata diffName = new MaterializedViewDefinitionMetadata(
        "mv_other_OFFLINE", Collections.singletonList("orders"),
        "SELECT ts FROM orders", partitionExprMaps, splitSpec, 0L, true);
    assertNotEquals(a, diffName);

    MaterializedViewDefinitionMetadata diffBase = new MaterializedViewDefinitionMetadata(
        "mv_OFFLINE", Collections.singletonList("products"),
        "SELECT ts FROM orders", partitionExprMaps, splitSpec, 0L, true);
    assertNotEquals(a, diffBase);

    MaterializedViewDefinitionMetadata diffSql = new MaterializedViewDefinitionMetadata(
        "mv_OFFLINE", Collections.singletonList("orders"),
        "SELECT ts FROM other", partitionExprMaps, splitSpec, 0L, true);
    assertNotEquals(a, diffSql);

    Map<String, String> diffExprMap = new HashMap<>();
    diffExprMap.put("ts", "renamed_ts");
    MaterializedViewDefinitionMetadata diffExpr = new MaterializedViewDefinitionMetadata(
        "mv_OFFLINE", Collections.singletonList("orders"),
        "SELECT ts FROM orders", diffExprMap, splitSpec, 0L, true);
    assertNotEquals(a, diffExpr);

    MaterializedViewSplitSpec diffSpec =
        new MaterializedViewSplitSpec("ts", "1:HOURS:EPOCH", "ts", "1:HOURS:EPOCH", 86400000L);
    MaterializedViewDefinitionMetadata diffSplit = new MaterializedViewDefinitionMetadata(
        "mv_OFFLINE", Collections.singletonList("orders"),
        "SELECT ts FROM orders", partitionExprMaps, diffSpec, 0L, true);
    assertNotEquals(a, diffSplit);

    MaterializedViewDefinitionMetadata diffStaleness = new MaterializedViewDefinitionMetadata(
        "mv_OFFLINE", Collections.singletonList("orders"),
        "SELECT ts FROM orders", partitionExprMaps, splitSpec, 60000L, true);
    assertNotEquals(a, diffStaleness);

    MaterializedViewDefinitionMetadata diffRewrite = new MaterializedViewDefinitionMetadata(
        "mv_OFFLINE", Collections.singletonList("orders"),
        "SELECT ts FROM orders", partitionExprMaps, splitSpec, 0L, false);
    assertNotEquals(a, diffRewrite);
  }

  /// SplitSpec equality covers `sourceTimeColumn`, `sourceTimeFormat`,
  /// `materializedViewTimeColumn`, and `bucketMs` — the same four fields whose Δ flips
  /// definition equality (so a regression that drops a field comparison is caught here too).
  /// `materializedViewTimeFormat` is intentionally NOT part of this contract today (it was
  /// added later, and the existing equals/hashCode predates it); changing that is tracked as a
  /// separate follow-up since it would alter the `createIfAbsent` idempotency surface.
  @Test
  public void testSplitSpecEqualsAndHashCode() {
    MaterializedViewSplitSpec a = new MaterializedViewSplitSpec("ts", "fmt", "v", "vfmt", 1000L);
    MaterializedViewSplitSpec b = new MaterializedViewSplitSpec("ts", "fmt", "v", "vfmt", 1000L);
    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());

    assertNotEquals(a, new MaterializedViewSplitSpec("other", "fmt", "v", "vfmt", 1000L));
    assertNotEquals(a, new MaterializedViewSplitSpec("ts", "other", "v", "vfmt", 1000L));
    assertNotEquals(a, new MaterializedViewSplitSpec("ts", "fmt", "other", "vfmt", 1000L));
    assertNotEquals(a, new MaterializedViewSplitSpec("ts", "fmt", "v", "vfmt", 2000L));
  }
}
