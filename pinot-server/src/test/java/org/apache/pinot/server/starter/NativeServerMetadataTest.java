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
package org.apache.pinot.server.starter;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Identity;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.ObjectVersion;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Operation;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Owner;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Participant;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Role;
import org.apache.pinot.common.config.provider.RecoverableClusterConfigCache;
import org.apache.pinot.common.config.provider.TableDataManagerMetadata;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc.CatalogRecord;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc.RecordKind;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/// Exercises complete-image recovery and per-incarnation metadata fencing for the native installation runtime.
public class NativeServerMetadataTest {
  @Test
  public void testLocalPublicationFenceTracksConfigAssignmentAndImageInvalidation()
      throws Exception {
    Fixture fixture = new Fixture();
    NativeServerMetadata metadata = new NativeServerMetadata(fixture._server, new RecoverableClusterConfigCache());
    UUID operationId = UUID.randomUUID();
    Identity captured = fixture._orders.identity(fixture._server, operationId, fixture._orders._assignment);
    assertFalse(metadata.isCurrent(captured));
    MetadataRpc.InstallationSnapshot initial = fixture.snapshot(10, Map.of(), fixture.records()).toBuilder()
        .addOperationIds(operationId.toString()).build();
    metadata.apply(initial);
    assertTrue(metadata.isCurrent(captured));
    metadata.apply(initial);
    assertTrue(metadata.isCurrent(captured), "An identical snapshot must preserve current authority");
    MetadataRpc.InstallationSnapshot unrelated = fixture.snapshot(11, Map.of("unrelated", "value"), fixture.records())
        .toBuilder().addOperationIds(operationId.toString()).build();
    metadata.apply(unrelated);
    assertTrue(metadata.isCurrent(captured), "An unrelated cluster config update must not fence captured work");
    metadata.invalidate();
    assertFalse(metadata.isCurrent(captured));
    metadata.apply(unrelated);
    assertTrue(metadata.isCurrent(captured));
    assertFalse(metadata.isCurrent(fixture._orders.identity(fixture._server, UUID.randomUUID(),
        fixture._orders._assignment)), "An operation absent from the complete image cannot publish");
    assertFalse(metadata.isCurrent(fixture._orders.identity(fixture._server.toBuilder()
        .setRegistrationEpoch(2).setIncarnation(UUID.randomUUID().toString()).build(), operationId,
        fixture._orders._assignment)), "A different SERVER session cannot publish");
    assertFalse(metadata.isCurrent(fixture._orders.identity(fixture._server.toBuilder()
        .setHistoryId(UUID.randomUUID().toString()).build(), operationId, fixture._orders._assignment)));

    MetadataRpc.OfflineReplicaAssignment target = fixture._orders._assignment.getValue().getOfflineReplicaAssignment();
    CatalogRecord reassigned = fixture._orders._assignment.toBuilder().setVersion(2).setLastModifiedIndex(12)
        .setValue(MetadataRpc.CatalogValue.newBuilder().setOfflineReplicaAssignment(target.toBuilder()
            .setAssignmentEpoch(2).setMaterializationId(UUID.randomUUID().toString()))).build();
    List<CatalogRecord> records = new ArrayList<>(fixture.records());
    records.replaceAll(record -> record.getObjectId().equals(reassigned.getObjectId()) ? reassigned : record);
    metadata.apply(fixture.snapshot(12, Map.of(), records).toBuilder().addOperationIds(operationId.toString()).build());
    assertFalse(metadata.isCurrent(captured), "A newer materialization must fence the old assignment");
    Identity next = fixture._orders.identity(fixture._server, operationId, reassigned);
    assertTrue(metadata.isCurrent(next));

    CatalogRecord offline = reassigned.toBuilder().setVersion(3).setLastModifiedIndex(13)
        .setValue(MetadataRpc.CatalogValue.newBuilder().setOfflineReplicaAssignment(reassigned.getValue()
            .getOfflineReplicaAssignment().toBuilder().setAssignmentEpoch(3)
            .setMaterializationId(UUID.randomUUID().toString()).setDesiredOnline(false))).build();
    records.replaceAll(record -> record.getObjectId().equals(offline.getObjectId()) ? offline : record);
    metadata.apply(fixture.snapshot(13, Map.of(), records).toBuilder().addOperationIds(operationId.toString()).build());
    assertFalse(metadata.isCurrent(fixture._orders.identity(fixture._server, operationId, offline)),
        "An exact identity with desiredOnline=false cannot publish");

    for (CatalogRecord changed : List.of(fixture._orders._table, fixture._orders._schema)) {
      NativeServerMetadata changedMetadata = new NativeServerMetadata(fixture._server,
          new RecoverableClusterConfigCache());
      List<CatalogRecord> changedRecords = new ArrayList<>(fixture.records());
      changedRecords.replaceAll(record -> record.getObjectId().equals(changed.getObjectId())
          ? record.toBuilder().setVersion(2).setLastModifiedIndex(12).build() : record);
      changedMetadata.apply(fixture.snapshot(12, Map.of(), changedRecords).toBuilder()
          .addOperationIds(operationId.toString()).build());
      assertFalse(changedMetadata.isCurrent(captured), "Changed loading config must fence previously prepared bytes");
    }
    NativeServerMetadata completed = new NativeServerMetadata(fixture._server, new RecoverableClusterConfigCache());
    completed.apply(initial);
    assertTrue(completed.isCurrent(captured));
    completed.apply(fixture.snapshot(14, Map.of(), fixture.records()));
    assertFalse(completed.isCurrent(captured), "Completed operation discovery must revoke local admission");
  }

  @Test
  public void testFailedInitialConfigDeliveryRecoversRemovedKeysFromDetachedSnapshot() {
    RecoverableClusterConfigCache cache = new RecoverableClusterConfigCache();
    Map<String, String> input = new HashMap<>();
    input.put("partial", "original");
    input.put("ignored", null);
    cache.updateClusterConfigs(input);
    Map<String, String> snapshot = cache.getClusterConfigs();
    input.put("partial", "mutated");
    assertEquals(snapshot, Map.of("partial", "original"));
    assertThrows(UnsupportedOperationException.class, () -> snapshot.put("partial", "mutated"));

    Map<String, String> applied = new HashMap<>();
    List<Set<String>> delivered = new ArrayList<>();
    AtomicBoolean fail = new AtomicBoolean(true);
    assertThrows(IllegalStateException.class, () -> cache.registerClusterConfigChangeListener((changed, values) -> {
      delivered.add(changed);
      applyKeys(applied, changed, values);
      if (fail.getAndSet(false)) {
        throw new IllegalStateException("Initial callback failed after applying a key");
      }
    }));
    assertEquals(applied, snapshot);

    cache.updateClusterConfigs(Map.of());
    assertTrue(applied.isEmpty(), "Retry must undo partial initial delivery when its key has since been removed");
    assertEquals(delivered, List.of(Set.of("partial"), Set.of("partial")));
    assertTrue(cache.getClusterConfigs().isEmpty());
    assertEquals(snapshot, Map.of("partial", "original"));
  }

  @Test
  public void testFailedClusterCallbackRetriesChangedAndUnsetKeysBeforePublishingRevision()
      throws Exception {
    Fixture fixture = new Fixture();
    RecoverableClusterConfigCache cache = new RecoverableClusterConfigCache();
    NativeServerMetadata metadata = new NativeServerMetadata(fixture._server, cache);
    Map<String, String> firstApplied = new HashMap<>();
    Map<String, String> secondApplied = new HashMap<>();
    List<Set<String>> delivered = new ArrayList<>();
    AtomicBoolean fail = new AtomicBoolean();
    cache.registerClusterConfigChangeListener((changed, values) -> {
      delivered.add(changed);
      if (fail.getAndSet(false)) {
        // Model a listener that applies one key and then fails before applying additions and removals.
        firstApplied.put("changed", values.get("changed"));
        throw new IllegalStateException("Partial callback failure");
      }
      applyKeys(firstApplied, changed, values);
    });
    cache.registerClusterConfigChangeListener((changed, values) -> applyKeys(secondApplied, changed, values));
    Map<String, String> original = Map.of("changed", "old", "removed", "old");
    metadata.apply(fixture.snapshot(10, original, fixture.records()));
    assertEquals(metadata.revision().getIndex(), 10);
    Map<String, String> replacement = Map.of("changed", "new", "added", "new");
    MetadataRpc.InstallationSnapshot next = fixture.snapshot(11, replacement, fixture.records());
    fail.set(true);
    assertThrows(IllegalStateException.class, () -> metadata.apply(next));
    assertThrows(IllegalStateException.class, metadata::revision);
    assertEquals(secondApplied, original, "Later listener must not have received the failed update");
    assertEquals(firstApplied.get("removed"), "old");

    metadata.apply(next);
    assertEquals(metadata.revision().getIndex(), 11);
    assertEquals(firstApplied, replacement);
    assertEquals(secondApplied, replacement);
    assertEquals(delivered.get(delivered.size() - 1), Set.of("changed", "added", "removed"));
  }

  @Test
  public void testInvalidationAndFailedDeliveryRetainSnapshotHighWatermark()
      throws Exception {
    Fixture fixture = new Fixture();
    RecoverableClusterConfigCache cache = new RecoverableClusterConfigCache();
    NativeServerMetadata metadata = new NativeServerMetadata(fixture._server, cache);
    MetadataRpc.InstallationSnapshot initial = fixture.snapshot(10, Map.of("key", "old"), fixture.records());
    metadata.apply(initial);
    metadata.invalidate();
    assertThrows(IllegalStateException.class, metadata::revision);
    assertThrows(IOException.class, () -> metadata.apply(fixture.snapshot(9, Map.of(), fixture.records())));
    assertThrows(IOException.class, () -> metadata.apply(initial.toBuilder()
        .addOperationIds(UUID.randomUUID().toString()).build()));
    assertThrows(IllegalStateException.class, metadata::revision);
    metadata.apply(initial);
    assertEquals(metadata.revision().getIndex(), 10);

    AtomicBoolean fail = new AtomicBoolean();
    cache.registerClusterConfigChangeListener((changed, values) -> {
      if (fail.getAndSet(false)) {
        throw new IllegalStateException("Listener failed");
      }
    });
    MetadataRpc.InstallationSnapshot failed = fixture.snapshot(12, Map.of("key", "new"), fixture.records());
    fail.set(true);
    assertThrows(IllegalStateException.class, () -> metadata.apply(failed));
    metadata.invalidate();
    assertThrows(IOException.class, () -> metadata.apply(fixture.snapshot(11, Map.of(), fixture.records())));
    assertThrows(IOException.class, () -> metadata.apply(failed.toBuilder()
        .addOperationIds(UUID.randomUUID().toString()).build()));
    assertThrows(IllegalStateException.class, metadata::revision);
    metadata.apply(failed);
    assertEquals(metadata.revision().getIndex(), 12);
  }

  @Test
  public void testMalformedTargetsRejectAtomicallyAndRecreatedTableDoesNotBlockOtherProviders()
      throws Exception {
    Fixture fixture = new Fixture();
    RecoverableClusterConfigCache cache = new RecoverableClusterConfigCache();
    AtomicInteger callbacks = new AtomicInteger();
    cache.registerClusterConfigChangeListener((changed, values) -> callbacks.incrementAndGet());
    NativeServerMetadata metadata = new NativeServerMetadata(fixture._server, cache);
    metadata.apply(fixture.snapshot(10, Map.of(), fixture.records()));
    int initialCallbacks = callbacks.get();
    TableDataManagerMetadata oldOrders = metadata.provider("db.orders_OFFLINE");
    TableDataManagerMetadata users = metadata.provider("db.users_OFFLINE");
    CatalogRecord assignment = fixture._orders._assignment;
    MetadataRpc.OfflineReplicaAssignment target = assignment.getValue().getOfflineReplicaAssignment();
    CatalogRecord content = fixture._orders._content;
    List<CatalogRecord> malformed = List.of(
        assignment.toBuilder().setValue(MetadataRpc.CatalogValue.newBuilder()
            .setOfflineReplicaAssignment(target.toBuilder().clearCompatibility())).build(),
        assignment.toBuilder().setValue(MetadataRpc.CatalogValue.newBuilder()
            .setOfflineReplicaAssignment(target.toBuilder().setAssignmentEpoch(0))).build(),
        assignment.toBuilder().setValue(MetadataRpc.CatalogValue.newBuilder()
            .setOfflineReplicaAssignment(target.toBuilder().setServer(fixture._server.toBuilder()
                .setIncarnation(UUID.randomUUID().toString())))).build(),
        assignment.toBuilder().setValue(MetadataRpc.CatalogValue.newBuilder()
            .setOfflineReplicaAssignment(target.toBuilder().setCompatibility(target.getCompatibility().toBuilder()
                .setSchema(target.getCompatibility().getSchema().toBuilder().setVersion(2))))).build(),
        content.toBuilder().setValue(MetadataRpc.CatalogValue.newBuilder().setOfflineSegmentContent(
            content.getValue().getOfflineSegmentContent().toBuilder().setDownloadUri("relative/path"))).build(),
        content.toBuilder().setValue(MetadataRpc.CatalogValue.newBuilder().setOfflineSegmentContent(
            content.getValue().getOfflineSegmentContent().toBuilder().setTableName("db..orders_OFFLINE"))).build());
    for (CatalogRecord invalid : malformed) {
      List<CatalogRecord> records = new ArrayList<>(fixture.records());
      records.replaceAll(record -> record.getObjectId().equals(invalid.getObjectId()) ? invalid : record);
      assertThrows(IOException.class, () -> metadata.apply(fixture.snapshot(11, Map.of("changed", "bad"), records)));
      assertEquals(metadata.revision().getIndex(), 10);
      assertEquals(callbacks.get(), initialCallbacks, "Invalid work must not reach config listeners");
      assertEquals(users.fetchSegment("db.users_OFFLINE", "users_0").segment().segmentName(), "users_0");
    }

    Family recreated = new Family(fixture, "db.orders", "orders_1", "new_value");
    List<CatalogRecord> replaced = new ArrayList<>(fixture._users.records());
    replaced.add(fixture._orders._table.toBuilder().setDeleted(true).clearValue().setVersion(2)
        .setLastModifiedIndex(11).build());
    replaced.add(fixture._orders._schema.toBuilder().setDeleted(true).clearValue().setVersion(2)
        .setLastModifiedIndex(11).build());
    replaced.add(fixture._orders._content);
    replaced.add(fixture._orders._assignment);
    replaced.addAll(recreated.records());
    metadata.apply(fixture.snapshot(12, Map.of(), replaced));
    assertEquals(metadata.revision().getIndex(), 12);
    assertThrows(IllegalStateException.class, () -> oldOrders.fetchTable("db.orders_OFFLINE"));
    TableDataManagerMetadata currentOrders = metadata.provider("db.orders_OFFLINE");
    assertTrue(currentOrders.fetchTable("db.orders_OFFLINE").schema().hasColumn("new_value"));
    assertNull(currentOrders.fetchSegment("db.orders_OFFLINE", "orders_0"),
        "A retained assignment to the deleted incarnation must not block the new provider");
    assertEquals(currentOrders.fetchSegment("db.orders_OFFLINE", "orders_1").segment().segmentName(), "orders_1");
    assertEquals(users.fetchSegment("db.users_OFFLINE", "users_0").segment().segmentName(), "users_0");

    // Both old and new incarnations can retain the same logical segment name; only the current incarnation matches.
    UUID currentContentId = UUID.randomUUID();
    MetadataRpc.OfflineSegmentContent sameName = recreated._content.getValue().getOfflineSegmentContent().toBuilder()
        .setSegmentName("orders_0").setSegmentObjectId(currentContentId.toString()).build();
    MetadataRpc.OfflineReplicaAssignment sameNameAssignment = recreated._assignment.getValue()
        .getOfflineReplicaAssignment().toBuilder().setSegmentName("orders_0")
        .setSegmentObjectId(currentContentId.toString()).setMaterializationId(UUID.randomUUID().toString()).build();
    replaced.add(record(RecordKind.OFFLINE_SEGMENT_CONTENT, currentContentId, currentContentId.toString())
        .setLastModifiedIndex(13).setValue(MetadataRpc.CatalogValue.newBuilder()
            .setOfflineSegmentContent(sameName)).build());
    UUID currentAssignmentId = UUID.randomUUID();
    replaced.add(record(RecordKind.OFFLINE_REPLICA_ASSIGNMENT, currentAssignmentId, currentAssignmentId.toString())
        .setLastModifiedIndex(13).setValue(MetadataRpc.CatalogValue.newBuilder()
            .setOfflineReplicaAssignment(sameNameAssignment)).build());
    metadata.apply(fixture.snapshot(13, Map.of(), replaced));
    assertTrue(currentOrders.fetchSegment("db.orders_OFFLINE", "orders_0").schema().hasColumn("new_value"));
    assertEquals(users.fetchSegment("db.users_OFFLINE", "users_0").segment().segmentName(), "users_0");
  }

  private static void applyKeys(Map<String, String> applied, Set<String> changed, Map<String, String> values) {
    for (String key : changed) {
      if (values.containsKey(key)) {
        applied.put(key, values.get(key));
      } else {
        applied.remove(key);
      }
    }
  }

  private static final class Fixture {
    private final UUID _history = UUID.randomUUID();
    private final UUID _clusterId = UUID.randomUUID();
    private final UUID _activation = UUID.randomUUID();
    private final MetadataRpc.ParticipantToken _server;
    private final Family _orders;
    private final Family _users;

    private Fixture()
        throws IOException {
      _server = MetadataRpc.ParticipantToken.newBuilder().setHistoryId(_history.toString()).setParticipantId("server")
          .setRole(MetadataRpc.ParticipantRole.SERVER).setIncarnation(UUID.randomUUID().toString())
          .setRegistrationEpoch(1).build();
      _orders = new Family(this, "db.orders", "orders_0", "value");
      _users = new Family(this, "db.users", "users_0", "user");
    }

    private List<CatalogRecord> records() {
      List<CatalogRecord> records = new ArrayList<>(_orders.records());
      records.addAll(_users.records());
      return records;
    }

    private MetadataRpc.InstallationSnapshot snapshot(long index, Map<String, String> config,
        List<CatalogRecord> records) {
      MetadataRpc.Revision revision = MetadataRpc.Revision.newBuilder().setHistoryId(_history.toString())
          .setIndex(index).build();
      MetadataRpc.MembershipControl control = MetadataRpc.MembershipControl.newBuilder()
          .setControlEpoch(MetadataRpc.ControlToken.newBuilder().setHistoryId(_history.toString()).setRaftTerm(1)
              .setBarrierIndex(1).setLeaderId("leader").setActivationId(_activation.toString()))
          .setRound(1).setClockBarrierIndex(1).setMinimumTickDelayMillis(10).setMaxConfiguredPermitLifetimeMillis(100)
          .setPolicy(MetadataRpc.MembershipPolicy.newBuilder()
              .setVersion(1).setMinTickIntervalMillis(10).setLeaseRounds(100).setPermitLifetimeMillis(100)
              .setMaxQueryLifetimeMillis(10).setMaxRevisionLag(1000)).build();
      MetadataRpc.ParticipantState participant = MetadataRpc.ParticipantState.newBuilder().setParticipant(_server)
          .setExpiryRound(100).setReady(true).setLastModifiedIndex(2)
          .setAppliedRevision(revision.toBuilder().setIndex(1)).build();
      return MetadataRpc.InstallationSnapshot.newBuilder().setStatus(MetadataRpc.ResultStatus.OK).setRevision(revision)
          .setParticipant(participant).setControl(control).addAllRecords(records)
          .addRecords(record(RecordKind.CLUSTER_CONFIG, _clusterId, "cluster").setVersion(index)
              .setLastModifiedIndex(index).setValue(MetadataRpc.CatalogValue.newBuilder()
                  .setClusterConfig(MetadataRpc.ClusterConfig.newBuilder().putAllProperties(config)))).build();
    }

    private MetadataRpc.ObjectVersion version(String objectId) {
      return MetadataRpc.ObjectVersion.newBuilder().setHistoryId(_history.toString()).setObjectId(objectId)
          .setVersion(1).build();
    }
  }

  private static final class Family {
    private final CatalogRecord _table;
    private final CatalogRecord _schema;
    private final CatalogRecord _content;
    private final CatalogRecord _assignment;

    private Family(Fixture fixture, String rawName, String segmentName, String column)
        throws IOException {
      UUID tableId = UUID.randomUUID();
      UUID schemaId = UUID.randomUUID();
      UUID contentId = UUID.randomUUID();
      TableConfig table = new TableConfigBuilder(TableType.OFFLINE).setTableName(rawName).build();
      Schema schema = new Schema.SchemaBuilder().setSchemaName(rawName)
          .addSingleValueDimension(column, DataType.INT).build();
      _table = record(RecordKind.TABLE_CONFIG, tableId, table.getTableName()).setValue(
          MetadataRpc.CatalogValue.newBuilder().setTableConfigJson(ByteString.copyFrom(JsonUtils.objectToBytes(table))))
          .build();
      _schema = record(RecordKind.SCHEMA, schemaId, rawName).setValue(MetadataRpc.CatalogValue.newBuilder()
          .setSchemaJson(ByteString.copyFrom(JsonUtils.objectToBytes(schema)))).build();
      MetadataRpc.OfflineSegmentContent content = MetadataRpc.OfflineSegmentContent.newBuilder()
          .setTableName(table.getTableName()).setTableObjectId(tableId.toString()).setSegmentName(segmentName)
          .setSegmentObjectId(contentId.toString()).setContentSha256("a".repeat(64))
          .setDownloadUri("file:/segments/" + segmentName + ".tar.gz")
          .setLoader(MetadataRpc.SegmentLoaderDescriptor.newBuilder().setCrc(1).setDataCrc(-1)
              .setCreationTimeMs(-1).setPushTimeMs(-1)).build();
      _content = record(RecordKind.OFFLINE_SEGMENT_CONTENT, contentId, contentId.toString())
          .setValue(MetadataRpc.CatalogValue.newBuilder().setOfflineSegmentContent(content)).build();
      UUID assignmentId = UUID.randomUUID();
      MetadataRpc.OfflineReplicaAssignment assignment = MetadataRpc.OfflineReplicaAssignment.newBuilder()
          .setTableName(content.getTableName()).setTableObjectId(content.getTableObjectId())
          .setSegmentName(segmentName).setSegmentObjectId(contentId.toString())
          .setContentSha256(content.getContentSha256())
          .setAssignmentEpoch(1).setServer(fixture._server).setMaterializationId(UUID.randomUUID().toString())
          .setCompatibility(MetadataRpc.MaterializationCompatibility.newBuilder()
              .setTable(fixture.version(tableId.toString())).setSchema(fixture.version(schemaId.toString())))
          .setDesiredOnline(true).build();
      _assignment = record(RecordKind.OFFLINE_REPLICA_ASSIGNMENT, assignmentId, assignmentId.toString())
          .setValue(MetadataRpc.CatalogValue.newBuilder().setOfflineReplicaAssignment(assignment)).build();
    }

    private List<CatalogRecord> records() {
      return List.of(_table, _schema, _content, _assignment);
    }

    private Identity identity(MetadataRpc.ParticipantToken server, UUID operationId, CatalogRecord assignmentRecord) {
      MetadataRpc.OfflineReplicaAssignment assignment = assignmentRecord.getValue().getOfflineReplicaAssignment();
      MetadataRpc.MaterializationCompatibility compatibility = assignment.getCompatibility();
      Participant participant = new Participant(server.getParticipantId(), Role.SERVER,
          UUID.fromString(server.getIncarnation()), server.getRegistrationEpoch());
      return new Identity(UUID.fromString(server.getHistoryId()), assignment.getTableName(),
          new ObjectVersion(UUID.fromString(compatibility.getTable().getObjectId()),
              compatibility.getTable().getVersion()), _schema.getName(),
          new ObjectVersion(UUID.fromString(compatibility.getSchema().getObjectId()),
              compatibility.getSchema().getVersion()), assignment.getSegmentName(),
          new ObjectVersion(UUID.fromString(assignment.getSegmentObjectId()), _content.getVersion()),
          assignment.getContentSha256(), participant,
          new ObjectVersion(UUID.fromString(assignmentRecord.getObjectId()), assignmentRecord.getVersion()),
          UUID.fromString(assignment.getMaterializationId()), new Operation(operationId, 1,
              new Owner(1, 0, 1, new Participant("controller", Role.CONTROLLER, UUID.randomUUID(), 1))));
    }
  }

  private static CatalogRecord.Builder record(RecordKind kind, UUID objectId, String name) {
    return CatalogRecord.newBuilder().setKind(kind).setName(name).setObjectId(objectId.toString())
        .setVersion(1).setSchemaVersion(1).setLastModifiedIndex(3);
  }
}
