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
package org.apache.pinot.coordination.ratis.metadata;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ActivateControlEpoch;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Acknowledge;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Action;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Command;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.ConfigTransaction;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.FeedScope;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.GetOutcome;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.GetRecord;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.GetRevision;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.GetStreamTip;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.MembershipCommand;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.MembershipQuery;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Mutation;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.ObjectVersion;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.ReadBarrier;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.RecordKind;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.RegisterClient;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Result;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.RetireClient;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Revision;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Status;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.StreamTipResult;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Transaction;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Exercises catalog invariants across conditional writes, lost responses, client retirement and snapshot replay.
public class MetadataCatalogTest {
  @Test
  public void testValidatedConfigFencesSchemaDeletionAndConcurrentTableCreationAcrossRestore()
      throws Exception {
    Schema schema = new Schema.SchemaBuilder().setSchemaName("orders")
        .addSingleValueDimension("id", DataType.STRING).build();
    TableConfig table = new TableConfigBuilder(TableType.OFFLINE).setTableName("orders").build();
    for (boolean deleteFirst : List.of(false, true)) {
      Fixture fixture = new Fixture();
      UUID schemaId = UUID.randomUUID();
      assertStatus(fixture.transact(Mutation.createSchema(fixture.history(), schemaId, schema)), "OK");
      Revision validated = fixture.catalog().currentRevision();
      ConfigTransaction delete = new ConfigTransaction(fixture.history(), "client", 1, "delete", 1, validated,
          List.of(Mutation.delete(RecordKind.SCHEMA, "orders", new ObjectVersion(fixture.history(), schemaId, 1))));
      Mutation create = Mutation.createTableConfig(fixture.history(), UUID.randomUUID(), table);
      ConfigTransaction createTable = new ConfigTransaction(fixture.history(), "client", 1, "create", 1,
          validated, List.of(create));
      // Exercise both serializations of the race; a legacy writer must invalidate validation too.
      JsonNode first = fixture.execute(deleteFirst ? delete : fixture.transaction("legacy", 1, create));
      assertStatus(first, "OK");
      ConfigTransaction losing = deleteFirst ? createTable : delete;
      JsonNode rejected = fixture.execute(losing);
      assertStatus(rejected, "CONFLICT");
      assertEquals(fixture.execute(losing), rejected, "Lost failure replies must replay the original result");
      assertStatus(fixture.read(RecordKind.SCHEMA, "orders"), deleteFirst ? "NOT_FOUND" : "OK");
      assertStatus(fixture.read(RecordKind.TABLE_CONFIG, "orders_OFFLINE"), deleteFirst ? "NOT_FOUND" : "OK");

      MetadataCatalog restored = new MetadataCatalog(fixture.cluster(), fixture.history());
      restored.restoreSnapshot(JsonUtils.bytesToJsonNode(JsonUtils.objectToBytes(fixture.catalog().captureSnapshot())));
      long index = fixture.index();
      assertEquals(JsonUtils.bytesToJsonNode(restored.execute(MetadataProtocol.encode(losing), ++index)), rejected);
      ConfigTransaction afterRestart = new ConfigTransaction(fixture.history(), "client", 1, "restart", 1,
          validated, losing.mutations());
      assertEquals(MetadataProtocol.decodeResult(restored.execute(MetadataProtocol.encode(afterRestart), ++index))
          .status(), Status.CONFLICT, "Retained tombstones must reconstruct the validation fence");
      GetStreamTip query = new GetStreamTip(fixture.history(), "client", 1, losing.streamId());
      StreamTipResult tip = MetadataProtocol.decodeStreamTipResult(restored.execute(
          MetadataProtocol.encode(new ReadBarrier(query)), ++index));
      assertEquals(tip.status(), Status.OK);
      assertEquals(tip.sequence(), 1L);
      assertEquals(tip.floor(), 0L);
      assertEquals(tip.digest(), MetadataProtocol.digest(losing));
      assertEquals(tip.outcome(), MetadataProtocol.fromTree(rejected, Result.class));
      assertEquals(tip.revision().index(), index);
      restored.execute(MetadataProtocol.encode(new Acknowledge(fixture.history(), "client", 1,
          losing.streamId(), 1)), ++index);
      StreamTipResult acknowledged = MetadataProtocol.decodeStreamTipResult(
          restored.query(MetadataProtocol.encode(query)));
      assertEquals(acknowledged.floor(), acknowledged.sequence());
      assertNull(acknowledged.digest());
      assertNull(acknowledged.outcome());
      assertEquals(MetadataProtocol.decodeResult(restored.execute(MetadataProtocol.encode(losing), ++index))
          .status(), Status.RESULT_EXPIRED);
    }
  }

  @Test
  public void testValidatedConfigIgnoresHeartbeatAndRequestProgressAndRetainsSuccess()
      throws Exception {
    Fixture fixture = new Fixture();
    UUID id = UUID.randomUUID();
    assertStatus(fixture.execute(fixture.transaction("main", 1,
        Mutation.createClusterConfig(fixture.history(), id, Map.of("value", "before")))), "OK");
    Revision validated = fixture.catalog().currentRevision();
    assertStatus(fixture.execute(new Acknowledge(fixture.history(), "client", 1, "main", 1)), "OK");
    assertStatus(fixture.execute(new ReadBarrier(new GetRevision(fixture.history()))), "OK");
    assertEquals(fixture.member(new ActivateControlEpoch(1, "controller", UUID.randomUUID())).status(), Status.OK);
    var control = fixture.catalog().membershipControl().controlEpoch();
    assertEquals(fixture.member(new MembershipProtocol.ConfigurePolicy(control, 0,
        new MembershipProtocol.Policy(1, 100, 3, 200, 300, 1000))).status(), Status.OK);
    var participant = fixture.member(new MembershipProtocol.Register("broker", MembershipProtocol.Role.BROKER,
        UUID.randomUUID(), 0, Map.of(), Set.of())).participant().participant();
    assertEquals(fixture.member(new MembershipProtocol.Tick(control, 0, 1)).status(), Status.OK);
    assertEquals(fixture.member(new MembershipProtocol.RenewBatch(List.of(new MembershipProtocol.Renewal(participant,
        1, fixture.catalog().currentRevision(), Set.of())))).renewals().getFirst().status(), Status.OK);
    Mutation update = Mutation.updateClusterConfig(new ObjectVersion(fixture.history(), id, 1),
        Map.of("value", "after"));
    ConfigTransaction command = new ConfigTransaction(fixture.history(), "client", 1, "main", 2, validated,
        List.of(update));
    assertEquals(MetadataProtocol.decodeCommand(MetadataProtocol.encode(command)), command);
    JsonNode result = fixture.execute(command);
    assertStatus(result, "OK");
    assertEquals(fixture.execute(command), result, "The command's own write must not invalidate its retry");
    ConfigTransaction changedRequest = new ConfigTransaction(fixture.history(), "client", 1, "main", 2,
        fixture.catalog().currentRevision(), List.of(update));
    assertStatus(fixture.execute(changedRequest), "CONFLICT");
    StreamTipResult tip = MetadataProtocol.decodeStreamTipResult(fixture.catalog().query(MetadataProtocol.encode(
        new GetStreamTip(fixture.history(), "client", 1, "main"))));
    assertEquals(tip.digest(), MetadataProtocol.digest(command));
    assertEquals(tip.outcome(), MetadataProtocol.fromTree(result, Result.class));
    StreamTipResult absent = MetadataProtocol.decodeStreamTipResult(fixture.catalog().query(MetadataProtocol.encode(
        new GetStreamTip(fixture.history(), "client", 1, "absent"))));
    assertEquals(absent.status(), Status.OK);
    assertEquals(absent.sequence(), 0L);
    assertEquals(absent.floor(), 0L);
    assertNull(absent.outcome());
    assertEquals(MetadataProtocol.decodeStreamTipResult(fixture.catalog().query(MetadataProtocol.encode(
        new GetStreamTip(fixture.history(), "client", 2, "main")))).status(), Status.FENCED);
    for (Revision invalid : List.of(new Revision(UUID.randomUUID(), validated.index()),
        new Revision(fixture.history(), -1), new Revision(fixture.history(), Long.MAX_VALUE))) {
      String stream = "invalid-" + invalid.index();
      JsonNode rejected = fixture.execute(new ConfigTransaction(fixture.history(), "client", 1, stream, 1,
          invalid, List.of(update)));
      assertStatus(rejected, invalid.historyId().equals(fixture.history()) ? "INVALID_REQUEST" : "HISTORY_MISMATCH");
    }
  }

  @Test
  public void testCommittedReadBarrierPreservesOutcomesAndAdvancesCompleteFeed()
      throws Exception {
    UUID cluster = UUID.randomUUID();
    UUID history = UUID.randomUUID();
    MetadataCatalog catalog = new MetadataCatalog(cluster, history);
    catalog.execute(MetadataProtocol.encode(new RegisterClient(history, "writer", UUID.randomUUID(), 0)), 1);
    byte[] outcome = catalog.execute(MetadataProtocol.encode(new Transaction(history, "writer", 1, "main", 1,
        List.of(Mutation.createClusterConfig(history, UUID.randomUUID(), Map.of("value", "first"))))), 2);
    FeedScope scope = new FeedScope(Set.of(RecordKind.CLUSTER_CONFIG), "test");
    var before = catalog.captureImage(scope, 10, 8192);
    Result revision = MetadataProtocol.decodeResult(catalog.execute(
        MetadataProtocol.encode(new ReadBarrier(new GetRevision(history))), 3));
    assertEquals(revision.revision().index(), 3L);
    Result record = MetadataProtocol.decodeResult(catalog.execute(MetadataProtocol.encode(new ReadBarrier(
        new GetRecord(history, RecordKind.CLUSTER_CONFIG, MetadataProtocol.CLUSTER_CONFIG_NAME))), 4));
    assertEquals(record.revision().index(), 4L);
    assertEquals(record.record().lastModifiedIndex(), 2L);
    assertEquals(record.record().version(), 1L);
    assertEquals(catalog.execute(MetadataProtocol.encode(new ReadBarrier(
        new GetOutcome(history, "writer", 1, "main", 1))), 5), outcome,
        "Reading an outcome must retain the operation's original serialized result and revision");
    catalog.execute(MetadataProtocol.encode(new Acknowledge(history, "writer", 1, "main", 1)), 6);
    Result expired = MetadataProtocol.decodeResult(catalog.execute(MetadataProtocol.encode(new ReadBarrier(
        new GetOutcome(history, "writer", 1, "main", 1))), 7));
    assertEquals(expired.status(), Status.RESULT_EXPIRED);
    assertEquals(expired.revision().index(), 7L);
    MembershipProtocol.Response membership = MembershipProtocol.decodeResponse(catalog.execute(
        MetadataProtocol.encode(new ReadBarrier(new MembershipQuery(history, new MembershipProtocol.GetControl()))),
        8));
    assertEquals(membership.status(), Status.OK);
    assertEquals(membership.revision().index(), 8L);
    Result wrongHistory = MetadataProtocol.decodeResult(catalog.execute(MetadataProtocol.encode(
        new ReadBarrier(new GetRevision(UUID.randomUUID()))), 9));
    assertEquals(wrongHistory.status(), Status.HISTORY_MISMATCH);
    assertEquals(wrongHistory.revision().index(), 9L);
    var changes = catalog.changes(scope, before.cursor(), 20, 20, 8192);
    assertEquals(changes.transactions().size(), 7);
    assertTrue(changes.transactions().stream().allMatch(change -> change.records().isEmpty()));
    MetadataCatalog restored = new MetadataCatalog(cluster, history);
    restored.restoreSnapshot(catalog.captureSnapshot());
    assertEquals(restored.currentRevision(), catalog.currentRevision());
    assertEquals(restored.changes(scope, before.cursor(), 20, 20, 8192), changes);
  }

  @Test
  public void testOversizedRestoredRecordReadPreservesCommittedProgress()
      throws Exception {
    Fixture fixture = new Fixture();
    assertStatus(fixture.transact(Mutation.createClusterConfig(fixture.history(), UUID.randomUUID(), Map.of())), "OK");
    ObjectNode snapshot = (ObjectNode) fixture.catalog().captureSnapshot();
    ObjectNode record = (ObjectNode) snapshot.at("/records/0");
    ObjectNode value = JsonUtils.newObjectNode().put("first", "").put("second", "");
    record.set("value", value);
    int recordLimit = 2 * MetadataProtocol.MAX_COMMAND_BYTES;
    int payload = recordLimit - MetadataProtocol.encodeValue(record).length;
    value.put("first", "x".repeat(payload / 2));
    value.put("second", "x".repeat(payload - payload / 2));
    assertEquals(MetadataProtocol.encodeValue(record).length, recordLimit);
    snapshot.put("journalFloor", fixture.index());
    snapshot.putArray("journal");
    MetadataCatalog restored = new MetadataCatalog(fixture.cluster(), fixture.history());
    restored.restoreSnapshot(snapshot);
    FeedScope scope = new FeedScope(Set.of(RecordKind.CLUSTER_CONFIG), "test");
    var before = restored.captureImage(scope).cursor();

    long readIndex = fixture.index() + 1;
    GetRecord query = new GetRecord(fixture.history(), RecordKind.CLUSTER_CONFIG, MetadataProtocol.CLUSTER_CONFIG_NAME);
    Result oversized = MetadataProtocol.decodeResult(restored.execute(
        MetadataProtocol.encode(new ReadBarrier(query)), readIndex));
    assertEquals(oversized.status(), Status.RESPONSE_TOO_LARGE);
    assertEquals(oversized.revision().index(), readIndex);
    assertEquals(restored.currentRevision().index(), readIndex);
    var changes = restored.changes(scope, before, 2, 1, 1024);
    assertEquals(changes.transactions().size(), 1);
    assertEquals(changes.transactions().getFirst().index(), readIndex);
    assertTrue(changes.transactions().getFirst().records().isEmpty());
    Result next = MetadataProtocol.decodeResult(restored.execute(MetadataProtocol.encode(
        new ReadBarrier(new GetRevision(fixture.history()))), readIndex + 1));
    assertEquals(next.status(), Status.OK);
    assertEquals(next.revision().index(), readIndex + 1);
  }

  @Test
  public void testAtomicTypedRecordsAndImmutableInputs()
      throws Exception {
    Fixture fixture = new Fixture();
    Schema schema = new Schema.SchemaBuilder().setSchemaName("baseballStats")
        .addSingleValueDimension("playerId", DataType.STRING).build();
    TableConfig table = new TableConfigBuilder(TableType.OFFLINE).setTableName("baseballStats").build();
    Map<String, String> settings = new LinkedHashMap<>();
    settings.put("enabled", "true");
    Mutation clusterMutation = Mutation.createClusterConfig(fixture.history(), UUID.randomUUID(), settings);
    Mutation schemaMutation = Mutation.createSchema(fixture.history(), UUID.randomUUID(), schema);
    Mutation tableMutation = Mutation.createTableConfig(fixture.history(), UUID.randomUUID(), table);
    settings.put("enabled", "false");
    ((ObjectNode) clusterMutation.value()).put("enabled", "escaped");
    schema.setSchemaName("changed");
    table.setTableName("changed_OFFLINE");

    JsonNode result = fixture.transact(clusterMutation, schemaMutation, tableMutation);
    assertStatus(result, "OK");
    assertEquals(result.path("versions").size(), 3);
    assertEquals(fixture.read(RecordKind.CLUSTER_CONFIG, "cluster").at("/record/value/enabled").asText(), "true");
    assertEquals(fixture.read(RecordKind.SCHEMA, "baseballStats").at("/record/value/schemaName").asText(),
        "baseballStats");
    assertEquals(fixture.read(RecordKind.TABLE_CONFIG, "baseballStats_OFFLINE")
        .at("/record/value/tableName").asText(), "baseballStats_OFFLINE");
  }

  @Test
  public void testLateValidationRejectsWholeTransactionAndReplaysFailure()
      throws Exception {
    Fixture fixture = new Fixture();
    Mutation valid = Mutation.createClusterConfig(fixture.history(), UUID.randomUUID(), Map.of("k", "v"));
    Mutation invalid = new Mutation(Action.CREATE, RecordKind.SCHEMA, "expected",
        new ObjectVersion(fixture.history(), UUID.randomUUID(), 0),
        JsonUtils.newObjectNode().put("schemaName", "other"));
    Transaction transaction = fixture.transaction("main", 1, valid, invalid);
    JsonNode first = fixture.execute(transaction);
    assertStatus(first, "INVALID_REQUEST");
    assertStatus(fixture.read(RecordKind.CLUSTER_CONFIG, "cluster"), "NOT_FOUND");
    assertEquals(fixture.execute(transaction), first);
    assertStatus(fixture.transact(valid), "OK");
    assertEquals(fixture.execute(transaction), first);
    assertEquals(fixture.catalog().captureSnapshot().path("records").size(), 1);
  }

  @Test
  public void testCasHistoryAndIncarnationPreventRecreateAba()
      throws Exception {
    Fixture fixture = new Fixture();
    UUID oldId = UUID.randomUUID();
    assertStatus(fixture.transact(Mutation.createClusterConfig(fixture.history(), oldId, Map.of("k", "v"))), "OK");
    ObjectVersion oldVersion = new ObjectVersion(fixture.history(), oldId, 1);
    assertStatus(fixture.transact(Mutation.updateClusterConfig(new ObjectVersion(fixture.history(), oldId, 2),
        Map.of("k", "wrong"))), "CONFLICT");
    assertStatus(fixture.transact(Mutation.updateClusterConfig(new ObjectVersion(UUID.randomUUID(), oldId, 1),
        Map.of("k", "wrong"))), "HISTORY_MISMATCH");
    assertStatus(fixture.transact(Mutation.delete(RecordKind.CLUSTER_CONFIG, "cluster", oldVersion)), "OK");
    FeedScope configScope = new FeedScope(Set.of(RecordKind.CLUSTER_CONFIG), "test");
    assertTrue(fixture.catalog().captureImage(configScope).records().isEmpty());
    assertStatus(fixture.transact(Mutation.createClusterConfig(fixture.history(), oldId, Map.of("k", "wrong"))),
        "CONFLICT");
    UUID newId = UUID.randomUUID();
    assertStatus(fixture.transact(Mutation.createClusterConfig(fixture.history(), newId, Map.of("k", "new"))), "OK");
    assertStatus(fixture.transact(Mutation.updateClusterConfig(oldVersion, Map.of("k", "wrong"))), "CONFLICT");
    JsonNode current = fixture.read(RecordKind.CLUSTER_CONFIG, "cluster").path("record");
    assertEquals(current.path("objectId").asText(), newId.toString());
    assertEquals(current.path("version").asLong(), 1L);
    assertEquals(current.path("value").path("k").asText(), "new");
    assertEquals(fixture.catalog().captureImage(configScope).records().getFirst().objectId(), newId);
    assertEquals(fixture.catalog().captureSnapshot().path("records").size(), 2);
  }

  @Test
  public void testSerializedRetryDigestAndContiguousRetirement()
      throws Exception {
    Fixture fixture = new Fixture();
    UUID id = UUID.randomUUID();
    Mutation create = Mutation.createClusterConfig(fixture.history(), id, Map.of("k", "v"));
    assertStatus(fixture.execute(fixture.transaction("main", 2, create)), "OUT_OF_ORDER");
    Transaction original = fixture.transaction("main", 1, create);
    JsonNode first = fixture.execute(original);
    assertStatus(first, "OK");
    assertEquals(fixture.execute(original), first);
    assertEquals(fixture.outcome("main", 1), first);
    assertStatus(fixture.execute(fixture.transaction("main", 1,
        Mutation.createClusterConfig(fixture.history(), id, Map.of("k", "different")))), "CONFLICT");
    Mutation update = Mutation.updateClusterConfig(new ObjectVersion(fixture.history(), id, 1), Map.of("k", "next"));
    Transaction next = fixture.transaction("main", 2, update);
    assertStatus(fixture.execute(next), "UNACKNOWLEDGED_RESULT");
    assertStatus(fixture.execute(new Acknowledge(fixture.history(), "client", 1, "main", 2)), "OUT_OF_ORDER");
    assertEquals(fixture.execute(original), first);
    assertStatus(fixture.execute(new Acknowledge(fixture.history(), "client", 1, "main", 1)), "OK");
    assertStatus(fixture.execute(new Acknowledge(fixture.history(), "client", 1, "main", 1)), "OK");
    assertStatus(fixture.execute(original), "RESULT_EXPIRED");
    assertStatus(fixture.outcome("main", 1), "RESULT_EXPIRED");
    assertStatus(fixture.execute(next), "OK");
    assertEquals(fixture.read(RecordKind.CLUSTER_CONFIG, "cluster").at("/record/value/k").asText(), "next");
  }

  @Test
  public void testRegistrationRetryRetirementAndEpochFences()
      throws Exception {
    Fixture fixture = new Fixture();
    RegisterClient original = new RegisterClient(fixture.history(), "client", fixture.incarnation(), 0);
    assertEquals(fixture.execute(original).path("clientEpoch").asLong(), 1L);
    assertStatus(fixture.execute(new RegisterClient(fixture.history(), "client", UUID.randomUUID(), 0)), "CONFLICT");
    Transaction old = fixture.transaction("main", 1,
        Mutation.createClusterConfig(fixture.history(), UUID.randomUUID(), Map.of("k", "v")));
    assertStatus(fixture.execute(old), "OK");
    assertStatus(fixture.execute(new RetireClient(fixture.history(), "client", 1)), "OK");
    assertStatus(fixture.execute(original), "FENCED");
    assertStatus(fixture.execute(old), "FENCED");
    assertStatus(fixture.outcome("main", 1), "FENCED");
    assertEquals(fixture.execute(new RegisterClient(fixture.history(), "client", UUID.randomUUID(), 1))
        .path("clientEpoch").asLong(), 2L);
    assertStatus(fixture.execute(old), "FENCED");
    MetadataCatalog recovered = new MetadataCatalog(fixture.cluster(), fixture.history());
    recovered.restoreSnapshot(fixture.catalog().captureSnapshot());
    assertStatus(JsonUtils.bytesToJsonNode(recovered.execute(MetadataProtocol.encode(old), fixture.index() + 1)),
        "FENCED");
  }

  @Test
  public void testSnapshotRecoveryReplayAndDetachedCapture()
      throws Exception {
    Fixture fixture = new Fixture();
    UUID id = UUID.randomUUID();
    Transaction create = fixture.transaction("main", 1,
        Mutation.createClusterConfig(fixture.history(), id, Map.of("k", "v")));
    JsonNode original = fixture.execute(create);
    ObjectNode snapshot = (ObjectNode) fixture.catalog().captureSnapshot();
    long snapshotIndex = fixture.index();
    MetadataCatalog recovered = new MetadataCatalog(fixture.cluster(), fixture.history());
    recovered.restoreSnapshot(snapshot);
    ((ObjectNode) snapshot.at("/records/0/value")).put("k", "escaped");
    assertEquals(recovered.captureSnapshot().at("/records/0/value/k").asText(), "v");
    assertEquals(fixture.read(RecordKind.CLUSTER_CONFIG, "cluster").at("/record/value/k").asText(), "v");

    List<Command> suffix = List.of(create, new Acknowledge(fixture.history(), "client", 1, "main", 1),
        fixture.transaction("main", 2,
            Mutation.updateClusterConfig(new ObjectVersion(fixture.history(), id, 1), Map.of("k", "updated"))));
    long replayIndex = snapshotIndex;
    for (Command command : suffix) {
      JsonNode uninterrupted = fixture.execute(command);
      JsonNode replayed = JsonUtils.bytesToJsonNode(recovered.execute(MetadataProtocol.encode(command), ++replayIndex));
      assertEquals(replayed, uninterrupted);
    }
    assertEquals(recovered.captureSnapshot(), fixture.catalog().captureSnapshot());
    assertEquals(original.path("revision").path("index").asLong(), snapshotIndex);
    assertStatus(JsonUtils.bytesToJsonNode(recovered.query(MetadataProtocol.encode(
        new GetOutcome(fixture.history(), "client", 1, "main", 1)))), "RESULT_EXPIRED");
  }

  @Test
  public void testInvalidSnapshotNeverChangesCurrentState()
      throws Exception {
    Fixture fixture = new Fixture();
    assertStatus(fixture.transact(Mutation.createClusterConfig(fixture.history(), UUID.randomUUID(), Map.of("k", "v"))),
        "OK");
    JsonNode original = fixture.catalog().captureSnapshot();
    List<ObjectNode> corruptions = new ArrayList<>();
    ObjectNode history = original.deepCopy();
    history.put("historyId", UUID.randomUUID().toString());
    corruptions.add(history);
    ObjectNode format = original.deepCopy();
    format.put("formatVersion", 99);
    corruptions.add(format);
    ObjectNode duplicate = original.deepCopy();
    ArrayNode records = (ArrayNode) duplicate.path("records");
    records.add(records.get(0).deepCopy());
    corruptions.add(duplicate);
    ObjectNode invalidIndex = original.deepCopy();
    invalidIndex.put("lastAppliedIndex", 0);
    corruptions.add(invalidIndex);
    ObjectNode invalidFloor = original.deepCopy();
    ((ObjectNode) invalidFloor.at("/clients/0/streams/s0")).put("floor", 100);
    corruptions.add(invalidFloor);
    ObjectNode unknown = original.deepCopy();
    unknown.put("unsupported", true);
    corruptions.add(unknown);
    for (ObjectNode corrupted : corruptions) {
      assertThrows(IOException.class, () -> fixture.catalog().restoreSnapshot(corrupted));
      assertEquals(fixture.catalog().captureSnapshot(), original);
    }
  }

  @Test
  public void testStrictCommittedDecoderDoesNotAdvanceOnUnsupportedData()
      throws Exception {
    Fixture fixture = new Fixture();
    String valid = new String(
        MetadataProtocol.encode(new RegisterClient(fixture.history(), "new", UUID.randomUUID(), 0)),
        StandardCharsets.UTF_8);
    List<String> invalid = List.of(valid + " {}", valid.replace("\"formatVersion\":1", "\"formatVersion\":2"),
        valid.replace("REGISTER_CLIENT", "FUTURE_COMMAND"),
        valid.replace("\"expectedEpoch\":0", "\"expectedEpoch\":0.5"),
        valid.replace("\"expectedEpoch\":0", "\"expectedEpoch\":\"0\""),
        valid.replace("\"expectedEpoch\":0", "\"expectedEpoch\":0,\"expectedEpoch\":0"),
        valid.replace("\"expectedEpoch\":0", "\"expectedEpoch\":0,\"unknown\":true"));
    JsonNode before = fixture.catalog().captureSnapshot();
    for (String value : invalid) {
      assertNotEquals(value, valid);
      assertThrows(IOException.class,
          () -> fixture.catalog().execute(value.getBytes(StandardCharsets.UTF_8), fixture.index() + 1));
      assertEquals(fixture.catalog().captureSnapshot(), before);
    }
    assertThrows(IOException.class,
        () -> fixture.catalog().execute(valid.getBytes(StandardCharsets.UTF_8), fixture.index()));
  }

  @Test
  public void testMembershipTimingRequiresNewLogAndSnapshotFormats()
      throws Exception {
    UUID cluster = UUID.randomUUID();
    UUID history = UUID.randomUUID();
    MetadataCatalog catalog = new MetadataCatalog(cluster, history);
    RegisterClient registration = new RegisterClient(history, "writer", UUID.randomUUID(), 0);
    byte[] unchanged = MetadataProtocol.encode(registration);
    assertEquals(JsonUtils.bytesToJsonNode(unchanged).path("formatVersion").asInt(), 1);
    assertEquals(MetadataProtocol.decodeCommand(unchanged), registration);
    catalog.execute(unchanged, 1);

    MembershipCommand activate = new MembershipCommand(history, "controller",
        new ActivateControlEpoch(1, "controller", UUID.randomUUID()));
    byte[] current = MetadataProtocol.encode(activate);
    assertEquals(JsonUtils.bytesToJsonNode(current).path("formatVersion").asInt(), 2);
    assertEquals(MetadataProtocol.decodeCommand(current), activate);
    ObjectNode legacyCommand = (ObjectNode) JsonUtils.bytesToJsonNode(current);
    legacyCommand.put("formatVersion", 1);
    JsonNode before = catalog.captureSnapshot();
    IOException oldLog = expectThrows(IOException.class,
        () -> catalog.execute(MetadataProtocol.encodeValue(legacyCommand), 1, 2));
    assertTrue(oldLog.getMessage().contains("maintenance-window migration to a new history"));
    assertEquals(catalog.captureSnapshot(), before, "A rejected old membership log cannot advance apply");
    assertEquals(MembershipProtocol.decodeResponse(catalog.execute(current, 1, 2)).status(), Status.OK);
    JsonNode initialized = catalog.captureSnapshot();
    assertEquals(initialized.path("formatVersion").asInt(), 10);
    for (int version = 3; version <= 7; version++) {
      ObjectNode legacy = initialized.deepCopy();
      legacy.put("formatVersion", version);
      ((ObjectNode) legacy.at("/membership/control")).remove(
          List.of("minimumTickDelayMillis", "maxConfiguredPermitLifetimeMillis"));
      for (JsonNode records : legacy.findValues("records")) {
        for (JsonNode record : records) {
          if (record.path("kind").asText().equals("MEMBERSHIP_CONTROL")) {
            ((ObjectNode) record.path("value")).remove(
                List.of("minimumTickDelayMillis", "maxConfiguredPermitLifetimeMillis"));
          }
        }
      }
      IOException oldSnapshot = expectThrows(IOException.class, () -> catalog.restoreSnapshot(legacy));
      assertTrue(oldSnapshot.getMessage().contains("maintenance-window migration"));
      assertEquals(catalog.captureSnapshot(), initialized);
    }
    for (String missing : List.of("minimumTickDelayMillis", "maxConfiguredPermitLifetimeMillis")) {
      ObjectNode incomplete = initialized.deepCopy();
      ((ObjectNode) incomplete.at("/membership/control")).remove(missing);
      assertThrows(IOException.class, () -> catalog.restoreSnapshot(incomplete));
      assertEquals(catalog.captureSnapshot(), initialized);
    }
  }

  @Test
  public void testCanonicalDigestPreservesDecimalPrecisionAndMapSemantics()
      throws Exception {
    Fixture fixture = new Fixture();
    UUID id = UUID.randomUUID();
    ObjectNode first = JsonUtils.newObjectNode().put("schemaName", "schema");
    first.put("numericMarker", new BigDecimal("1.000000000000000000000000000001"));
    ObjectNode second = JsonUtils.newObjectNode().put("numericMarker",
        new BigDecimal("1.000000000000000000000000000002")).put("schemaName", "schema");
    Transaction left = fixture.transaction("main", 1, new Mutation(Action.CREATE, RecordKind.SCHEMA, "schema",
        new ObjectVersion(fixture.history(), id, 0), first));
    Transaction right = fixture.transaction("main", 1, new Mutation(Action.CREATE, RecordKind.SCHEMA, "schema",
        new ObjectVersion(fixture.history(), id, 0), second));
    assertNotEquals(MetadataProtocol.digest(MetadataProtocol.decodeCommand(MetadataProtocol.encode(left))),
        MetadataProtocol.digest(MetadataProtocol.decodeCommand(MetadataProtocol.encode(right))));
    ObjectNode reordered = JsonUtils.newObjectNode();
    reordered.set("numericMarker", first.get("numericMarker"));
    reordered.put("schemaName", "schema");
    Transaction same = fixture.transaction("main", 1, new Mutation(Action.CREATE, RecordKind.SCHEMA, "schema",
        new ObjectVersion(fixture.history(), id, 0), reordered));
    assertEquals(MetadataProtocol.digest(left), MetadataProtocol.digest(same));
    assertTrue(new String(MetadataProtocol.encode(left), StandardCharsets.UTF_8)
        .contains("1.000000000000000000000000000001"));
  }

  @Test
  public void testMalformedPayloadAndBoundedTransaction()
      throws Exception {
    Fixture fixture = new Fixture();
    Mutation missingType = new Mutation(Action.CREATE, RecordKind.TABLE_CONFIG, "bad_OFFLINE",
        new ObjectVersion(fixture.history(), UUID.randomUUID(), 0),
        JsonUtils.newObjectNode().put("tableName", "bad_OFFLINE"));
    assertStatus(fixture.transact(missingType), "INVALID_REQUEST");
    Mutation create = Mutation.createClusterConfig(fixture.history(), UUID.randomUUID(), Map.of("k", "v"));
    assertStatus(fixture.transact(create, create), "INVALID_REQUEST");
    assertFalse(fixture.catalog().captureSnapshot().path("records").elements().hasNext());
    List<Mutation> many = new ArrayList<>();
    for (int i = 0; i <= MetadataProtocol.MAX_MUTATIONS; i++) {
      many.add(create);
    }
    assertStatus(fixture.execute(new Transaction(fixture.history(), "client", 1, "bounded", 1, many)),
        "INVALID_REQUEST");
    assertThrows(IOException.class, () -> MetadataProtocol.encode(fixture.transaction("large", 1,
        Mutation.createClusterConfig(fixture.history(), UUID.randomUUID(),
            Map.of("large", "x".repeat(MetadataProtocol.MAX_COMMAND_BYTES))))));
  }

  private static void assertStatus(JsonNode result, String expected) {
    assertEquals(result.path("status").asText(), expected, result.toString());
  }

  private static final class Fixture {
    private final UUID _cluster = UUID.randomUUID();
    private final UUID _history = UUID.randomUUID();
    private final UUID _incarnation = UUID.randomUUID();
    private final MetadataCatalog _catalog = new MetadataCatalog(_cluster, _history);
    private long _index;
    private int _stream;

    private Fixture()
        throws IOException {
      assertStatus(execute(new RegisterClient(_history, "client", _incarnation, 0)), "OK");
    }

    private JsonNode execute(Command command)
        throws IOException {
      return JsonUtils.bytesToJsonNode(_catalog.execute(MetadataProtocol.encode(command), ++_index));
    }

    private MembershipProtocol.Response member(MembershipProtocol.Operation operation)
        throws IOException {
      return MembershipProtocol.decodeResponse(_catalog.execute(MetadataProtocol.encode(
          new MembershipCommand(_history, "membership", operation)), 1, ++_index));
    }

    private JsonNode transact(Mutation... mutations)
        throws IOException {
      return execute(transaction("s" + _stream++, 1, mutations));
    }

    private Transaction transaction(String stream, long sequence, Mutation... mutations) {
      return new Transaction(_history, "client", 1, stream, sequence, List.of(mutations));
    }

    private JsonNode read(RecordKind kind, String name)
        throws IOException {
      return JsonUtils.bytesToJsonNode(_catalog.query(MetadataProtocol.encode(new GetRecord(_history, kind, name))));
    }

    private JsonNode outcome(String stream, long sequence)
        throws IOException {
      return JsonUtils.bytesToJsonNode(_catalog.query(MetadataProtocol.encode(new GetOutcome(_history, "client", 1,
          stream, sequence))));
    }

    private UUID cluster() {
      return _cluster;
    }

    private UUID history() {
      return _history;
    }

    private UUID incarnation() {
      return _incarnation;
    }

    private MetadataCatalog catalog() {
      return _catalog;
    }

    private long index() {
      return _index;
    }
  }
}
