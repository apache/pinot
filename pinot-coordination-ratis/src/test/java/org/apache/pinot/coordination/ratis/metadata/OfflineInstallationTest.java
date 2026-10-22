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
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.LongFunction;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ActivateControlEpoch;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.BeginDrain;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Capability;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ConfigurePolicy;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ParticipantToken;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Policy;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Register;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Response;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Role;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.SetReady;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Tick;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Unregister;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Acknowledge;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.CatalogRecord;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Command;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.GetInstallationWork;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.GetRecord;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.InstallationSessionCommand;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.MembershipCommand;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Mutation;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.NativeInstallationCommand;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.NativeOperationCommand;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.ObjectVersion;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.OfflineSegmentCommand;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.OwnershipCommand;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.RecordKind;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.RegisterClient;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Result;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Revision;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Status;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Transaction;
import org.apache.pinot.coordination.ratis.metadata.OfflineInstallationProtocol.InstallationSnapshot;
import org.apache.pinot.coordination.ratis.metadata.OfflineInstallationProtocol.ReportInstallation;
import org.apache.pinot.coordination.ratis.metadata.OfflineInstallationProtocol.Work;
import org.apache.pinot.coordination.ratis.metadata.OfflineInstallationProtocol.WorkResult;
import org.apache.pinot.coordination.ratis.metadata.OfflineSegmentProtocol.AssignReplica;
import org.apache.pinot.coordination.ratis.metadata.OfflineSegmentProtocol.Compatibility;
import org.apache.pinot.coordination.ratis.metadata.OfflineSegmentProtocol.Content;
import org.apache.pinot.coordination.ratis.metadata.OfflineSegmentProtocol.LoaderDescriptor;
import org.apache.pinot.coordination.ratis.metadata.OfflineSegmentProtocol.PublishContent;
import org.apache.pinot.coordination.ratis.metadata.OperationProtocol.AssignedOfflineSegmentReload;
import org.apache.pinot.coordination.ratis.metadata.OperationProtocol.Cancel;
import org.apache.pinot.coordination.ratis.metadata.OperationProtocol.Claim;
import org.apache.pinot.coordination.ratis.metadata.OperationProtocol.Complete;
import org.apache.pinot.coordination.ratis.metadata.OperationProtocol.CompleteInstalled;
import org.apache.pinot.coordination.ratis.metadata.OperationProtocol.OfflineSegmentReload;
import org.apache.pinot.coordination.ratis.metadata.OperationProtocol.OperationState;
import org.apache.pinot.coordination.ratis.metadata.OperationProtocol.Progress;
import org.apache.pinot.coordination.ratis.metadata.OperationProtocol.State;
import org.apache.pinot.coordination.ratis.metadata.OperationProtocol.Submit;
import org.apache.pinot.coordination.ratis.metadata.OperationProtocol.TargetMaterialization;
import org.apache.pinot.coordination.ratis.metadata.OwnershipProtocol.AssignOwner;
import org.apache.pinot.coordination.ratis.metadata.OwnershipProtocol.ConfigureLayout;
import org.apache.pinot.coordination.ratis.metadata.OwnershipProtocol.OwnerToken;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/// Installation evidence must survive retries and recovery without allowing stale attempts or server sessions to
/// certify a current operation. Real participant filesystem installation is exercised by the core action tests.
public class OfflineInstallationTest {
  private static final String TABLE = "orders_OFFLINE";

  @Test
  public void testOwnSessionBootstrapProjectionBoundsAndRecovery()
      throws Exception {
    Fixture fixture = new Fixture();
    Work work = fixture.work(fixture._first).work();
    assertEquals(fixture.report(fixture._second, report(fixture.work(fixture._second).work())).status(), Status.OK);
    assertEquals(fixture.controller(sequence -> new Transaction(fixture._history, "controller", 1,
        "coordination", sequence, List.of(
            Mutation.createClusterConfig(fixture._history, UUID.randomUUID(), Map.of("setting", "value")),
            Mutation.createTableConfig(fixture._history, UUID.randomUUID(),
                new TableConfigBuilder(TableType.OFFLINE).setTableName("unassigned").build()),
            Mutation.createSchema(fixture._history, UUID.randomUUID(), new Schema.SchemaBuilder()
                .setSchemaName("unassigned").addSingleValueDimension("id", DataType.INT).build())))).status(),
        Status.OK);
    assertEquals(fixture.member(new SetReady(fixture._first, new Revision(fixture._history, fixture._index), false))
        .status(), Status.OK);
    InstallationSnapshot snapshot = fixture._catalog.installationSnapshot(fixture._first, 128,
        OfflineInstallationProtocol.MAX_SNAPSHOT_BYTES);
    assertEquals(snapshot.status(), Status.OK);
    assertFalse(snapshot.participant().ready());
    assertEquals(snapshot.participant().participant(), fixture._first);
    assertEquals(snapshot.control(), fixture._catalog.membershipControl());
    assertEquals(snapshot.revision(), new Revision(fixture._history, fixture._index));
    assertEquals(snapshot.records().size(), 5);
    assertEquals(snapshot.operationIds(), List.of(fixture._operation));
    assertEquals(fixture.work(fixture._first).status(), Status.FENCED,
        "Bootstrap access must not allow an unready server to execute work");
    for (CatalogRecord record : snapshot.records()) {
      assertTrue(record.lastModifiedIndex() <= snapshot.revision().index());
      assertFalse(record.name().contains("unassigned"));
      if (record.kind() == RecordKind.OFFLINE_REPLICA_ASSIGNMENT) {
        assertEquals(OfflineSegmentProtocol.decodeAssignment(record.value()).server(), fixture._first);
      }
    }
    int encodedBytes = MetadataProtocol.encodeValue(snapshot).length;
    assertEquals(fixture._catalog.installationSnapshot(fixture._first, 6, encodedBytes), snapshot);
    for (InstallationSnapshot rejected : List.of(
        fixture._catalog.installationSnapshot(fixture._first, 5, encodedBytes),
        fixture._catalog.installationSnapshot(fixture._first, 6, encodedBytes - 1))) {
      assertEquals(rejected.status(), Status.RESPONSE_TOO_LARGE);
      assertTrue(rejected.records().isEmpty());
      assertTrue(rejected.operationIds().isEmpty());
      assertEquals(rejected.participant(), null);
      assertEquals(rejected.control(), null);
    }
    // Metadata is reconstructed from the durable catalog after recovery, with no separate projection ledger.
    MetadataCatalog restored = new MetadataCatalog(fixture._cluster, fixture._history);
    restored.restoreSnapshot(fixture._catalog.captureSnapshot());
    assertEquals(restored.installationSnapshot(fixture._first, 128,
        OfflineInstallationProtocol.MAX_SNAPSHOT_BYTES), snapshot);
    assertEquals(fixture.operation(new Cancel(fixture._operation, 2, fixture._owner, "cancelled before installation"))
        .status(), Status.OK);
    assertEquals(fixture.segment(new AssignReplica(fixture._owner, work.content().segmentObjectId(), 1,
        fixture._first, work.target().compatibility(), false)).status(), Status.OK);
    InstallationSnapshot released = fixture._catalog.installationSnapshot(fixture._first, 128,
        OfflineInstallationProtocol.MAX_SNAPSHOT_BYTES);
    assertTrue(released.operationIds().isEmpty());
    assertTrue(released.records().stream().noneMatch(record -> record.kind() == RecordKind.OFFLINE_REPLICA_ASSIGNMENT),
        "Complete-image omission revokes released assignments without retaining unbounded release history");
    assertEquals(fixture.controller(sequence -> new Transaction(fixture._history, "controller", 1, "coordination",
        sequence, List.of(Mutation.delete(RecordKind.TABLE_CONFIG, TABLE, work.target().compatibility().table()),
            Mutation.delete(RecordKind.SCHEMA, "orders", work.target().compatibility().schema())))).status(),
        Status.OK);
    InstallationSnapshot deleted = fixture._catalog.installationSnapshot(fixture._second, 128,
        OfflineInstallationProtocol.MAX_SNAPSHOT_BYTES);
    assertEquals(deleted.status(), Status.OK, "A deleted table cannot block unrelated server metadata hydration");
    assertEquals(deleted.records().size(), 5);
    assertEquals(deleted.records().stream().filter(CatalogRecord::deleted).count(), 2);
    assertTrue(deleted.records().stream().anyMatch(record -> record.objectId().equals(fixture._table)
        && record.deleted()), "The exact old table incarnation remains a visible fence");
    restored.restoreSnapshot(fixture._catalog.captureSnapshot());
    assertEquals(restored.installationSnapshot(fixture._second, 128,
        OfflineInstallationProtocol.MAX_SNAPSHOT_BYTES), deleted);
    UUID replacementTable = UUID.randomUUID();
    assertEquals(fixture.controller(sequence -> new Transaction(fixture._history, "controller", 1, "coordination",
        sequence, List.of(Mutation.createTableConfig(fixture._history, replacementTable,
                new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE).build()),
            Mutation.createSchema(fixture._history, UUID.randomUUID(), new Schema.SchemaBuilder()
                .setSchemaName("orders").addSingleValueDimension("id", DataType.INT).build())))).status(), Status.OK);
    InstallationSnapshot recreated = fixture._catalog.installationSnapshot(fixture._second, 128,
        OfflineInstallationProtocol.MAX_SNAPSHOT_BYTES);
    assertEquals(recreated.status(), Status.OK);
    assertTrue(recreated.records().stream().anyMatch(record -> record.objectId().equals(fixture._table)
        && record.deleted()), "Name recreation must preserve the assignment's original incarnation fence");
    assertFalse(recreated.records().stream().anyMatch(record -> record.objectId().equals(replacementTable)),
        "An unassigned replacement table must not substitute for the old referenced object");
    assertEquals(fixture.member(new BeginDrain(fixture._first)).status(), Status.OK);
    InstallationSnapshot draining = fixture._catalog.installationSnapshot(fixture._first, 128,
        OfflineInstallationProtocol.MAX_SNAPSHOT_BYTES);
    assertEquals(draining.status(), Status.OK, "Draining readers still need a coherent membership projection");
    assertTrue(draining.participant().draining());
    assertFalse(draining.participant().ready());
    assertEquals(fixture.work(fixture._first).status(), Status.FENCED,
        "A draining projection must not authorize installation work");
    assertEquals(fixture.member(new Unregister(fixture._first)).status(), Status.OK);
    ParticipantToken replacement = fixture.member(new Register(fixture._first.participantId(), Role.SERVER,
        UUID.randomUUID(), 1, Map.of(), Set.of(Capability.SERVER_QUERY))).participant().participant();
    assertEquals(fixture._catalog.installationSnapshot(fixture._first, 128,
        OfflineInstallationProtocol.MAX_SNAPSHOT_BYTES).status(), Status.FENCED);
    InstallationSnapshot empty = fixture._catalog.installationSnapshot(replacement, 128,
        OfflineInstallationProtocol.MAX_SNAPSHOT_BYTES);
    assertEquals(empty.status(), Status.OK);
    assertFalse(empty.participant().ready());
    assertEquals(empty.records().size(), 1, "Replacement must not inherit the old registration's assignments");
    assertEquals(empty.records().getFirst().kind(), RecordKind.CLUSTER_CONFIG);
    for (int round = 0; round < 3; round++) {
      assertEquals(fixture.member(new Tick(fixture._catalog.membershipControl().controlEpoch(), round, 1)).status(),
          Status.OK);
    }
    assertEquals(fixture._catalog.installationSnapshot(replacement, 128,
        OfflineInstallationProtocol.MAX_SNAPSHOT_BYTES).status(), Status.FENCED);
  }

  @Test
  public void testEveryTargetReceiptRequiredAndRequestReplayIsStable()
      throws Exception {
    Fixture fixture = new Fixture();
    assertEquals(fixture.complete().status(), Status.NOT_READY);
    Work first = fixture.work(fixture._first).work();
    assertTrue(first.initialLoad());
    ReportInstallation report = report(first);
    InstallationSessionCommand command = fixture.reportCommand(fixture._first, report);
    Result accepted = fixture.apply(command);
    assertEquals(accepted.status(), Status.OK);
    assertEquals(fixture.apply(command), accepted);
    assertEquals(fixture.complete().status(), Status.NOT_READY);
    assertEquals(fixture.report(fixture._first, new ReportInstallation(report.operationId(), report.attempt(),
        report.owner(), report.installed(), UUID.randomUUID(), report.manifestSha256())).status(), Status.CONFLICT);
    assertEquals(fixture.report(fixture._second, report(fixture.work(fixture._second).work())).status(), Status.OK);
    assertEquals(fixture.operation(new Complete(fixture._operation, 2, fixture._owner, 1, State.SUCCEEDED,
        new Progress(2, 0, "unverified counters"), List.of())).status(), Status.INVALID_REQUEST);
    assertEquals(fixture.complete().status(), Status.OK);
    OperationState completed = fixture.state();
    assertEquals(completed.status(), State.SUCCEEDED);
    assertEquals(completed.result().completedTargets(), 2);
    assertEquals(completed.completionProof().installations().size(), 2);
    // A retry returns the original durable acceptance even after the operation has completed.
    assertEquals(fixture.apply(fixture._lastReport), fixture._lastReportResult);
    MetadataCatalog restored = new MetadataCatalog(fixture._cluster, fixture._history);
    restored.restoreSnapshot(fixture._catalog.captureSnapshot());
    assertEquals(restored.captureSnapshot(), fixture._catalog.captureSnapshot());
  }

  @Test
  public void testAttemptAndParticipantSessionFenceReports()
      throws Exception {
    Fixture fixture = new Fixture();
    Work prior = fixture.work(fixture._first).work();
    ReportInstallation stale = report(prior);
    assertEquals(fixture.report(fixture._second, stale).status(), Status.FENCED);
    AssignedOfflineSegmentReload assigned = (AssignedOfflineSegmentReload) fixture.state().input();
    assertEquals(fixture.operation(new Claim(fixture._operation, 2, fixture._owner, assigned.dependencies())).status(),
        Status.OK);
    assertEquals(fixture.report(fixture._first, stale).status(), Status.FENCED);
    ReportInstallation current = report(fixture.work(fixture._first).work());
    assertEquals(fixture.report(fixture._first, current).status(), Status.OK);
    assertEquals(fixture.member(new Unregister(fixture._first)).status(), Status.OK);
    ParticipantToken replacement = fixture.register("server-a", Role.SERVER, 1);
    assertNotEquals(OfflineInstallationProtocol.clientId(replacement),
        OfflineInstallationProtocol.clientId(fixture._first));
    assertEquals(fixture.apply(new InstallationSessionCommand(fixture._first,
        new Acknowledge(fixture._history, OfflineInstallationProtocol.clientId(fixture._first), 1, "installation",
            fixture._serverSequences.get(fixture._first.participantId())))).status(), Status.FENCED);
    assertEquals(fixture.work(replacement).status(), Status.FENCED);
    assertEquals(fixture.complete().status(), Status.FENCED);
    // A replacement process gets a new desired materialization and a separate explicit reconciliation operation.
    assertEquals(fixture.segment(new AssignReplica(fixture._owner, prior.content().segmentObjectId(), 1,
        replacement, prior.target().compatibility(), true)).status(), Status.OK);
    fixture._operation = UUID.randomUUID();
    UUID assignmentId = prior.target().assignment().objectId();
    TargetMaterialization target = new TargetMaterialization(replacement, 2,
        OfflineSegmentProtocol.materializationId(fixture._history, assignmentId, 2));
    OfflineSegmentReload reconcile = new OfflineSegmentReload(TABLE, fixture._table, prior.content().segmentName(),
        prior.content().segmentObjectId(), prior.content().contentSha256(), List.of(target), false, false, true, 1234);
    assertEquals(fixture.operation(new Submit(fixture._operation, reconcile)).status(), Status.OK);
    AssignedOfflineSegmentReload replacementIntent = (AssignedOfflineSegmentReload) fixture.state().input();
    assertEquals(fixture.operation(new Claim(fixture._operation, 1, fixture._owner, replacementIntent.dependencies()))
        .status(), Status.OK);
    Work reconciled = fixture.work(replacement).work();
    assertTrue(reconciled.reconcile());
    assertFalse(reconciled.initialLoad());
    assertEquals(replacementIntent.reload().submittedAtMs(), 1234);
    assertEquals(fixture.work(fixture._first).status(), Status.FENCED);
    assertEquals(fixture.apply(new InstallationSessionCommand(replacement,
        new RegisterClient(fixture._history, OfflineInstallationProtocol.clientId(replacement),
            replacement.incarnation(), 0))).status(), Status.OK);
    fixture._serverSequences.put(replacement.participantId(), 0L);
    assertEquals(fixture.report(replacement, report(reconciled)).status(), Status.OK);
    assertEquals(fixture.complete().status(), Status.OK);
    JsonNode snapshot = fixture._catalog.captureSnapshot();
    MetadataCatalog restored = new MetadataCatalog(fixture._cluster, fixture._history);
    restored.restoreSnapshot(snapshot);
    assertEquals(restored.captureSnapshot(), snapshot);
    ObjectNode oldFormat = snapshot.deepCopy();
    oldFormat.put("formatVersion", 6);
    assertThrows(IOException.class, () -> restored.restoreSnapshot(oldFormat));
  }

  @Test
  public void testCompletionProofCannotBeInventedOnRestoreAndLegacyEncodingIsStable()
      throws Exception {
    Fixture fixture = new Fixture();
    OperationState running = fixture.state();
    String encoded = new String(MetadataProtocol.encodeValue(running), StandardCharsets.UTF_8);
    assertFalse(encoded.contains("completionProof"));
    assertEquals(OperationProtocol.decodeOperation(MetadataProtocol.toTree(running)), running);
    OfflineSegmentReload reload = ((AssignedOfflineSegmentReload) running.input()).reload();
    OfflineSegmentReload legacy = new OfflineSegmentReload(reload.tableName(), reload.tableObjectId(),
        reload.segmentName(), reload.segmentObjectId(), reload.contentSha256(), reload.targets());
    String legacyJson = new String(MetadataProtocol.encodeValue(legacy), StandardCharsets.UTF_8);
    assertFalse(legacyJson.contains("initialLoad"));
    assertFalse(legacyJson.contains("forceDownload"));
    assertFalse(legacyJson.contains("reconcile"));
    assertFalse(legacyJson.contains("submittedAtMs"));
    assertEquals(MetadataProtocol.fromTree(MetadataProtocol.toTree(legacy), OfflineSegmentReload.class), legacy);
    for (String name : List.of("initialLoad", "reconcile", "submittedAtMs")) {
      ObjectNode invalid = MetadataProtocol.toTree(legacy).deepCopy();
      invalid.putNull(name);
      assertThrows(IOException.class, () -> MetadataProtocol.fromTree(invalid, OfflineSegmentReload.class));
    }
    assertEquals(fixture.report(fixture._first, report(fixture.work(fixture._first).work())).status(), Status.OK);
    assertEquals(fixture.report(fixture._second, report(fixture.work(fixture._second).work())).status(), Status.OK);
    assertEquals(fixture.complete().status(), Status.OK);
    JsonNode valid = fixture._catalog.captureSnapshot();
    ObjectNode versionSix = valid.deepCopy();
    versionSix.put("formatVersion", 6);
    MetadataCatalog migrated = new MetadataCatalog(fixture._cluster, fixture._history);
    assertThrows(IOException.class, () -> migrated.restoreSnapshot(versionSix));
    ObjectNode corrupt = valid.deepCopy();
    String missingReceipt = UUID.randomUUID().toString();
    // Keep the journal and latest image identical; recovery must reject the proof's missing referenced receipt.
    for (JsonNode records : corrupt.findValues("records")) {
      for (JsonNode record : records) {
        if (record.path("kind").asText().equals(RecordKind.CONTROLLER_OPERATION.name())
            && record.path("value").hasNonNull("completionProof")) {
          ((ObjectNode) record.at("/value/completionProof/installations/0")).put("objectId", missingReceipt);
        }
      }
    }
    MetadataCatalog restored = new MetadataCatalog(fixture._cluster, fixture._history);
    restored.restoreSnapshot(valid);
    assertThrows(IOException.class, () -> restored.restoreSnapshot(corrupt));
    assertEquals(restored.captureSnapshot(), valid);
    ObjectNode oldFormat = valid.deepCopy();
    oldFormat.put("formatVersion", 5);
    assertThrows(IOException.class, () -> restored.restoreSnapshot(oldFormat));
  }

  private static ReportInstallation report(Work work) {
    return new ReportInstallation(work.operationId(), work.attempt(), work.owner(), work.target(), UUID.randomUUID(),
        "a".repeat(64));
  }

  private static final class Fixture {
    private final UUID _cluster = UUID.randomUUID();
    private final UUID _history = UUID.randomUUID();
    private final UUID _table = UUID.randomUUID();
    private UUID _operation = UUID.randomUUID();
    private final MetadataCatalog _catalog = new MetadataCatalog(_cluster, _history);
    private final Map<String, Long> _serverSequences = new HashMap<>();
    private final ParticipantToken _first;
    private final ParticipantToken _second;
    private final OwnerToken _owner;
    private long _index;
    private long _sequence;
    private InstallationSessionCommand _lastReport;
    private Result _lastReportResult;

    private Fixture()
        throws Exception {
      assertEquals(apply(new RegisterClient(_history, "controller", UUID.randomUUID(), 0)).status(), Status.OK);
      assertEquals(member(new ActivateControlEpoch(1, "controller", UUID.randomUUID())).status(), Status.OK);
      assertEquals(member(new ConfigurePolicy(_catalog.membershipControl().controlEpoch(), 0,
          new Policy(1, 100, 3, 200, 300, 1000))).status(), Status.OK);
      ParticipantToken controller = register("controller", Role.CONTROLLER, 0);
      _first = register("server-a", Role.SERVER, 0);
      _second = register("server-b", Role.SERVER, 0);
      OwnershipProtocol.Layout layout = new OwnershipProtocol.Layout(1, 24,
          OwnershipProtocol.HashVersion.MURMUR2_UTF8_MASK_POSITIVE_V1);
      assertEquals(owner(new ConfigureLayout(_catalog.membershipControl().controlEpoch(), layout)).status(), Status.OK);
      OwnershipProtocol.TableShard scope = OwnershipProtocol.tableScope(layout, TABLE);
      assertEquals(owner(new AssignOwner(_catalog.membershipControl().controlEpoch(), 1, scope, 0, controller))
          .status(), Status.OK);
      _owner = new OwnerToken(_history, 1, scope, 1, controller);
      assertEquals(controller(sequence -> new Transaction(_history, "controller", 1, "coordination", sequence, List.of(
          Mutation.createTableConfig(_history, _table, new TableConfigBuilder(TableType.OFFLINE).setTableName("orders")
              .build()), Mutation.createSchema(_history, UUID.randomUUID(), new Schema.SchemaBuilder()
              .setSchemaName("orders").addSingleValueDimension("id", DataType.INT).build())))).status(), Status.OK);
      Content content = new Content(TABLE, _table, "segment", UUID.randomUUID(), "b".repeat(64),
          "file:///immutable/segment.tar.gz", new LoaderDescriptor(123, -1, false, null, null, 0, 0, Map.of()));
      Compatibility compatibility = new Compatibility(version(record(RecordKind.TABLE_CONFIG, TABLE)),
          version(record(RecordKind.SCHEMA, "orders")));
      assertEquals(segment(new PublishContent(_owner, compatibility.table(), content)).status(), Status.OK);
      for (ParticipantToken server : List.of(_first, _second)) {
        assertEquals(segment(new AssignReplica(_owner, content.segmentObjectId(), 0, server, compatibility, true))
            .status(), Status.OK);
        assertEquals(apply(new InstallationSessionCommand(server,
            new RegisterClient(_history, OfflineInstallationProtocol.clientId(server), server.incarnation(), 0)))
            .status(), Status.OK);
      }
      List<TargetMaterialization> targets = List.of(target(content, _first), target(content, _second));
      assertEquals(operation(new Submit(_operation, new OfflineSegmentReload(TABLE, _table, content.segmentName(),
          content.segmentObjectId(), content.contentSha256(), targets, false, true))).status(), Status.OK);
      AssignedOfflineSegmentReload assigned = (AssignedOfflineSegmentReload) state().input();
      assertEquals(operation(new Claim(_operation, 1, _owner, assigned.dependencies())).status(), Status.OK);
    }

    private TargetMaterialization target(Content content, ParticipantToken server) {
      UUID assignmentId = OfflineSegmentProtocol.assignmentObjectId(_history, content.segmentObjectId(),
          server.participantId());
      return new TargetMaterialization(server, 1, OfflineSegmentProtocol.materializationId(_history, assignmentId, 1));
    }

    private ParticipantToken register(String id, Role role, long epoch)
        throws IOException {
      Response registered = member(new Register(id, role, UUID.randomUUID(), epoch, Map.of(),
          role == Role.SERVER ? Set.of(Capability.SERVER_QUERY) : Set.of()));
      assertEquals(registered.status(), Status.OK);
      ParticipantToken token = registered.participant().participant();
      assertEquals(member(new SetReady(token, new Revision(_history, _index), true)).status(), Status.OK);
      return token;
    }

    private Response member(MembershipProtocol.Operation operation)
        throws IOException {
      return MembershipProtocol.decodeResponse(_catalog.execute(MetadataProtocol.encode(
          new MembershipCommand(_history, "controller", operation)), 1, ++_index));
    }

    private Result controller(LongFunction<Command> command)
        throws IOException {
      if (_sequence > 0) {
        assertEquals(apply(new Acknowledge(_history, "controller", 1, "coordination", _sequence)).status(), Status.OK);
      }
      return apply(command.apply(++_sequence));
    }

    private Result owner(OwnershipProtocol.Operation operation)
        throws IOException {
      return controller(sequence -> new OwnershipCommand(_history, "controller", 1, "coordination", sequence,
          operation));
    }

    private Result segment(OfflineSegmentProtocol.Operation operation)
        throws IOException {
      return controller(sequence -> new OfflineSegmentCommand(_history, "controller", 1, "coordination", sequence,
          operation));
    }

    private Result operation(OperationProtocol.Operation operation)
        throws IOException {
      return controller(sequence -> new NativeOperationCommand(_history, "controller", 1, "coordination", sequence,
          operation));
    }

    private Result complete()
        throws IOException {
      OperationState state = state();
      return operation(new CompleteInstalled(_operation, record(RecordKind.CONTROLLER_OPERATION,
          OperationProtocol.recordName(_operation)).version(), _owner, state.attempt()));
    }

    private InstallationSessionCommand reportCommand(ParticipantToken server, ReportInstallation report)
        throws IOException {
      long previous = _serverSequences.getOrDefault(server.participantId(), 0L);
      if (previous > 0) {
        assertEquals(apply(new InstallationSessionCommand(server,
            new Acknowledge(_history, OfflineInstallationProtocol.clientId(server), 1, "installation", previous)))
            .status(), Status.OK);
      }
      _serverSequences.put(server.participantId(), previous + 1);
      return new InstallationSessionCommand(server, new NativeInstallationCommand(_history,
          OfflineInstallationProtocol.clientId(server), 1, "installation", previous + 1, report));
    }

    private Result report(ParticipantToken server, ReportInstallation report)
        throws IOException {
      _lastReport = reportCommand(server, report);
      _lastReportResult = apply(_lastReport);
      return _lastReportResult;
    }

    private WorkResult work(ParticipantToken server)
        throws IOException {
      return OfflineInstallationProtocol.decodeWorkResult(_catalog.query(MetadataProtocol.encode(
          new GetInstallationWork(_history, server, _operation))));
    }

    private CatalogRecord record(RecordKind kind, String name)
        throws IOException {
      return MetadataProtocol.decodeResult(_catalog.query(MetadataProtocol.encode(new GetRecord(_history, kind, name))))
          .record();
    }

    private OperationState state()
        throws IOException {
      return OperationProtocol.decodeOperation(record(RecordKind.CONTROLLER_OPERATION,
          OperationProtocol.recordName(_operation)).value());
    }

    private ObjectVersion version(CatalogRecord record) {
      return new ObjectVersion(_history, record.objectId(), record.version());
    }

    private Result apply(Command command)
        throws IOException {
      return MetadataProtocol.decodeResult(_catalog.execute(MetadataProtocol.encode(command), 1, ++_index));
    }
  }
}
