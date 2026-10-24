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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.pinot.common.config.provider.TableDataManagerMetadata.SegmentSnapshot;
import org.apache.pinot.coordination.ratis.client.MetadataImage;
import org.apache.pinot.coordination.ratis.client.RatisOfflineTableMetadata;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ActivateControlEpoch;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.BeginDrain;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Capability;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ConfigurePolicy;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ControlToken;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ParticipantToken;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Policy;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Register;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Response;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Role;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.SetReady;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Tick;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Acknowledge;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.CatalogRecord;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.ChangePage;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Command;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.FeedCursor;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.FeedScope;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.GetOutcome;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.GetRecord;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.MembershipCommand;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Mutation;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.NativeOperationCommand;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.ObjectVersion;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.OfflineSegmentCommand;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.OperationCommand;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.OwnershipCommand;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.RecordKind;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.RegisterClient;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Result;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Revision;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Status;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Transaction;
import org.apache.pinot.coordination.ratis.metadata.OfflineSegmentProtocol.AssignReplica;
import org.apache.pinot.coordination.ratis.metadata.OfflineSegmentProtocol.Compatibility;
import org.apache.pinot.coordination.ratis.metadata.OfflineSegmentProtocol.Content;
import org.apache.pinot.coordination.ratis.metadata.OfflineSegmentProtocol.LoaderDescriptor;
import org.apache.pinot.coordination.ratis.metadata.OfflineSegmentProtocol.PublishContent;
import org.apache.pinot.coordination.ratis.metadata.OperationProtocol.AssignedOfflineSegmentReload;
import org.apache.pinot.coordination.ratis.metadata.OperationProtocol.Cancel;
import org.apache.pinot.coordination.ratis.metadata.OperationProtocol.Checkpoint;
import org.apache.pinot.coordination.ratis.metadata.OperationProtocol.Claim;
import org.apache.pinot.coordination.ratis.metadata.OperationProtocol.Complete;
import org.apache.pinot.coordination.ratis.metadata.OperationProtocol.OfflineSegmentReload;
import org.apache.pinot.coordination.ratis.metadata.OperationProtocol.OperationState;
import org.apache.pinot.coordination.ratis.metadata.OperationProtocol.Progress;
import org.apache.pinot.coordination.ratis.metadata.OperationProtocol.State;
import org.apache.pinot.coordination.ratis.metadata.OperationProtocol.Submit;
import org.apache.pinot.coordination.ratis.metadata.OperationProtocol.TargetMaterialization;
import org.apache.pinot.coordination.ratis.metadata.OwnershipProtocol.AssignOwner;
import org.apache.pinot.coordination.ratis.metadata.OwnershipProtocol.Assignment;
import org.apache.pinot.coordination.ratis.metadata.OwnershipProtocol.ConfigureLayout;
import org.apache.pinot.coordination.ratis.metadata.OwnershipProtocol.HashVersion;
import org.apache.pinot.coordination.ratis.metadata.OwnershipProtocol.Layout;
import org.apache.pinot.coordination.ratis.metadata.OwnershipProtocol.OwnerToken;
import org.apache.pinot.coordination.ratis.metadata.OwnershipProtocol.TableShard;
import org.apache.pinot.spi.config.table.TableConfig;
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


/// Exercises ownership and operation ledgers through deterministic committed commands, without claiming physical
/// segment reload completion. Publication, retry outcomes and snapshots share the catalog's transaction boundary.
public class ControllerCoordinationTest {
  private static final String TABLE = "analytics.orders_OFFLINE";
  private static final String SCHEMA = "analytics.orders";
  private static final FeedScope ALL = new FeedScope(Set.of(RecordKind.values()), "controller-test");
  private static final Layout LAYOUT = new Layout(1, 24, HashVersion.MURMUR2_UTF8_MASK_POSITIVE_V1);

  @Test
  public void testCoordinatorAndCompleteParticipantFences()
      throws Exception {
    Fixture collision = new Fixture(false);
    UUID layoutId = UUID.nameUUIDFromBytes((collision._history + "\0" + RecordKind.OWNER_LAYOUT + "\0"
        + OwnershipProtocol.LAYOUT_NAME).getBytes(StandardCharsets.UTF_8));
    assertEquals(collision.transaction(Mutation.createClusterConfig(collision._history, layoutId,
        Map.of("reserved", "existing"))).status(), Status.OK);
    CatalogRecord reserved = collision.record(RecordKind.CLUSTER_CONFIG, MetadataProtocol.CLUSTER_CONFIG_NAME);
    assertEquals(collision.owner(new ConfigureLayout(collision.control(), LAYOUT)).status(), Status.CONFLICT);
    assertEquals(collision.record(RecordKind.CLUSTER_CONFIG, MetadataProtocol.CLUSTER_CONFIG_NAME), reserved);
    assertEquals(MetadataProtocol.decodeResult(collision._catalog.query(MetadataProtocol.encode(new GetRecord(
        collision._history, RecordKind.OWNER_LAYOUT, OwnershipProtocol.LAYOUT_NAME)))).status(), Status.NOT_FOUND);
    assertEquals(collision.transaction(Mutation.delete(RecordKind.CLUSTER_CONFIG, MetadataProtocol.CLUSTER_CONFIG_NAME,
        collision.version(reserved))).status(), Status.OK);
    JsonNode tombstones = collision._catalog.captureSnapshot().path("records");
    assertEquals(collision.owner(new ConfigureLayout(collision.control(), LAYOUT)).status(), Status.CONFLICT);
    assertEquals(collision._catalog.captureSnapshot().path("records"), tombstones);

    Fixture fixture = new Fixture();
    TableShard scope = fixture.scope();
    assertEquals(scope, OwnershipProtocol.tableScope(LAYOUT, "analytics.orders_REALTIME"));
    assertEquals(scope, OwnershipProtocol.tableScope(LAYOUT, "analytics.orders"));
    assertNotEquals(scope, OwnershipProtocol.tableScope(LAYOUT, "orders_OFFLINE"),
        "Database qualification is part of the ownership key");
    ControlToken oldControl = fixture.control();
    fixture._term = 2;
    assertEquals(fixture.owner(new AssignOwner(oldControl, 1, scope, 0, fixture._first)).status(), Status.FENCED);
    assertEquals(fixture.member(new ActivateControlEpoch(2, "controller-b", UUID.randomUUID())).status(), Status.OK);
    assertEquals(fixture.owner(new AssignOwner(oldControl, 1, scope, 0, fixture._first)).status(), Status.FENCED);

    ParticipantToken first = fixture._first;
    List<ParticipantToken> invalid = List.of(
        new ParticipantToken(fixture._history, first.participantId(), Role.SERVER, first.incarnation(), 1),
        new ParticipantToken(fixture._history, first.participantId(), Role.CONTROLLER, UUID.randomUUID(), 1),
        new ParticipantToken(fixture._history, first.participantId(), Role.CONTROLLER, first.incarnation(), 2));
    for (ParticipantToken token : invalid) {
      assertEquals(fixture.owner(new AssignOwner(fixture.control(), 1, scope, 0, token)).status(), Status.FENCED);
    }
    ParticipantToken wrongHistory = new ParticipantToken(UUID.randomUUID(), first.participantId(), Role.CONTROLLER,
        first.incarnation(), first.registrationEpoch());
    assertEquals(fixture.owner(new AssignOwner(fixture.control(), 1, scope, 0, wrongHistory)).status(),
        Status.HISTORY_MISMATCH);
    assertEquals(fixture.member(new SetReady(first, fixture.revision(), false)).status(), Status.OK);
    assertEquals(fixture.owner(new AssignOwner(fixture.control(), 1, scope, 0, first)).status(), Status.FENCED);
    assertEquals(fixture.member(new SetReady(first, fixture.revision(), true)).status(), Status.OK);
    assertEquals(fixture.owner(new AssignOwner(fixture.control(), 1, scope, 0, first)).status(), Status.OK);
    assertEquals(fixture.assignment().ownerEpoch(), 1L);
    assertEquals(fixture.owner(new AssignOwner(fixture.control(), 1, scope, 0, first)).status(), Status.CONFLICT);
    assertEquals(fixture.member(new BeginDrain(fixture._second)).status(), Status.OK);
    assertEquals(fixture.owner(new AssignOwner(fixture.control(), 1, scope, 1, fixture._second)).status(),
        Status.FENCED);
    for (int round = 0; round < 3; round++) {
      assertEquals(fixture.member(new Tick(fixture.control(), round, 1)).status(), Status.OK);
    }
    assertEquals(fixture.owner(new AssignOwner(fixture.control(), 1, scope, 1, first)).status(), Status.FENCED);
    ParticipantToken replacement = fixture.register(first.participantId(), Role.CONTROLLER, 1);
    assertEquals(replacement.registrationEpoch(), 2L);
    assertNotEquals(replacement.incarnation(), first.incarnation());
    assertEquals(fixture.owner(new AssignOwner(fixture.control(), 1, scope, 1, first)).status(), Status.FENCED);
    assertEquals(fixture.owner(new AssignOwner(fixture.control(), 1, scope, 1, replacement)).status(), Status.OK);
    assertEquals(fixture.assignment().owner(), replacement);
    assertEquals(fixture.assignment().ownerEpoch(), 2L);
  }

  @Test
  public void testAttemptFenceAndAtomicTerminalPublication()
      throws Exception {
    Fixture fixture = new Fixture();
    OwnerToken owner = fixture.assign(fixture._first, 0);
    UUID id = UUID.randomUUID();
    assertEquals(fixture.operation(new Submit(id, fixture.input())).status(), Status.OK);
    assertEquals(fixture.claim(id, owner).status(), Status.OK);
    long oldAttempt = fixture.operationState(id).attempt();
    assertEquals(fixture.operation(new Checkpoint(id, fixture.operationVersion(id), owner, oldAttempt,
        new Progress(0, 0, "started"))).status(), Status.OK);
    assertEquals(fixture.claim(id, owner).status(), Status.OK);
    long attempt = fixture.operationState(id).attempt();
    assertEquals(attempt, oldAttempt + 1);
    assertEquals(fixture.operation(new Checkpoint(id, fixture.operationVersion(id), owner, oldAttempt,
        new Progress(1, 0, "late attempt"))).status(), Status.FENCED);
    assertEquals(fixture.operation(new Complete(id, fixture.operationVersion(id), owner, oldAttempt, State.SUCCEEDED,
        new Progress(1, 0, "late attempt"), List.of())).status(), Status.FENCED);

    CatalogRecord beforeTable = fixture.record(RecordKind.TABLE_CONFIG, TABLE);
    CatalogRecord beforeOperation = fixture.operationRecord(id);
    Mutation effect = Mutation.updateTableConfig(fixture.version(beforeTable), fixture.table(2));
    Mutation staleEffect = Mutation.updateTableConfig(new ObjectVersion(fixture._history, fixture._tableId, 99),
        fixture.table(2));
    OperationCommand failed = fixture.command(new Complete(id, beforeOperation.version(), owner, attempt,
        State.SUCCEEDED, new Progress(1, 0, "done"), List.of(effect, staleEffect)));
    FeedCursor failedCursor = fixture._catalog.captureImage(ALL).cursor();
    assertEquals(fixture.apply(failed).status(), Status.CONFLICT);
    assertEquals(fixture.record(RecordKind.TABLE_CONFIG, TABLE), beforeTable);
    assertEquals(fixture.operationRecord(id), beforeOperation);
    assertTrue(fixture.changes(failedCursor).transactions().getFirst().records().isEmpty());

    OperationCommand complete = fixture.command(new Complete(id, beforeOperation.version(), owner, attempt,
        State.SUCCEEDED, new Progress(1, 0, "done"), List.of(effect)));
    FeedCursor cursor = fixture._catalog.captureImage(ALL).cursor();
    Result outcome = fixture.apply(complete);
    assertEquals(outcome.status(), Status.OK);
    assertEquals(fixture.operationState(id).status(), State.SUCCEEDED);
    assertEquals(fixture.record(RecordKind.TABLE_CONFIG, TABLE).value().at("/segmentsConfig/replication").asText(),
        "2");
    ChangePage publication = fixture.changes(cursor);
    assertEquals(publication.transactions().size(), 1);
    assertEquals(publication.transactions().getFirst().index(), outcome.revision().index());
    assertEquals(publication.transactions().getFirst().records().stream().map(CatalogRecord::kind)
        .collect(Collectors.toSet()), Set.of(RecordKind.TABLE_CONFIG, RecordKind.CONTROLLER_OPERATION));
    assertEquals(publication.transactions().getFirst().records().size(), 2);
    assertEquals(fixture.apply(complete), outcome);
    assertEquals(fixture.record(RecordKind.TABLE_CONFIG, TABLE).version(), beforeTable.version() + 1);
    assertEquals(fixture.operation(new Submit(id, fixture.input())).status(), Status.CONFLICT);
  }

  @Test
  public void testTransferDependencyAndDeleteRecreateFencesAllowCancellation()
      throws Exception {
    Fixture fixture = new Fixture();
    OwnerToken first = fixture.assign(fixture._first, 0);
    UUID id = UUID.randomUUID();
    assertEquals(fixture.operation(new Submit(id, fixture.input())).status(), Status.OK);
    assertEquals(fixture.claim(id, first).status(), Status.OK);
    long attempt = fixture.operationState(id).attempt();
    CatalogRecord schema = fixture.record(RecordKind.SCHEMA, SCHEMA);
    assertEquals(fixture.transaction(Mutation.updateSchema(fixture.version(schema), fixture.schema())).status(),
        Status.OK);
    assertEquals(fixture.operation(new Checkpoint(id, fixture.operationVersion(id), first, attempt,
        new Progress(0, 0, "stale dependency"))).status(), Status.CONFLICT);
    OwnerToken second = fixture.assign(fixture._second, 1);
    assertEquals(fixture.operation(new Complete(id, fixture.operationVersion(id), first, attempt, State.SUCCEEDED,
        new Progress(1, 0, "former owner"), List.of())).status(), Status.FENCED);
    assertEquals(fixture.claim(id, second).status(), Status.OK);
    assertTrue(fixture.operationState(id).attempt() > attempt);
    long nextAttempt = fixture.operationState(id).attempt();
    CatalogRecord table = fixture.record(RecordKind.TABLE_CONFIG, TABLE);
    assertEquals(fixture.transaction(Mutation.delete(RecordKind.TABLE_CONFIG, TABLE, fixture.version(table))).status(),
        Status.OK);
    assertEquals(fixture.transaction(Mutation.createTableConfig(fixture._history, UUID.randomUUID(), fixture.table(1)))
        .status(), Status.OK);
    assertEquals(fixture.operation(new Complete(id, fixture.operationVersion(id), second, nextAttempt, State.SUCCEEDED,
        new Progress(1, 0, "old incarnation"), List.of())).status(), Status.FENCED);
    assertEquals(fixture.operation(new Cancel(id, fixture.operationVersion(id), first, "former owner")).status(),
        Status.FENCED);
    assertEquals(fixture.operation(new Cancel(id, fixture.operationVersion(id), second, "table was replaced")).status(),
        Status.OK);
    assertEquals(fixture.operationState(id).status(), State.CANCELLED);
    assertEquals(fixture.operationState(id).attempt(), nextAttempt + 1);
    assertEquals(fixture.record(RecordKind.TABLE_CONFIG, TABLE).version(), 1L);
  }

  @Test
  public void testImmutableOfflineContentAndAssignmentFences()
      throws Exception {
    Fixture fixture = new Fixture();
    OwnerToken owner = fixture.assign(fixture._first, 0);
    Content content = fixture.content();
    ObjectVersion table = fixture.version(fixture.record(RecordKind.TABLE_CONFIG, TABLE));
    assertEquals(fixture.segment(new PublishContent(owner, table, content)).status(), Status.OK);
    assertEquals(fixture.segment(new PublishContent(owner, table, content)).status(), Status.CONFLICT);
    Compatibility initial = fixture.compatibility();
    assertEquals(fixture.segment(new AssignReplica(owner, content.segmentObjectId(), 0, fixture._server, initial, true))
        .status(), Status.OK);
    OfflineSegmentProtocol.Assignment first = fixture.replica(content);
    assertEquals(first.assignmentEpoch(), 1L);
    assertTrue(first.desiredOnline());
    assertEquals(first.materializationId(), OfflineSegmentProtocol.materializationId(fixture._history,
        OfflineSegmentProtocol.assignmentObjectId(fixture._history, content.segmentObjectId(),
            fixture._server.participantId()), 1));
    AtomicReference<MetadataImage> image = new AtomicReference<>(MetadataImage.fromSnapshot(ALL,
        fixture._catalog.captureImage(ALL)));
    RatisOfflineTableMetadata metadata = new RatisOfflineTableMetadata(image::get, fixture._server, fixture._tableId);
    SegmentSnapshot loading = metadata.fetchSegment(TABLE, content.segmentName());
    assertEquals(loading.segment().downloadUri(), content.downloadUri());
    assertEquals(loading.segment().crc(), content.loader().crc());
    assertEquals(loading.tableConfig().getValidationConfig().getReplication(), "1");
    assertEquals(loading.schema().getSchemaName(), SCHEMA);

    assertEquals(fixture.transaction(Mutation.updateTableConfig(table, fixture.table(2))).status(), Status.OK);
    image.set(MetadataImage.fromSnapshot(ALL, fixture._catalog.captureImage(ALL)));
    assertThrows(IllegalStateException.class, () -> metadata.fetchSegment(TABLE, content.segmentName()));
    assertEquals(fixture.segment(new AssignReplica(owner, content.segmentObjectId(), 1, fixture._server, initial, true))
        .status(), Status.CONFLICT);
    Compatibility current = fixture.compatibility();
    assertEquals(fixture.segment(new AssignReplica(owner, content.segmentObjectId(), 1, fixture._server, current, true))
        .status(), Status.OK);
    OfflineSegmentProtocol.Assignment next = fixture.replica(content);
    assertEquals(next.assignmentEpoch(), 2L);
    assertNotEquals(next.materializationId(), first.materializationId());
    image.set(MetadataImage.fromSnapshot(ALL, fixture._catalog.captureImage(ALL)));
    assertEquals(metadata.fetchSegment(TABLE, content.segmentName()).tableConfig().getValidationConfig()
        .getReplication(), "2");
    // Mutating a caller's decoded configuration cannot change the published metadata image.
    loading.tableConfig().getValidationConfig().setReplication("99");
    assertEquals(metadata.fetchTable(TABLE).tableConfig().getValidationConfig().getReplication(), "2");
    assertEquals(fixture.segment(new AssignReplica(owner, content.segmentObjectId(), 1, fixture._server, current, true))
        .status(), Status.CONFLICT);

    assertEquals(fixture.member(new BeginDrain(fixture._server)).status(), Status.OK);
    image.set(MetadataImage.fromSnapshot(ALL, fixture._catalog.captureImage(ALL)));
    assertThrows(IllegalStateException.class, () -> metadata.fetchSegment(TABLE, content.segmentName()));
    assertEquals(fixture.segment(new AssignReplica(owner, content.segmentObjectId(), 2, fixture._server, current, true))
        .status(), Status.FENCED);
    assertEquals(fixture.transaction(Mutation.delete(RecordKind.TABLE_CONFIG, TABLE, current.table())).status(),
        Status.OK);
    assertEquals(fixture.transaction(Mutation.createTableConfig(fixture._history, UUID.randomUUID(), fixture.table(1)))
        .status(), Status.OK);
    OwnerToken successor = fixture.assign(fixture._second, 1);
    assertEquals(fixture.segment(new AssignReplica(owner, content.segmentObjectId(), 2, fixture._server, current,
        false)).status(), Status.FENCED);
    assertEquals(fixture.segment(new AssignReplica(successor, content.segmentObjectId(), 2, fixture._server, current,
        false)).status(), Status.OK);
    assertFalse(fixture.replica(content).desiredOnline());
    assertEquals(fixture.replica(content).assignmentEpoch(), 3L);
    MetadataCatalog recovered = new MetadataCatalog(fixture._cluster, fixture._history);
    recovered.restoreSnapshot(fixture._catalog.captureSnapshot());
    assertEquals(recovered.captureSnapshot(), fixture._catalog.captureSnapshot());
  }

  @Test
  public void testNativeReloadRequiresAuthoritativeAssignment()
      throws Exception {
    Fixture fixture = new Fixture();
    OwnerToken owner = fixture.assign(fixture._first, 0);
    assertEquals(fixture.nativeOperation(new Submit(UUID.randomUUID(), fixture.input())).status(), Status.NOT_FOUND);
    Content content = fixture.content();
    assertEquals(fixture.segment(new PublishContent(owner, fixture.compatibility().table(), content)).status(),
        Status.OK);
    assertEquals(fixture.segment(new AssignReplica(owner, content.segmentObjectId(), 0, fixture._server,
        fixture.compatibility(), true)).status(), Status.OK);
    OfflineSegmentProtocol.Assignment replica = fixture.replica(content);
    UUID id = UUID.randomUUID();
    OfflineSegmentReload input = new OfflineSegmentReload(TABLE, fixture._tableId, content.segmentName(),
        content.segmentObjectId(), content.contentSha256(), List.of(new TargetMaterialization(fixture._server,
        replica.assignmentEpoch(), replica.materializationId())), true);
    assertEquals(fixture.nativeOperation(new Submit(id, input)).status(), Status.OK);
    AssignedOfflineSegmentReload assigned = (AssignedOfflineSegmentReload) fixture.operationState(id).input();
    assertEquals(assigned.dependencies().size(), 4);
    assertEquals(fixture.nativeOperation(new Claim(id, 1, owner, assigned.dependencies())).status(), Status.OK);
    // A controller's counters cannot certify physical installation without committed target observations.
    assertEquals(fixture.nativeOperation(new Complete(id, 2, owner, 1, State.SUCCEEDED,
        new Progress(1, 0, "unverified"), List.of())).status(), Status.INVALID_REQUEST);
    assertEquals(fixture.operationState(id).status(), State.RUNNING);
    assertEquals(fixture.segment(new AssignReplica(owner, content.segmentObjectId(), 1, fixture._server,
        fixture.compatibility(), true)).status(), Status.OK);
    assertEquals(fixture.nativeOperation(new Checkpoint(id, 2, owner, 1, new Progress(0, 0, "stale assignment")))
        .status(), Status.FENCED);
    assertEquals(fixture.nativeOperation(new Cancel(id, 2, owner, "replan current assignment")).status(), Status.OK);

    UUID legacyId = UUID.randomUUID();
    assertEquals(fixture.operation(new Submit(legacyId, fixture.input())).status(), Status.OK);
    assertEquals(fixture.nativeOperation(new Claim(legacyId, 1, owner,
        List.of(fixture.compatibility().table()))).status(), Status.INVALID_REQUEST);
    MetadataCatalog restored = new MetadataCatalog(fixture._cluster, fixture._history);
    restored.restoreSnapshot(fixture._catalog.captureSnapshot());
    assertEquals(restored.captureSnapshot(), fixture._catalog.captureSnapshot());
  }

  @Test
  public void testOfflineSnapshotReferencesAndLegacyMembershipRejection()
      throws Exception {
    Fixture fixture = new Fixture();
    OwnerToken owner = fixture.assign(fixture._first, 0);
    ObjectNode legacy = fixture._catalog.captureSnapshot().deepCopy();
    legacy.put("formatVersion", 4);
    MetadataCatalog recovered = new MetadataCatalog(fixture._cluster, fixture._history);
    assertThrows(IOException.class, () -> recovered.restoreSnapshot(legacy));
    Content content = fixture.content();
    assertEquals(fixture.segment(new PublishContent(owner, fixture.compatibility().table(), content)).status(),
        Status.OK);
    assertEquals(fixture.segment(new AssignReplica(owner, content.segmentObjectId(), 0, fixture._server,
        fixture.compatibility(), true)).status(), Status.OK);
    JsonNode valid = fixture._catalog.captureSnapshot();
    recovered.restoreSnapshot(valid);
    ObjectNode corrupt = valid.deepCopy();
    rewriteRecord(corrupt, RecordKind.OFFLINE_REPLICA_ASSIGNMENT,
        value -> ((ObjectNode) value.at("/compatibility/schema")).put("version", 999));
    assertThrows(IOException.class, () -> recovered.restoreSnapshot(corrupt));
    assertEquals(recovered.captureSnapshot(), valid);
    ObjectNode oldFormat = valid.deepCopy();
    oldFormat.put("formatVersion", 4);
    assertThrows(IOException.class, () -> recovered.restoreSnapshot(oldFormat));
    assertEquals(recovered.captureSnapshot(), valid);
  }

  @Test
  public void testDurableOutcomeAndSnapshotReferenceValidation()
      throws Exception {
    Fixture fixture = new Fixture();
    OwnerToken owner = fixture.assign(fixture._first, 0);
    UUID id = UUID.randomUUID();
    OperationCommand submit = fixture.command(new Submit(id, fixture.input()));
    byte[] request = MetadataProtocol.encode(submit);
    assertFalse(new String(request, StandardCharsets.UTF_8).contains("forceDownload"),
        "Default false must preserve the original serialized intent and durable request digest");
    assertEquals(MetadataProtocol.encode(MetadataProtocol.decodeCommand(request)), request);
    OfflineSegmentReload input = fixture.input();
    OfflineSegmentReload forced = new OfflineSegmentReload(input.tableName(), input.tableObjectId(),
        input.segmentName(), input.segmentObjectId(), input.contentSha256(), input.targets(), true);
    assertEquals(MetadataProtocol.fromTree(MetadataProtocol.toTree(forced), OfflineSegmentReload.class), forced);
    ObjectNode malformed = MetadataProtocol.toTree(forced).deepCopy();
    malformed.remove("segmentName");
    assertThrows(IOException.class, () -> MetadataProtocol.fromTree(malformed, OfflineSegmentReload.class));
    byte[] original = fixture._catalog.execute(request, fixture._term, ++fixture._index);
    assertEquals(MetadataProtocol.decodeResult(original).status(), Status.OK);
    JsonNode snapshot = fixture._catalog.captureSnapshot();
    MetadataCatalog restored = new MetadataCatalog(fixture._cluster, fixture._history);
    restored.restoreSnapshot(snapshot);
    assertEquals(restored.captureSnapshot(), snapshot);
    assertEquals(restored.execute(request, fixture._term, fixture._index + 1), original);
    assertEquals(restored.query(MetadataProtocol.encode(new GetOutcome(fixture._history, "controller", 1,
        "coordination", submit.sequence()))), original);
    assertEquals(restored.captureImage(ALL).records(), fixture._catalog.captureImage(ALL).records());

    assertEquals(fixture.claim(id, owner).status(), Status.OK);
    long supersededVersion = fixture.operationVersion(id);
    assertEquals(fixture.operation(new Checkpoint(id, supersededVersion, owner, fixture.operationState(id).attempt(),
        new Progress(0, 0, "in progress"))).status(), Status.OK);
    restored.restoreSnapshot(fixture._catalog.captureSnapshot());
    JsonNode before = restored.captureSnapshot();
    ObjectNode absentOwner = before.deepCopy();
    rewriteRecord(absentOwner, RecordKind.OWNER_ASSIGNMENT,
        value -> ((ObjectNode) value.path("owner")).put("participantId", "missing-controller"));
    assertThrows(IOException.class, () -> restored.restoreSnapshot(absentOwner));
    assertEquals(restored.captureSnapshot(), before);
    ObjectNode futureOwner = before.deepCopy();
    rewriteRecord(futureOwner, RecordKind.OWNER_ASSIGNMENT,
        value -> ((ObjectNode) value.path("owner")).put("registrationEpoch", 999));
    assertThrows(IOException.class, () -> restored.restoreSnapshot(futureOwner));
    assertEquals(restored.captureSnapshot(), before);
    ObjectNode missingDependency = before.deepCopy();
    String absentObjectId = UUID.randomUUID().toString();
    rewriteRecord(missingDependency, RecordKind.CONTROLLER_OPERATION, value -> {
      if (value.path("dependencies").size() > 1) {
        ((ObjectNode) value.path("dependencies").get(1)).put("objectId", absentObjectId);
      }
    });
    assertThrows(IOException.class, () -> restored.restoreSnapshot(missingDependency));
    assertEquals(restored.captureSnapshot(), before);

    ObjectNode corruptHistory = before.deepCopy();
    boolean changed = false;
    for (JsonNode transaction : corruptHistory.path("journal")) {
      for (JsonNode record : transaction.path("records")) {
        if (record.path("kind").asText().equals(RecordKind.CONTROLLER_OPERATION.name())
            && record.path("version").asLong() == supersededVersion) {
          ((ObjectNode) record.at("/value/dependencies/1")).put("objectId", absentObjectId);
          changed = true;
        }
      }
    }
    assertTrue(changed, "The corruption must affect a superseded running-operation journal entry");
    assertEquals(corruptHistory.path("records"), before.path("records"));
    assertThrows(IOException.class, () -> restored.restoreSnapshot(corruptHistory));
    assertEquals(restored.captureSnapshot(), before);

    ObjectNode invalidLegacy = before.deepCopy();
    invalidLegacy.put("formatVersion", 3);
    assertThrows(IOException.class, () -> restored.restoreSnapshot(invalidLegacy));
    Fixture legacy = new Fixture(false);
    ObjectNode legacyMembership = legacy._catalog.captureSnapshot().deepCopy();
    legacyMembership.put("formatVersion", 3);
    MetadataCatalog migrated = new MetadataCatalog(legacy._cluster, legacy._history);
    assertThrows(IOException.class, () -> migrated.restoreSnapshot(legacyMembership));
  }

  /// Keep the journal's latest images consistent, so recovery must reject the invalid cross-record reference itself.
  private static void rewriteRecord(ObjectNode snapshot, RecordKind kind, Consumer<ObjectNode> change) {
    for (JsonNode records : snapshot.findValues("records")) {
      for (JsonNode record : records) {
        if (record.path("kind").asText().equals(kind.name())) {
          change.accept((ObjectNode) record.path("value"));
        }
      }
    }
  }

  private static final class Fixture {
    private final UUID _cluster = UUID.randomUUID();
    private final UUID _history = UUID.randomUUID();
    private final UUID _tableId = UUID.randomUUID();
    private final MetadataCatalog _catalog = new MetadataCatalog(_cluster, _history);
    private final ParticipantToken _first;
    private final ParticipantToken _second;
    private final ParticipantToken _server;
    private long _index;
    private long _term = 1;
    private long _sequence;

    private Fixture()
        throws Exception {
      this(true);
    }

    private Fixture(boolean configureLayout)
        throws Exception {
      assertEquals(apply(new RegisterClient(_history, "controller", UUID.randomUUID(), 0)).status(), Status.OK);
      assertEquals(member(new ActivateControlEpoch(1, "controller-a", UUID.randomUUID())).status(), Status.OK);
      assertEquals(member(new ConfigurePolicy(control(), 0, new Policy(1, 100, 3, 200, 300, 1000))).status(),
          Status.OK);
      _first = register("controller-a", Role.CONTROLLER, 0);
      _second = register("controller-b", Role.CONTROLLER, 0);
      _server = register("server-a", Role.SERVER, 0);
      if (configureLayout) {
        assertEquals(owner(new ConfigureLayout(control(), LAYOUT)).status(), Status.OK);
      }
      assertEquals(transaction(Mutation.createTableConfig(_history, _tableId, table(1)),
          Mutation.createSchema(_history, UUID.randomUUID(), schema())).status(), Status.OK);
    }

    private ParticipantToken register(String id, Role role, long expectedEpoch)
        throws Exception {
      Response response = member(new Register(id, role, UUID.randomUUID(), expectedEpoch, Map.of(),
          role == Role.SERVER ? Set.of(Capability.SERVER_QUERY) : Set.of()));
      assertEquals(response.status(), Status.OK);
      ParticipantToken participant = response.participant().participant();
      assertEquals(member(new SetReady(participant, revision(), true)).status(), Status.OK);
      return participant;
    }

    private Response member(MembershipProtocol.Operation request)
        throws IOException {
      return MembershipProtocol.decodeResponse(_catalog.execute(MetadataProtocol.encode(
          new MembershipCommand(_history, "controller", request)), _term, ++_index));
    }

    private Result owner(OwnershipProtocol.Operation request)
        throws IOException {
      return apply(new OwnershipCommand(_history, "controller", 1, "coordination", nextSequence(), request));
    }

    private Result operation(OperationProtocol.Operation request)
        throws IOException {
      return apply(command(request));
    }

    private Result segment(OfflineSegmentProtocol.Operation request)
        throws IOException {
      return apply(new OfflineSegmentCommand(_history, "controller", 1, "coordination", nextSequence(), request));
    }

    private Result nativeOperation(OperationProtocol.Operation request)
        throws IOException {
      return apply(new NativeOperationCommand(_history, "controller", 1, "coordination", nextSequence(), request));
    }

    private Content content() {
      return new Content(TABLE, _tableId, "segment-0", UUID.randomUUID(), "b".repeat(64),
          "file:///immutable/segment-0.tar.gz", new LoaderDescriptor(123, -1, false, null, null, 0, 0, Map.of()));
    }

    private Compatibility compatibility()
        throws IOException {
      return new Compatibility(version(record(RecordKind.TABLE_CONFIG, TABLE)),
          version(record(RecordKind.SCHEMA, SCHEMA)));
    }

    private OfflineSegmentProtocol.Assignment replica(Content content)
        throws IOException {
      return OfflineSegmentProtocol.decodeAssignment(record(RecordKind.OFFLINE_REPLICA_ASSIGNMENT,
          OfflineSegmentProtocol.assignmentName(content.segmentObjectId(), _server.participantId())).value());
    }

    private OperationCommand command(OperationProtocol.Operation request)
        throws IOException {
      return new OperationCommand(_history, "controller", 1, "coordination", nextSequence(), request);
    }

    private long nextSequence()
        throws IOException {
      if (_sequence > 0) {
        assertEquals(apply(new Acknowledge(_history, "controller", 1, "coordination", _sequence)).status(), Status.OK);
      }
      return ++_sequence;
    }

    private Result apply(Command command)
        throws IOException {
      return MetadataProtocol.decodeResult(_catalog.execute(MetadataProtocol.encode(command), _term, ++_index));
    }

    private Result transaction(Mutation... mutations)
        throws IOException {
      return apply(new Transaction(_history, "controller", 1, "coordination", nextSequence(), List.of(mutations)));
    }

    private OwnerToken assign(ParticipantToken participant, long expectedEpoch)
        throws IOException {
      assertEquals(owner(new AssignOwner(control(), 1, scope(), expectedEpoch, participant)).status(), Status.OK);
      return new OwnerToken(_history, 1, scope(), assignment().ownerEpoch(), participant);
    }

    private Assignment assignment()
        throws IOException {
      return OwnershipProtocol.decodeAssignment(record(RecordKind.OWNER_ASSIGNMENT, OwnershipProtocol.name(scope()))
          .value());
    }

    private Result claim(UUID id, OwnerToken owner)
        throws IOException {
      return operation(new Claim(id, operationVersion(id), owner,
          List.of(version(record(RecordKind.TABLE_CONFIG, TABLE)), version(record(RecordKind.SCHEMA, SCHEMA)))));
    }

    private CatalogRecord record(RecordKind kind, String name)
        throws IOException {
      Result result = MetadataProtocol.decodeResult(_catalog.query(MetadataProtocol.encode(new GetRecord(_history,
          kind, name))));
      assertEquals(result.status(), Status.OK);
      return result.record();
    }

    private CatalogRecord operationRecord(UUID id)
        throws IOException {
      return record(RecordKind.CONTROLLER_OPERATION, OperationProtocol.recordName(id));
    }

    private long operationVersion(UUID id)
        throws IOException {
      return operationRecord(id).version();
    }

    private OperationState operationState(UUID id)
        throws IOException {
      return OperationProtocol.decodeOperation(operationRecord(id).value());
    }

    private ObjectVersion version(CatalogRecord record) {
      return new ObjectVersion(_history, record.objectId(), record.version());
    }

    private OfflineSegmentReload input() {
      return new OfflineSegmentReload(TABLE, _tableId, "segment-0", UUID.randomUUID(), "a".repeat(64),
          List.of(new TargetMaterialization(_server, 1, UUID.randomUUID())));
    }

    private ChangePage changes(FeedCursor cursor)
        throws IOException {
      return _catalog.changes(ALL, cursor, 128, 128, MetadataProtocol.MAX_CHANGE_BYTES);
    }

    private TableConfig table(int replicas) {
      return new TableConfigBuilder(TableType.OFFLINE).setTableName("analytics.orders").setNumReplicas(replicas)
          .build();
    }

    private Schema schema() {
      return new Schema.SchemaBuilder().setSchemaName(SCHEMA).addSingleValueDimension("id", DataType.INT).build();
    }

    private TableShard scope() {
      return OwnershipProtocol.tableScope(LAYOUT, TABLE);
    }

    private ControlToken control() {
      return _catalog.membershipControl().controlEpoch();
    }

    private Revision revision() {
      return new Revision(_history, _index);
    }
  }
}
