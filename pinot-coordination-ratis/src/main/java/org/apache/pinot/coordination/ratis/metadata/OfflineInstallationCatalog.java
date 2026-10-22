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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ControlState;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ParticipantState;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ParticipantToken;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Role;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.CatalogRecord;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.ObjectVersion;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.RecordKind;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Revision;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Status;
import org.apache.pinot.coordination.ratis.metadata.OfflineInstallationProtocol.InstallationSnapshot;
import org.apache.pinot.coordination.ratis.metadata.OfflineInstallationProtocol.Materialization;
import org.apache.pinot.coordination.ratis.metadata.OfflineInstallationProtocol.Receipt;
import org.apache.pinot.coordination.ratis.metadata.OfflineInstallationProtocol.ReportInstallation;
import org.apache.pinot.coordination.ratis.metadata.OfflineInstallationProtocol.Work;
import org.apache.pinot.coordination.ratis.metadata.OfflineSegmentProtocol.Assignment;
import org.apache.pinot.coordination.ratis.metadata.OfflineSegmentProtocol.Compatibility;
import org.apache.pinot.coordination.ratis.metadata.OfflineSegmentProtocol.Content;
import org.apache.pinot.coordination.ratis.metadata.OperationProtocol.AssignedOfflineSegmentReload;
import org.apache.pinot.coordination.ratis.metadata.OperationProtocol.OfflineSegmentReload;
import org.apache.pinot.coordination.ratis.metadata.OperationProtocol.OperationState;
import org.apache.pinot.coordination.ratis.metadata.OperationProtocol.State;
import org.apache.pinot.coordination.ratis.metadata.OperationProtocol.TargetMaterialization;


/// Deterministic acceptance of participant installation evidence. A receipt is immutable historical evidence for
/// one operation attempt; current ownership, membership and desired materialization remain separate serving fences.
final class OfflineInstallationCatalog {
  private OfflineInstallationCatalog() {
  }

  /// Omission from a complete image revokes released materializations. Operation hints cannot keep superseded
  /// assignments alive or accumulate indefinitely; their durable receipts/outcomes remain separately addressable.
  static boolean targetsCurrentAssignment(OperationState operation, ParticipantToken server, OperationCatalog.View view)
      throws IOException {
    if (!(operation.input() instanceof AssignedOfflineSegmentReload assigned)) {
      return false;
    }
    CatalogRecord record = view.record(RecordKind.OFFLINE_REPLICA_ASSIGNMENT,
        OfflineSegmentProtocol.assignmentName(assigned.reload().segmentObjectId(), server.participantId()));
    if (record == null || record.deleted()) {
      return false;
    }
    Assignment current = OfflineSegmentProtocol.decodeAssignment(record.value());
    return current.desiredOnline() && current.server().equals(server)
        && assigned.reload().targets().contains(new TargetMaterialization(server, current.assignmentEpoch(),
            current.materializationId()));
  }

  /// Admission uses the same complete projection as discovery, including future membership-header growth. A
  /// rejected placement publishes neither assignment nor intent, and cannot strand an already admitted server.
  static void admitProjection(ParticipantToken server, OperationCatalog.View view) throws IOException {
    Set<UUID> records = new HashSet<>();
    long bytes = ProjectionReservation.HEADER_BYTES + ProjectionReservation.CLUSTER_BYTES + 1;
    int operations = 0;
    List<CatalogRecord> selected = new ArrayList<>();
    int clusterRecords = 1; // Reserve the singleton even if cluster configuration is created after admission.
    for (CatalogRecord record : view.records(RecordKind.OFFLINE_REPLICA_ASSIGNMENT)) {
      Assignment assignment = OfflineSegmentProtocol.decodeAssignment(record.value());
      if (assignment.desiredOnline() && assignment.server().equals(server)) {
        selected.add(record);
        selected.add(view.record(assignment.segmentObjectId()));
        selected.add(view.record(assignment.compatibility().table().objectId()));
        selected.add(view.record(assignment.compatibility().schema().objectId()));
      }
    }
    for (CatalogRecord record : selected) {
      require(record != null, Status.NOT_READY, "Placement projection dependency is unavailable");
      if (records.add(record.objectId())) {
        bytes += view.recordBytes(record) + 1;
        projectionBounds(records.size() + clusterRecords, operations, bytes);
      }
    }
    for (CatalogRecord record : view.records(RecordKind.CONTROLLER_OPERATION)) {
      OperationState operation = OperationProtocol.decodeOperation(record.value());
      if ((operation.status() == State.ACCEPTED || operation.status() == State.RUNNING)
          && targetsCurrentAssignment(operation, server, view)) {
        operations++;
        bytes += 39; // A quoted UUID plus an array separator.
        projectionBounds(records.size() + clusterRecords, operations, bytes);
      }
    }
  }

  private static void projectionBounds(int records, int operations, long bytes) {
    require(records + operations <= OfflineInstallationProtocol.MAX_SNAPSHOT_RECORDS
        && bytes <= OfflineInstallationProtocol.MAX_SNAPSHOT_BYTES, Status.NOT_READY,
        "Server installation projection is full; placement cannot admit additional work");
  }

  /// Synthetic full-escape strings and widest counters bound the header independently of future endpoint renewals.
  private static final class ProjectionReservation {
    private static final long HEADER_BYTES = headerBytes();
    private static final long CLUSTER_BYTES = clusterBytes();

    private static long clusterBytes() {
      try {
        // Every config value must fit inside a MAX_COMMAND_BYTES mutation. Add the entire worst-case record
        // envelope rather than assuming that command/record overhead or counter widths happen to cancel out.
        CatalogRecord envelope = new CatalogRecord(RecordKind.CLUSTER_CONFIG, MetadataProtocol.CLUSTER_CONFIG_NAME,
            new UUID(0, 0), Long.MAX_VALUE, MetadataProtocol.FORMAT_VERSION, Long.MAX_VALUE, false,
            MetadataProtocol.toTree(Map.of()));
        return MetadataProtocol.MAX_COMMAND_BYTES + MetadataProtocol.encodeValue(envelope).length;
      } catch (IOException e) {
        throw new ExceptionInInitializerError(e);
      }
    }

    private static long headerBytes() {
      UUID id = new UUID(0, 0);
      String identifier = "\u0000".repeat(MembershipProtocol.MAX_IDENTIFIER_LENGTH);
      Map<String, String> endpoints = new HashMap<>();
      for (int ordinal = 0; ordinal < MembershipProtocol.MAX_ENDPOINTS; ordinal++) {
        endpoints.put("\u0000".repeat(MembershipProtocol.MAX_IDENTIFIER_LENGTH - 1) + (char) (16 + ordinal),
            "\u0000".repeat(MembershipProtocol.MAX_ENDPOINT_VALUE_LENGTH));
      }
      ParticipantToken token = new ParticipantToken(id, identifier, Role.SERVER, id, Long.MAX_VALUE);
      ParticipantState participant = new ParticipantState(token, endpoints,
          Set.of(MembershipProtocol.Capability.SERVER_QUERY), Long.MAX_VALUE, false, false, false,
          new Revision(id, Long.MAX_VALUE), Long.MAX_VALUE, Long.MAX_VALUE);
      MembershipProtocol.ControlToken control =
          new MembershipProtocol.ControlToken(id, Long.MAX_VALUE, Long.MAX_VALUE, identifier, id);
      MembershipProtocol.Policy policy = new MembershipProtocol.Policy(Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE,
          Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE);
      ControlState state = new ControlState(control, Long.MAX_VALUE, policy, Long.MAX_VALUE, Long.MAX_VALUE,
          Long.MAX_VALUE);
      try {
        return MetadataProtocol.encodeValue(new InstallationSnapshot(Status.OK, new Revision(id, Long.MAX_VALUE),
            "", participant, List.of(), List.of(), state)).length;
      } catch (IOException e) {
        throw new ExceptionInInitializerError(e);
      }
    }
  }

  static void validateSession(ParticipantToken server, OperationCatalog.View view) {
    validateProjectionSession(server, view);
    ParticipantState current = view.participant(server.participantId());
    require(current.ready() && !current.draining(), Status.FENCED, "Installation participant is not ready or draining");
  }

  /// Metadata is available before readiness and while draining, but never to an expired or replaced registration.
  /// Draining readers need the committed view for bounded delivery checks; it grants no installation authority.
  static void validateProjectionSession(ParticipantToken server, OperationCatalog.View view) {
    require(server != null && view.historyId().equals(server.historyId()), Status.HISTORY_MISMATCH,
        "Installation participant history differs");
    require(server.role() == Role.SERVER && server.incarnation() != null && server.registrationEpoch() > 0,
        Status.FENCED, "Installation requires a complete SERVER registration");
    OfflineSegmentCatalog.identifier(server.participantId(), 1024);
    ParticipantState current = view.participant(server.participantId());
    ControlState control = view.control();
    require(current != null && current.participant().equals(server) && control != null
        && control.controlEpoch() != null && !current.retired()
        && current.expiryRound() > control.round(), Status.FENCED,
        "Installation participant is replaced, retired or expired");
  }

  static List<CatalogRecord> stage(ReportInstallation request, OperationCatalog.View view, long index)
      throws IOException {
    validateReport(request, view.historyId());
    require(index > 0, Status.INVALID_REQUEST, "Committed installation index is required");
    Work work = work(request.operationId(), request.installed().server(), view);
    require(work.attempt() == request.attempt() && work.owner().equals(request.owner()), Status.FENCED,
        "Installation operation attempt or owner changed");
    require(work.target().equals(request.installed()), Status.FENCED,
        "Installed materialization differs from authoritative operation work");
    String name = OfflineInstallationProtocol.recordName(request.operationId(), request.attempt(),
        request.installed().server().participantId());
    CatalogRecord previous = view.record(RecordKind.OFFLINE_INSTALLATION, name);
    if (previous != null) {
      require(!previous.deleted() && OfflineInstallationProtocol.decodeReceipt(previous.value())
          .equals(new Receipt(view.historyId(), request)), Status.CONFLICT,
          "Installation evidence cannot be replaced for the same operation attempt and target");
      return List.of();
    }
    return List.of(new CatalogRecord(RecordKind.OFFLINE_INSTALLATION, name,
        OfflineInstallationProtocol.objectId(view.historyId(), request.operationId(), request.attempt(),
            request.installed().server().participantId()), 1, MetadataProtocol.FORMAT_VERSION, index, false,
        MetadataProtocol.toTree(new Receipt(view.historyId(), request))));
  }

  static Work work(UUID operationId, ParticipantToken server, OperationCatalog.View view)
      throws IOException {
    validateSession(server, view);
    require(operationId != null, Status.INVALID_REQUEST, "Installation operation ID is required");
    CatalogRecord record = view.record(RecordKind.CONTROLLER_OPERATION, OperationProtocol.recordName(operationId));
    require(record != null && !record.deleted(), Status.NOT_FOUND, "Installation operation is absent");
    OperationState state = OperationProtocol.decodeOperation(record.value());
    require(state.input() instanceof AssignedOfflineSegmentReload, Status.INVALID_REQUEST,
        "Installation work requires an assigned native operation");
    require(state.status() == State.RUNNING, Status.CONFLICT, "Installation operation is not running");
    OperationCatalog.currentAttempt(state, state.owner(), state.attempt(), view);
    AssignedOfflineSegmentReload assigned = (AssignedOfflineSegmentReload) state.input();
    OperationCatalog.currentAssigned(assigned, view);
    OperationCatalog.currentDependencies(state.dependencies(), view);
    OfflineSegmentReload reload = assigned.reload();
    require(reload.targets().stream().anyMatch(target -> target.server().equals(server)), Status.FENCED,
        "Participant is not a target of this operation");
    Content content = OfflineSegmentCatalog.currentContent(reload.segmentObjectId(), view);
    Assignment assignment = OfflineSegmentCatalog.currentAssignment(reload.segmentObjectId(), server, view);
    ObjectVersion assignmentVersion = new ObjectVersion(view.historyId(),
        OfflineSegmentProtocol.assignmentObjectId(view.historyId(), reload.segmentObjectId(), server.participantId()),
        assignment.assignmentEpoch());
    Materialization materialization = new Materialization(content.tableName(), content.tableObjectId(),
        content.segmentName(), content.segmentObjectId(), content.contentSha256(), server, assignmentVersion,
        assignment.materializationId(), assignment.compatibility());
    return new Work(operationId, state.attempt(), state.owner(), materialization, content,
        view.record(assignment.compatibility().table().objectId()),
        view.record(assignment.compatibility().schema().objectId()),
        reload.forceDownload(), reload.initialLoad(), reload.reconcile());
  }

  static List<ObjectVersion> completionReceipts(OperationState state, OperationCatalog.View view)
      throws IOException {
    AssignedOfflineSegmentReload assigned = (AssignedOfflineSegmentReload) state.input();
    List<ObjectVersion> receipts = new ArrayList<>();
    for (TargetMaterialization target : assigned.reload().targets()) {
      CatalogRecord record = view.record(RecordKind.OFFLINE_INSTALLATION,
          OfflineInstallationProtocol.recordName(state.operationId(), state.attempt(),
              target.server().participantId()));
      require(record != null && !record.deleted(), Status.NOT_READY,
          "Every target requires a committed installation receipt");
      validateRecord(record, view.historyId(), Long.MAX_VALUE);
      Receipt receipt = OfflineInstallationProtocol.decodeReceipt(record.value());
      matchingOperation(receipt.installation(), state, true);
      receipts.add(new ObjectVersion(view.historyId(), record.objectId(), record.version()));
    }
    return List.copyOf(receipts);
  }

  static void validateRecord(CatalogRecord record, UUID historyId, long snapshotIndex)
      throws IOException {
    require(record != null && record.kind() == RecordKind.OFFLINE_INSTALLATION && !record.deleted()
        && record.version() == 1 && record.schemaVersion() == MetadataProtocol.FORMAT_VERSION
        && record.lastModifiedIndex() > 0 && record.lastModifiedIndex() <= snapshotIndex && record.value() != null,
        Status.INVALID_REQUEST, "Invalid immutable installation receipt envelope");
    Receipt receipt = OfflineInstallationProtocol.decodeReceipt(record.value());
    require(historyId.equals(receipt.historyId()), Status.HISTORY_MISMATCH, "Installation receipt history differs");
    ReportInstallation installation = receipt.installation();
    validateReport(installation, historyId);
    String participantId = installation.installed().server().participantId();
    require(OfflineInstallationProtocol.recordName(installation.operationId(), installation.attempt(), participantId)
        .equals(record.name()) && OfflineInstallationProtocol.objectId(historyId, installation.operationId(),
        installation.attempt(), participantId).equals(record.objectId()), Status.INVALID_REQUEST,
        "Installation receipt identity differs from its operation attempt and target");
  }

  static void validateReferences(CatalogRecord record, OperationCatalog.View view)
      throws IOException {
    ReportInstallation installation = OfflineInstallationProtocol.decodeReceipt(record.value()).installation();
    CatalogRecord operation = view.record(RecordKind.CONTROLLER_OPERATION,
        OperationProtocol.recordName(installation.operationId()));
    require(operation != null && !operation.deleted(), Status.INVALID_REQUEST,
        "Installation receipt references an absent operation");
    OperationState state = OperationProtocol.decodeOperation(operation.value());
    matchingOperation(installation, state, false);
    OwnershipCatalog.validateStoredTableOwner(installation.owner(), installation.installed().tableName(), view);
    Materialization installed = installation.installed();
    for (ObjectVersion version : List.of(installed.assignment(), installed.compatibility().table(),
        installed.compatibility().schema())) {
      CatalogRecord dependency = view.record(version.objectId());
      require(dependency != null && dependency.version() >= version.version()
          && (dependency.version() > version.version() || dependency.lastModifiedIndex() <= record.lastModifiedIndex()),
          Status.INVALID_REQUEST, "Installation receipt references an absent or future dependency");
    }
  }

  static void validateCompletionReferences(CatalogRecord operation, OperationCatalog.View view)
      throws IOException {
    OperationState state = OperationProtocol.decodeOperation(operation.value());
    for (ObjectVersion version : state.completionProof().installations()) {
      CatalogRecord receipt = view.record(version.objectId());
      require(receipt != null && receipt.kind() == RecordKind.OFFLINE_INSTALLATION && !receipt.deleted()
          && receipt.version() == version.version() && receipt.lastModifiedIndex() < operation.lastModifiedIndex(),
          Status.INVALID_REQUEST, "Operation completion proof references an absent or future installation receipt");
      matchingOperation(OfflineInstallationProtocol.decodeReceipt(receipt.value()).installation(), state, true);
    }
  }

  private static void matchingOperation(ReportInstallation installation, OperationState state, boolean exactAttempt) {
    require(state.input() instanceof AssignedOfflineSegmentReload
        && installation.operationId().equals(state.operationId())
        && installation.attempt() <= state.attempt(), Status.INVALID_REQUEST,
        "Installation receipt references an absent or future native operation attempt");
    if (exactAttempt || installation.attempt() == state.attempt()) {
      require(installation.attempt() == state.attempt() && installation.owner().equals(state.owner()),
          Status.INVALID_REQUEST, "Installation receipt operation attempt or owner differs");
    }
    AssignedOfflineSegmentReload assigned = (AssignedOfflineSegmentReload) state.input();
    OfflineSegmentReload reload = assigned.reload();
    Materialization installed = installation.installed();
    require(installed.tableName().equals(reload.tableName()) && installed.tableObjectId().equals(reload.tableObjectId())
        && installed.segmentName().equals(reload.segmentName())
        && installed.segmentObjectId().equals(reload.segmentObjectId())
        && installed.contentSha256().equals(reload.contentSha256())
        && reload.targets().contains(new TargetMaterialization(installed.server(), installed.assignment().version(),
            installed.materializationId())) && assigned.dependencies().contains(installed.assignment())
        && assigned.dependencies().contains(installed.compatibility().table())
        && assigned.dependencies().contains(installed.compatibility().schema()), Status.INVALID_REQUEST,
        "Installation receipt differs from immutable operation target or compatibility");
  }

  private static void validateReport(ReportInstallation report, UUID historyId) {
    require(report != null && report.operationId() != null && report.attempt() > 0 && report.installationId() != null,
        Status.INVALID_REQUEST, "Installation receipt requires operation, attempt and physical installation identity");
    digest(report.manifestSha256());
    Materialization installed = report.installed();
    require(installed != null && installed.tableObjectId() != null && installed.segmentObjectId() != null,
        Status.INVALID_REQUEST, "Installation receipt requires table and content identities");
    OfflineSegmentCatalog.identifier(installed.tableName(), 1024);
    OfflineSegmentCatalog.identifier(installed.segmentName(), 1024);
    digest(installed.contentSha256());
    OwnershipCatalog.validateStoredTableOwnerShape(report.owner(), installed.tableName(), historyId);
    ParticipantToken server = installed.server();
    require(server != null && historyId.equals(server.historyId()) && server.role() == Role.SERVER
        && server.incarnation() != null && server.registrationEpoch() > 0, Status.INVALID_REQUEST,
        "Installation receipt requires a complete SERVER token in this history");
    OfflineSegmentCatalog.identifier(server.participantId(), 1024);
    version(installed.assignment(), historyId);
    UUID assignmentId = OfflineSegmentProtocol.assignmentObjectId(historyId, installed.segmentObjectId(),
        server.participantId());
    require(assignmentId.equals(installed.assignment().objectId())
        && OfflineSegmentProtocol.materializationId(historyId, assignmentId, installed.assignment().version())
            .equals(installed.materializationId()), Status.INVALID_REQUEST,
        "Installation assignment or materialization identity differs");
    Compatibility compatibility = installed.compatibility();
    require(compatibility != null, Status.INVALID_REQUEST, "Installation compatibility is required");
    version(compatibility.table(), historyId);
    version(compatibility.schema(), historyId);
    require(compatibility.table().objectId().equals(installed.tableObjectId())
        && !compatibility.table().objectId().equals(compatibility.schema().objectId()), Status.INVALID_REQUEST,
        "Installation compatibility requires the target table and a distinct schema");
  }

  private static void version(ObjectVersion version, UUID historyId) {
    require(version != null && historyId.equals(version.historyId()) && version.objectId() != null
        && version.version() > 0,
        Status.INVALID_REQUEST, "Installation dependency identity, version or history is invalid");
  }

  private static void digest(String value) {
    require(value != null && value.matches("[0-9a-f]{64}"), Status.INVALID_REQUEST,
        "Installation digests must be lowercase SHA-256 values");
  }

  private static void require(boolean condition, Status status, String message) {
    if (!condition) {
      throw new CoordinationRejection(status, message);
    }
  }
}
